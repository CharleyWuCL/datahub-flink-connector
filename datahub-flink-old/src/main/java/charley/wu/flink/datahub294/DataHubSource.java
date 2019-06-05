/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package charley.wu.flink.datahub294;

import charley.wu.flink.datahub294.async.AsyncReadService;
import charley.wu.flink.datahub294.client.DataHubClientFactory;
import charley.wu.flink.datahub294.config.DataHubConfig;
import charley.wu.flink.datahub294.serialization.basic.DataHubDeserializer;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import com.aliyun.datahub.exception.SubscriptionOfflineException;
import com.aliyun.datahub.model.GetCursorRequest.CursorType;
import com.aliyun.datahub.model.GetRecordsResult;
import com.aliyun.datahub.model.ListShardResult;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RocketMQSource is based on RocketMQ pull consumer mode, and provides exactly once reliability
 * guarantees when checkpoints are enabled. Otherwise, the connector doesn't provide any reliability
 * guarantees.
 */
public class DataHubSource<OUT> extends RichParallelSourceFunction<OUT>
    implements CheckpointedFunction, ResultTypeQueryable<OUT> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(DataHubSource.class);

  private Properties props;
  private DataHubConfig config;
  private DataHubClientFactory factory;
  private DatahubClient mainClient;
  private AsyncReadService readService;

  private DataHubDeserializer<OUT> deserializer;
  private RecordSchema schema;
  private RunningChecker runningChecker;

  private transient ListState<Tuple2<String, String>> unionCursorStates;
  private Map<String, String> cursorTable;
  private Map<String, String> restoredCursors;

  private String project;
  private String topic;
  private String subId;

  private static final String CURSORS_STATE_NAME = "topic-shard-cursor-states";

  private transient volatile boolean restored;

  public DataHubSource(Properties props, DataHubDeserializer<OUT> deserializer) {
    this.deserializer = deserializer;
    this.props = props;
    this.config = new DataHubConfig(props);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      Validate.notEmpty(this.props, "DataHub props can not be empty");
      Validate.notNull(this.deserializer, "DataHubDeserializer can not be null");

      this.project = props.getProperty(DataHubConfig.SOURCE_PROJECT);
      this.topic = props.getProperty(DataHubConfig.SOURCE_TOPIC);
      this.subId = props.getProperty(DataHubConfig.SOURCE_SUBID);

      Validate.notNull(this.project, "DataHub Project can not be null");
      Validate.notNull(this.topic, "DataHub Topic can not be null");
      Validate.notNull(this.subId, "DataHub SubId can not be null");

      // New DataHub factory.
      this.factory = new DataHubClientFactory(this.config);

      // New a DataHub client.
      this.mainClient = factory.create();

      // Get topic schema.
      schema = mainClient.getTopic(project, topic).getRecordSchema();

      // Get topic shards.
      ListShardResult shardResult = mainClient.listShard(project, topic);
      if (shardResult == null || shardResult.getShards().isEmpty()) {
        throw new Exception("No shard of " + project + "." + topic);
      }

      readService = new AsyncReadService(factory, shardResult.getShards());
      runningChecker = new RunningChecker();

      if (cursorTable == null) {
        cursorTable = new ConcurrentHashMap<>();
      }
      if (restoredCursors == null) {
        restoredCursors = new ConcurrentHashMap<>();
      }
    } catch (Exception e) {
      LOG.error("Open datahub source error.", e);
      throw e;
    }
  }

  @Override
  public void run(SourceContext context) throws Exception {
    LOG.debug("connector run....");
    // The lock that guarantees that record emission and state updates are atomic,
    // from the view of taking a checkpoint.
    final Object lock = context.getCheckpointLock();

    int delayWhenMessageNotFound = 100;
    int pullBatchSize = 50;

    readService.registerCallback((client, readContext) -> {
      String shardId = readContext.getShardId();
      OffsetContext offsetCtx = readContext.getOffsetCtx();

      try {
        if (offsetCtx == null) {
          offsetCtx = client.initOffsetContext(project, topic, subId, shardId);
          readContext.setOffsetCtx(offsetCtx);
        }

        // Step1. 获取对应shard的点位信息，并获取到用于下次读取数据的Cursor信息
        String cursor = getCursor(client, shardId, offsetCtx);

        // Step2. 消费消息
        boolean found = false;
        GetRecordsResult recordsResult = client
            .getRecords(project, topic, shardId, cursor, pullBatchSize, schema);
        if (recordsResult.getRecordCount() > 0) {
          for (RecordEntry record : recordsResult.getRecords()) {

            // TODO Deal with key
            //deserializer.deserializeKeys(record);
            OUT data = deserializer.deserializeValue(record);

            // output and state update are atomic
            synchronized (lock) {
              context.collectWithTimestamp(data, record.getSystemTime());
            }

            // 处理数据完成后，设置点位
            offsetCtx.setOffset(record.getOffset());
          }
          found = true;
        }

        synchronized (lock) {
          putShardCursor(client, shardId, recordsResult.getNextCursor(), offsetCtx);
        }

        if (found) {
          readContext.setReadNextDelayTimeMillis(0); // no delay when messages were found
        } else {
          readContext.setReadNextDelayTimeMillis(delayWhenMessageNotFound);
        }

      } catch (OffsetResetedException e) {
        LOG.warn("Offset was be reset");

        client.updateOffsetContext(offsetCtx);
        String cursor = getSequenceCursor(client, shardId, offsetCtx);
        LOG.info("Restart consume shard: {}, reset offset: {}, cursor: {}", shardId,
            offsetCtx.toObjectNode().toString(), cursor);

        synchronized (lock) {
          readContext.setOffsetCtx(offsetCtx);
          putShardCursor(client, shardId, cursor, offsetCtx);
        }
      } catch (OffsetSessionChangedException e) {
        readService.shutdown();
        LOG.error("Subscriber is consumed by another client.");
        throw new RuntimeException(e);
      } catch (SubscriptionOfflineException e) {
        readContext.setReadNextDelayTimeMillis(5000);
        LOG.error("Subscriber is offline.");
      } catch (Exception e) {
        readService.shutdown();
        throw new RuntimeException(e);
      }
    });

    try {
      readService.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    runningChecker.setRunning(true);
    awaitTermination();
  }

  private String getCursor(DatahubClient client, String shardId, OffsetContext offsetCtx) {
    String cursor = cursorTable.get(shardId);
    if (restored && Strings.isNullOrEmpty(cursor)) {
      cursor = restoredCursors.get(shardId);
    }
    if (Strings.isNullOrEmpty(cursor)) {
      cursor = getCursorAtFirstTime(client, shardId, offsetCtx);
    }
    cursorTable.put(shardId, cursor);
    return cursorTable.get(shardId);
  }

  private String getCursorAtFirstTime(DatahubClient client, String shardId,
      OffsetContext offsetCtx) {
    try {
      if (offsetCtx.hasOffset()) {
        // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
        return getSequenceCursor(client, shardId, offsetCtx);
      }
    } catch (Exception e) {
      LOG.error("Get cursor by sequence error.", e);
    }

    // 获取最旧数据的Cursor
    return getOldestCursor(client, shardId);
  }

  private String getOldestCursor(DatahubClient client, String shardId) {
    return client.getCursor(project, topic, shardId, CursorType.OLDEST).getCursor();
  }

  private String getSequenceCursor(DatahubClient client, String shardId, OffsetContext offset) {
    long seq = offset.getOffset().getSequence() + 1;
    return client.getCursor(project, topic, shardId, CursorType.SEQUENCE, seq).getCursor();
  }

  private void awaitTermination() throws InterruptedException {
    while (runningChecker.isRunning()) {
      Thread.sleep(50);
    }
  }

  private void putShardCursor(DatahubClient client, String shardId, String cursor,
      OffsetContext offsetCtx) {
    cursorTable.put(shardId, cursor);
    client.commitOffset(offsetCtx);
  }

  @Override
  public void cancel() {
    LOG.debug("cancel ...");
    if (runningChecker != null) {
      runningChecker.setRunning(false);
    }

    if (readService != null) {
      readService.shutdown();
    }

    if (cursorTable != null) {
      cursorTable.clear();
    }

    if (restoredCursors != null) {
      restoredCursors.clear();
    }
  }

  @Override
  public void close() throws Exception {
    LOG.debug("close ...");
    // pretty much the same logic as cancelling
    try {
      cancel();
    } finally {
      super.close();
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // called when a snapshot for a checkpoint is requested

    if (!runningChecker.isRunning()) {
      LOG.debug("snapshotState() called on closed connector; returning null.");
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshotting state {} ...", context.getCheckpointId());
    }

    unionCursorStates.clear();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshotted state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
          cursorTable, context.getCheckpointId(), context.getCheckpointTimestamp());
    }

    for (Map.Entry<String, String> entry : cursorTable.entrySet()) {
      unionCursorStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // called every time the user-defined function is initialized,
    // be that when the function is first initialized or be that
    // when the function is actually recovering from an earlier checkpoint.
    // Given this, initializeState() is not only the place where different types of state are initialized,
    // but also where state recovery logic is included.
    LOG.debug("initialize State ...");

    this.unionCursorStates = context.getOperatorStateStore()
        .getUnionListState(new ListStateDescriptor<>(
            CURSORS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        })));

    this.restored = context.isRestored();

    if (restored) {
      if (restoredCursors == null) {
        restoredCursors = new ConcurrentHashMap<>();
      }
      for (Tuple2<String, String> mqOffsets : unionCursorStates.get()) {
        // unionCursorStates is the restored global union state;
        // should only snapshot mqs that actually belong to us
        restoredCursors.put(mqOffsets.f0, mqOffsets.f1);
      }
      LOG.info("Setting restore state in the consumer. Using the following offsets: {}",
          restoredCursors);
    } else {
      LOG.info("No restore state for the consumer.");
    }
  }

  @Override
  public TypeInformation<OUT> getProducedType() {
    return deserializer.getProducedType();
  }

}
