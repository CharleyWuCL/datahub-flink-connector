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

package charley.wu.flink.connector.datahub;

import charley.wu.flink.connector.datahub.async.AsyncReadService;
import charley.wu.flink.connector.datahub.client.DataHubClientFactory;
import charley.wu.flink.connector.datahub.config.DataHubConfig;
import charley.wu.flink.connector.datahub.serialization.basic.DataHubDeserializer;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.SeekOutOfRangeException;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import com.aliyun.datahub.exception.SubscriptionOfflineException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
  private DatahubClient client;
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

  private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

  private transient volatile boolean restored;

  public DataHubSource(Properties props, DataHubDeserializer<OUT> deserializer) {
    this.deserializer = deserializer;
    this.props = props;
    this.config = new DataHubConfig(props);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    Validate.notEmpty(this.props, "DataHub props can not be empty");
    Validate.notNull(this.deserializer, "DataHubDeserializer can not be null");

    this.project = props.getProperty(DataHubConfig.SOURCE_PROJECT_KEY);
    this.topic = props.getProperty(DataHubConfig.SOURCE_TOPIC_KEY);
    this.subId = props.getProperty(DataHubConfig.SOURCE_SUBID_KEY);

    Validate.notNull(this.project, "DataHub Project can not be null");
    Validate.notNull(this.topic, "DataHub Topic can not be null");
    Validate.notNull(this.subId, "DataHub SubId can not be null");

    this.client = new DataHubClientFactory(this.config).create();

    // Step1. 获取shard信息
    List<ShardEntry> shards = client.listShard(project, topic).getShards();
    if (shards == null || shards.size() == 0) {
      throw new Exception("No shard of " + project + "." + topic);
    }
    List<String> shardIds = shards.stream().map(ShardEntry::getShardId)
        .collect(Collectors.toList());

    // 1. 如果使用点位服务，首先需要openSubscriptionSession获取订阅的sessionId和versionId信息，
    //    OpenSession只需初始化一次
    // Step2. 获取Offset Map
    Map<String, SubscriptionOffset> offsets = client
        .openSubscriptionSession(project, topic, subId, shardIds).getOffsets();
    readService = new AsyncReadService(client, offsets);
    runningChecker = new RunningChecker();

    if (cursorTable == null) {
      cursorTable = new ConcurrentHashMap<>();
    }
    if (restoredCursors == null) {
      restoredCursors = new ConcurrentHashMap<>();
    }

    // 3. 读取并保存点位，这里以读取BLOB数据为例，并且每1000条记录保存一次点位
    schema = client.getTopic(project, topic).getRecordSchema();
  }


  @Override
  public void run(SourceContext context) throws Exception {
    LOG.debug("connector run....");
    // The lock that guarantees that record emission and state updates are atomic,
    // from the view of taking a checkpoint.
    final Object lock = context.getCheckpointLock();

    int delayWhenMessageNotFound = 100;

    int threadNum = 1;
    int pullBatchSize = 10;

    readService.setThreadNum(threadNum);
    readService.registerCallback((readContext) -> {
      String shardId = readContext.getShardId();
      SubscriptionOffset offset = readContext.getOffset();
      try {
        // Step1. 获取对应shard的点位信息，并获取到用于下次读取数据的Cursor信息
        String cursor = getShardCursor(shardId, offset);

        // Step2. 消费消息
        boolean found = false;
        GetRecordsResult recordsResult = client
            .getRecords(project, topic, shardId, schema, cursor, pullBatchSize);
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
            offset.setSequence(record.getSequence());
            offset.setTimestamp(record.getSystemTime());
          }
          found = true;
        }

        synchronized (lock) {
          putShardCursor(shardId, recordsResult.getNextCursor(), offset);
        }

        if (found) {
          readContext.setReadNextDelayTimeMillis(0); // no delay when messages were found
        } else {
          readContext.setReadNextDelayTimeMillis(delayWhenMessageNotFound);
        }

      } catch (SubscriptionOfflineException e) {
        // Offline: 订阅下线
        // TODO Do something. 退出
        LOG.error("Subscriber is offline.");
      } catch (OffsetSessionChangedException e) {
        // SessionChange: 表示订阅被其他客户端同时消费
        // TODO Do something. 退出
        LOG.error("Subscriber is consumed by another client.");
      } catch (OffsetResetedException e) {
        // 表示点位被重置，重新获取SubscriptionOffset信息，这里以Sequence重置为例
        // 如果以Timestamp重置，需要通过CursorType.SYSTEM_TIME获取cursor
        offset = client
            .getSubscriptionOffset(project, topic, subId, Collections.singletonList(shardId))
            .getOffsets().get(shardId);
        long nextSequence = offset.getSequence() + 1;
        String cursor = client
            .getCursor(project, topic, shardId, CursorType.SEQUENCE, nextSequence)
            .getCursor();

        synchronized (lock) {
          putShardCursor(shardId, cursor, offset);
        }
      } catch (DatahubClientException e) {
        // TODO: 针对不同异常决定是否退出
      } catch (Exception e) {
        // TODO Do something. 异常处理
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


  private void awaitTermination() throws InterruptedException {
    while (runningChecker.isRunning()) {
      Thread.sleep(50);
    }
  }

  private String getShardCursor(String shardId, SubscriptionOffset offset) {
    String cursor = cursorTable.get(shardId);
    if (restored && Strings.isNullOrEmpty(cursor)) {
      cursor = restoredCursors.get(shardId);
    }
    if (Strings.isNullOrEmpty(cursor)) {
      cursor = getCursorAtFirstTime(shardId, offset);
    }
    cursorTable.put(shardId, cursor);
    return cursorTable.get(shardId);
  }

  private String getCursorAtFirstTime(String shardId, SubscriptionOffset offset) {
    String cursor;
    if (offset.getSequence() >= 0) {
      try {
        // 获取下一条记录的Cursor
        long nextSequence = offset.getSequence() + 1;
        // 备注：如果按照SEQUENCE getCursor报SeekOutOfRange错误，需要回退到按照SYSTEM_TIME或者OLDEST/LATEST进行getCursor
        cursor = getCursor(shardId, CursorType.SEQUENCE, nextSequence);
      } catch (SeekOutOfRangeException e) {
        LOG.warn("Seek out of range, change cursor type to OLDEST");
        cursor = getCursor(shardId, CursorType.OLDEST);
      }
    } else {
      // 获取最旧数据的Cursor
      cursor = getCursor(shardId, CursorType.OLDEST);
    }
    return cursor;
  }

  private String getCursor(String shardId, CursorType type) {
    return getCursor(shardId, type, -1);
  }

  private String getCursor(String shardId, CursorType type, long param) {
    return client.getCursor(project, topic, shardId, type, param).getCursor();
  }

  private void putShardCursor(String shardId, String cursor, SubscriptionOffset offset) {
    cursorTable.put(shardId, cursor);
    client.commitSubscriptionOffset(project, topic, subId,
        Collections.singletonMap(shardId, offset));
  }

  @Override
  public void cancel() {
    LOG.debug("cancel ...");
    runningChecker.setRunning(false);

    if (readService != null) {
      readService.shutdown();
    }

    cursorTable.clear();
    restoredCursors.clear();
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
            OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
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
