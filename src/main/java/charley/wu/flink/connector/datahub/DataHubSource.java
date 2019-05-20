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
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.SubscriptionOfflineException;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
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
public class AsyncReadService {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncReadService.class);

  private int threadNum = 10;

  private final Map<String, SubscriptionOffset> offsets;
  private ExecutorService threadPool;
  private ReadCallback callback;
  private Map<String, ReadTask> tasks;

  public AsyncReadService(Map<String, SubscriptionOffset> offsets) {
    this.offsets = offsets;
  }

  public void start() throws DataHubException {
    this.threadPool = Executors.newFixedThreadPool(this.threadNum);
    this.tasks = new ConcurrentHashMap<>();
    putTask(tasks);
    LOG.info("DataHub AsyncReadService start OK.");
  }

  public void shutdown() {
    if(tasks != null){
      tasks.forEach((shardId, task) -> {
        task.cancel();
      });
    }

    if (this.threadPool != null) {
      this.threadPool.shutdown();
    }
  }

  private void putTask(Map<String, ReadTask> tasks) {
    Iterator<Entry<String, SubscriptionOffset>> it = offsets.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, SubscriptionOffset> next = it.next();
      String shardId = next.getKey();
      SubscriptionOffset offset = next.getValue();
      ReadContext context = new ReadContext(shardId, offset);
      ReadTask task = new ReadTask(context);
      tasks.put(shardId, task);
      this.threadPool.execute(task);
    }
  }

  public int getThreadNum() {
    return threadNum;
  }

  public void setThreadNum(int threadNum) {
    this.threadNum = threadNum;
  }

  public void registerCallback(ReadCallback callback) {
    this.callback = callback;
  }

  class ReadTask implements Runnable {

    private volatile boolean cancelled = false;
    private final ReadContext context;

    public ReadTask(ReadContext context) {
      this.context = context;
    }

    @Override
    public void run() {
      while (!this.isCancelled()) {
        if (callback != null) {
          try {
            callback.execute(context);
          } catch (Throwable e) {
            context.setReadNextDelayTimeMillis(1000);
            LOG.error("doPullTask Exception", e);
          }
        } else {
          LOG.error("Read Task Callback not set.");
          cancelled = true;
        }
      }
      LOG.warn("The Read Task is cancelled, {}", context.getShardId());
    }

    public boolean isCancelled() {
      return cancelled;
    }

    public void cancel(){
      this.cancelled = true;
    }
  }
}
