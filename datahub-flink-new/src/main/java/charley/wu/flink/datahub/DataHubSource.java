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

package charley.wu.flink.datahub;

import charley.wu.flink.datahub.client.ConsumerFactory;
import charley.wu.flink.datahub.config.DHConfig;
import charley.wu.flink.datahub.coordinate.consumer.Consumer;
import charley.wu.flink.datahub.metrics.DelayGauge;
import charley.wu.flink.datahub.serialization.basic.DataHubDeserializer;
import charley.wu.flink.datahub.utils.ConfigUtil;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.exception.DatahubClientException;
import java.util.Properties;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
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

  private static final int NOT_FOUND_DELAY = 100;
  private static final int ERROR_DELAY = 500;
  private static final int READ_RETRY = 20;

  private DelayGauge delayGauge;
  private Counter counter;

  private Properties props;
  private DHConfig config;
  private ConsumerFactory factory;
  private Consumer consumer;
  private boolean readStop = false;
  private int delayWhenMessageNotFound;

  private RateLimiter rateLimiter;

  private DataHubDeserializer<OUT> deserializer;

  public DataHubSource(Properties props, DataHubDeserializer<OUT> deserializer) {
    this.deserializer = deserializer;
    this.props = props;
    this.config = new DHConfig(props);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      Validate.notEmpty(this.props, "DataHub props can not be empty");
      Validate.notNull(this.deserializer, "DataHubDeserializer can not be null");

      String project = props.getProperty(DHConfig.SOURCE_PROJECT);
      String topic = props.getProperty(DHConfig.SOURCE_TOPIC);

      Validate.notNull(project, "DataHub Project can not be null");
      Validate.notNull(topic, "DataHub Topic can not be null");

      this.delayGauge = new DelayGauge();

      // 创建Consumer
      this.factory = new ConsumerFactory(config);
      this.consumer = factory.create();

      int rate = ConfigUtil.getInteger(props, DHConfig.SOURCE_RATE, DHConfig.DEFAULT_SOURCE_RATE);
      this.rateLimiter = RateLimiter.create(rate);

      getRuntimeContext().getMetricGroup().gauge("delay", delayGauge);
      this.counter = ((OperatorMetricGroup) getRuntimeContext().getMetricGroup()).getIOMetricGroup()
          .getNumRecordsInCounter();

    } catch (Exception e) {
      if(consumer != null){
        consumer.close();
      }
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

    while (!readStop) {
      // 限流
      rateLimiter.acquire();
      try {
        if (delayWhenMessageNotFound != 0) {
          Thread.sleep(delayWhenMessageNotFound);
        }

        RecordEntry record = consumer.read(READ_RETRY);
        boolean found = false;
        if (record != null) {
          // process
          OUT data = deserializer.deserializeValue(record);

          synchronized (lock) {
            context.collectWithTimestamp(data, record.getSystemTime());
            counter.inc();
            delayGauge.setValue(System.currentTimeMillis() - record.getSystemTime());
          }
          found = true;
        }

        if (found) {
          delayWhenMessageNotFound = 0; // no delay when messages were found
        } else {
          delayWhenMessageNotFound = NOT_FOUND_DELAY;
        }
      } catch (DatahubClientException e) {
        // - subscription exception, will not recover
        // print some log or just use a new consumer
        consumer.close();
        consumer = factory.create();
        delayWhenMessageNotFound = ERROR_DELAY;
      }
    }
  }

  @Override
  public void cancel() {
    LOG.debug("cancel ...");
    readStop = true;
    if(consumer != null){
      consumer.close();
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
    // Do nothing.
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // called every time the user-defined function is initialized,
    // be that when the function is first initialized or be that
    // when the function is actually recovering from an earlier checkpoint.
    // Given this, initializeState() is not only the place where different types of state are initialized,
    // but also where state recovery logic is included.
    // Do nothing.
  }

  @Override
  public TypeInformation<OUT> getProducedType() {
    return deserializer.getProducedType();
  }

}
