package charley.wu.flink.datahub;

import charley.wu.flink.datahub.client.ProducerFactory;
import charley.wu.flink.datahub.config.DataHubConfig;
import charley.wu.flink.datahub.selector.ShardSelector;
import charley.wu.flink.datahub.serialization.Serializer;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.clientlibrary.exception.ClientException;
import com.aliyun.datahub.clientlibrary.producer.Producer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

/**
 * DataHub flink sink.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
@Slf4j
public class DataHubSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;

  private static final int MAX_RETRY = 5;

  private final DataHubConfig config;
  private final Serializer<IN> serializer;
  private final ShardSelector<IN> selector;

  private boolean batchFlushOnCheckpoint; // false by default
  private int batchSize = 128;
  private final int initialCap = batchSize + (batchSize >> 1);
  private final Map<String, List<RecordEntry>> batchMap;

  private ProducerFactory factory;
  private Producer producer;
  private RecordSchema schema;
  private Counter counter;

  public DataHubSink(Properties props, Serializer<IN> serializer,
      ShardSelector<IN> selector) {
    this.config = new DataHubConfig(props);
    this.serializer = serializer;
    this.selector = selector;

    this.batchMap = new HashMap<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      factory = new ProducerFactory(config);
      producer = factory.create();
      schema = factory.schema();

      Validate.isTrue(schemaHasEventTime(schema), "DataHub Topic schema, event_time not exists.");
      selector.setShardList(factory.shardIds());

      if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext())
          .isCheckpointingEnabled()) {
        log.warn(
            "Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
        batchFlushOnCheckpoint = false;
      }

      this.counter = ((OperatorMetricGroup) getRuntimeContext().getMetricGroup()).getIOMetricGroup()
          .getNumRecordsOutCounter();
    } catch (Exception e) {
      producerClose();
      log.error("Open datahub sink error.", e);
      throw e;
    }
  }

  @Override
  public void invoke(IN input, Context context) throws Exception {
    RecordEntry record = prepareRecord(schema, input);
    String shardId = selector.getShard(input);

    // batch
    if (batchFlushOnCheckpoint) {
      List<RecordEntry> batchList = getBatchList(shardId);
      batchList.add(record);
      if (batchList.size() >= batchSize) {
        flushSync();
      }
    } else {
      putRecord(shardId, Collections.singletonList(record));
    }
    counter.inc();
  }

  private boolean schemaHasEventTime(RecordSchema schema) {
    Field field = schema.getField(Serializer.EVENT_TIME);
    return Objects.nonNull(field);
  }

  private RecordEntry prepareRecord(RecordSchema schema, IN input) {
    RecordEntry record = new RecordEntry();
    serializer.serializeAttrs(record, input);
    serializer.serializeData(record, schema, input);
    return record;
  }

  private List<RecordEntry> getBatchList(String shardId) {
    List<RecordEntry> batchList;
    batchList = batchMap.get(shardId);
    if (batchList == null) {
      synchronized (batchMap) {
        batchMap.put(shardId, new ArrayList<>(initialCap));
        batchList = batchMap.get(shardId);
      }
    }
    return batchList;
  }

  private void flushSync() throws Exception {
    if (batchFlushOnCheckpoint) {
      synchronized (batchMap) {
        batchMap.forEach(this::putBatch);
      }
    }
  }

  private void putBatch(String shardId, List<RecordEntry> batchList) {
    if (batchList.size() > 0) {
      putRecord(shardId, batchList);
      batchList.clear();
    }
  }

  private void putRecord(String shardId, List<RecordEntry> batchList) {
    try {
      producer.send(batchList, shardId, MAX_RETRY);
    } catch (ClientException e) {
      producerClose();
      producer = factory.create();
    }
  }

  public void producerClose() {
    try {
      if (Objects.nonNull(this.producer)) {
        this.producer.close();
      }
    } catch (Exception e) {
      log.warn("Close producer error, dropped.", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (this.producer != null) {
      flushSync();
      producerClose();
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    flushSync();
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext)
      throws Exception {
  }

  public DataHubSink<IN> setBatchFlushOnCheckpoint(boolean batchFlushOnCheckpoint) {
    this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
    log.info("[DataHub] The value batchFlushOnCheckpoint is: {}", batchFlushOnCheckpoint);
    return this;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
}
