package charley.wu.flink.datahub;

import charley.wu.flink.datahub.client.DataHubClientFactory;
import charley.wu.flink.datahub.config.DHConfig;
import charley.wu.flink.datahub.retry.RetryForever;
import charley.wu.flink.datahub.retry.RetryLoop;
import charley.wu.flink.datahub.selector.ShardSelector;
import charley.wu.flink.datahub.serialization.basic.DataHubSerializer;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.ShardEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataHub flink sink.
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class DataHubSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(DataHubSink.class);

  private Properties properties;
  private DHConfig config;
  private DataHubClientFactory factory;
  private DatahubClient client;

  private DataHubSerializer<IN> serializer;
  private ShardSelector<IN> selector;

  private boolean batchFlushOnCheckpoint; // false by default
  private int batchSize = 128;
  private int initialCap = batchSize + (batchSize >> 1);
  private Map<String, List<RecordEntry>> batchMap;

  private String sinkProject;
  private String sinkTopic;

  private Counter counter;

  public DataHubSink(Properties props, DataHubSerializer<IN> serializer,
      ShardSelector<IN> selector) {
    this.properties = props;
    this.config = new DHConfig(props);
    this.serializer = serializer;
    this.selector = selector;

    this.sinkProject = this.config.getSinkProject();
    this.sinkTopic = this.config.getSinkTopic();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    Validate.notEmpty(this.properties, "DataHub properties can not be empty");
    Validate.notNull(this.selector, "Shard Selector can not be null");
    Validate.notNull(this.serializer, "DataHubSerializer can not be null");

    Validate.notNull(this.sinkProject, "DataHub Project can not be null");
    Validate.notNull(this.sinkTopic, "DataHub Topic can not be null");

    this.factory = new DataHubClientFactory(this.config);
    this.client = factory.create();
    ListShardResult shardResult = this.client.listShard(sinkProject, sinkTopic);
    Validate.notNull(shardResult, "DataHub ListShardResult can not be null");
    List<String> shardIds = shardResult.getShards().stream().map(ShardEntry::getShardId)
        .collect(Collectors.toList());
    Validate.notEmpty(shardIds, "DataHub Shard Number can not be empty");
    this.selector.setShardList(shardIds);

    this.batchMap = new HashMap<>();

    if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext())
        .isCheckpointingEnabled()) {
      LOG.warn(
          "Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
      batchFlushOnCheckpoint = false;
    }

    this.counter = ((OperatorMetricGroup) getRuntimeContext().getMetricGroup()).getIOMetricGroup()
        .getNumRecordsOutCounter();
  }

  @Override
  public void invoke(IN input, Context context) throws Exception {
    RecordEntry record = prepareRecord(input);
    String shardId = selector.getShard(input);

    // batch
    if (batchFlushOnCheckpoint) {
      List<RecordEntry> batchList = getBatchList(shardId);
      batchList.add(record);
      if (batchList.size() >= batchSize) {
        synchronized (batchMap) {
          putBatch(shardId, batchList);
        }
      }
    } else {
      putRecord(shardId, Collections.singletonList(record));
    }

    counter.inc();
  }

  private List<RecordEntry> getBatchList(String shardId) {
    List<RecordEntry> batchList;
    batchList = batchMap.get(shardId);
    if (batchList == null) {
      synchronized (batchMap) {
        if (batchList == null) {
          batchMap.put(shardId, new ArrayList<>(initialCap));
          batchList = batchMap.get(shardId);
        }
      }
    }
    return batchList;
  }

  private RecordEntry prepareRecord(IN input) {
    RecordEntry record = new RecordEntry();
    serializer.serializeAttrs(record, input);
    serializer.serializeData(record, input);
    return record;
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
      putRecordWithRetry(shardId, batchList);
    } catch (DatahubClientException e) {
      client = null;
      client = factory.create();
    }
  }

  private void putRecordWithRetry(String shardId, List<RecordEntry> batchList)
      throws DatahubClientException {
    RetryLoop retryLoop = new RetryLoop(new RetryForever(5));
    while (retryLoop.shouldContinue()) {
      try {
        client.putRecordsByShard(sinkProject, sinkTopic, shardId, batchList);
        retryLoop.markComplete();
      } catch (DatahubClientException e) {
        retryLoop.takeRuntimeException(e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      flushSync();
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
    LOG.info("[DataHub] The value batchFlushOnCheckpoint is: {}", batchFlushOnCheckpoint);
    return this;
  }

  public DataHubSink<IN> setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }
}
