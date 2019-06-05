package charley.wu.flink.datahub294;

import charley.wu.flink.datahub294.client.DataHubClientFactory;
import charley.wu.flink.datahub294.config.DataHubConfig;
import charley.wu.flink.datahub294.retry.RetryForever;
import charley.wu.flink.datahub294.retry.RetryLoop;
import charley.wu.flink.datahub294.selector.ShardSelector;
import charley.wu.flink.datahub294.serialization.basic.DataHubSerializer;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.LimitExceededException;
import com.aliyun.datahub.model.ListShardResult;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.ShardEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
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
  private DataHubConfig config;
  private DatahubClient client;

  private DataHubSerializer<IN> serializer;
  private RecordSchema schema;
  private ShardSelector<IN> selector;

  private boolean batchFlushOnCheckpoint = false; // false by default
  private int batchSize = 128;
  private final List<RecordEntry> batchList;

  private String sinkProject;
  private String sinkTopic;

  public DataHubSink(Properties props, DataHubSerializer<IN> serializer,
      ShardSelector<IN> selector) {
    this.properties = props;
    this.config = new DataHubConfig(props);
    this.serializer = serializer;
    this.selector = selector;

    this.sinkProject = this.config.getSinkProject();
    this.sinkTopic = this.config.getSinkTopic();

    int initialCap = batchSize + (batchSize >> 1);
    this.batchList = new ArrayList<>(initialCap);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    Validate.notEmpty(this.properties, "DataHub properties can not be empty");
    Validate.notNull(this.selector, "Shard Selector can not be null");
    Validate.notNull(this.serializer, "DataHubSerializer can not be null");

    Validate.notNull(this.sinkProject, "DataHub Project can not be null");
    Validate.notNull(this.sinkTopic, "DataHub Topic can not be null");

    this.client = new DataHubClientFactory(this.config).create();

    // Get topic schema.
    schema = client.getTopic(sinkProject, sinkTopic).getRecordSchema();

    ListShardResult shardResult = this.client.listShard(sinkProject, sinkTopic);
    Validate.notNull(shardResult, "DataHub ListShardResult can not be null");
    List<String> shardIds = shardResult.getShards().stream().map(ShardEntry::getShardId)
        .collect(Collectors.toList());
    Validate.notEmpty(shardIds, "DataHub Shard Number can not be empty");
    this.selector.setShardList(shardIds);

    if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext())
        .isCheckpointingEnabled()) {
      LOG.warn(
          "Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
      batchFlushOnCheckpoint = false;
    }
  }

  @Override
  public void invoke(IN input, Context context) throws Exception {
    String shardId = selector.getShard(input);
    RecordEntry record = prepareRecord(shardId, input);

    // batch
    if (batchFlushOnCheckpoint) {
      batchList.add(record);
      if (batchList.size() >= batchSize) {
        synchronized (batchList) {
          flushSync();
        }
      }
    } else {
      putRecordWithRetry(Collections.singletonList(record));
    }
  }

  private RecordEntry prepareRecord(String shardId, IN input) {
    RecordEntry record = new RecordEntry(schema);
    serializer.serializeAttrs(record, input);
    serializer.serializeData(record, input);
    record.setShardId(shardId);
    return record;
  }

  private void flushSync() throws Exception {
    if (batchFlushOnCheckpoint) {
      synchronized (batchList) {
        putRecordWithRetry(batchList);
        batchList.clear();
      }
    }
  }

  private void putRecordWithRetry(List<RecordEntry> batchList) {
    RetryLoop retryLoop = new RetryLoop(new RetryForever(5));
    while (retryLoop.shouldContinue()) {
      try {
        client.putRecords(sinkProject, sinkTopic, batchList);
        retryLoop.markComplete();
      } catch (LimitExceededException e) {
        LOG.info("LimitExceeded error, Retry forever.");
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
