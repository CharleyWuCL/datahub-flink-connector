package charley.wu.flink.datahub.client;

import charley.wu.flink.datahub.config.DataHubConfig;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.model.CompressType;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.producer.Producer;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;

/**
 * Datahub Producer Factory.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class ProducerFactory implements Serializable {

  private final DataHubConfig connectorConfig;
  private final ProducerConfig producerConfig;

  private final String project;
  private final String topic;

  public ProducerFactory(DataHubConfig connectorConfig) {
    this.connectorConfig = connectorConfig;

    this.project = connectorConfig.getSinkProject();
    this.topic = connectorConfig.getSinkTopic();

    this.producerConfig = connectorConfig.buildProducerConfig();
    this.producerConfig.getHttpConfig().setCompressType(CompressType.LZ4);
  }

  public Producer create() {
    return new Producer(project, topic, producerConfig);
  }

  public RecordSchema schema() {
    DatahubConfig datahubConfig = connectorConfig.buildDatahubConfig();
    DatahubClient client = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig).build();
    return client.getTopic(project, topic).getRecordSchema();
  }

  public List<String> shardIds(){
    DatahubConfig datahubConfig = connectorConfig.buildDatahubConfig();
    DatahubClient client = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig).build();
    ListShardResult shardResult = client.listShard(project, topic);
    Validate.notNull(shardResult, "DataHub ListShardResult can not be null");
    List<String> shardIds = shardResult.getShards().stream().map(ShardEntry::getShardId)
        .collect(Collectors.toList());
    Validate.notEmpty(shardIds, "DataHub Shard Number can not be empty");
    return shardIds;
  }

}
