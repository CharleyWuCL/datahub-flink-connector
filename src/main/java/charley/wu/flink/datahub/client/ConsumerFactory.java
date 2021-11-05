package charley.wu.flink.datahub.client;

import charley.wu.flink.datahub.config.DataHubConfig;
import com.aliyun.datahub.client.model.CompressType;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import java.io.Serializable;

/**
 * DataHub Consumer Factory.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class ConsumerFactory implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final int FETCH_SIZE = 128;

  private final ConsumerConfig consumerConfig;

  private final String project;
  private final String topic;
  private final String subId;

  public ConsumerFactory(DataHubConfig connectorConfig) {
    this.consumerConfig = connectorConfig.buildConsumerConfig();
    this.consumerConfig.setFetchSize(FETCH_SIZE);
    this.consumerConfig.getHttpConfig().setCompressType(CompressType.LZ4);

    this.project = connectorConfig.getSourceProject();
    this.topic = connectorConfig.getSourceTopic();
    this.subId = connectorConfig.getSubId();
  }

  public Consumer create() {
    return new Consumer(project, topic, subId, consumerConfig);
  }

}
