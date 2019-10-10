package charley.wu.flink.datahub.client;


import charley.wu.flink.datahub.config.DHConfig;
import charley.wu.flink.datahub.coordinate.config.ConsumerConfig;
import charley.wu.flink.datahub.coordinate.consumer.Consumer;

/**
 * DataHub Consumer Factory.
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class ConsumerFactory {

  private static final int FETCH_SIZE = 100;

  private DHConfig connectorConfig;
  private ConsumerConfig consumerConfig;

  private String project;
  private String topic;
  private String subId;

  public ConsumerFactory(DHConfig connectorConfig) {
    this.connectorConfig = connectorConfig;
    this.consumerConfig = connectorConfig.buildConsumerConfig();
    this.consumerConfig.setFetchSize(FETCH_SIZE);

    this.project = connectorConfig.getSourceProject();
    this.topic = connectorConfig.getSourceTopic();
    this.subId = connectorConfig.getSubId();
  }

  public Consumer create() {
    return new Consumer(project, topic, subId, consumerConfig);
  }

}
