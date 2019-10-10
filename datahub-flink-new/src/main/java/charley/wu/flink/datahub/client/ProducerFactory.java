package charley.wu.flink.datahub.client;


import charley.wu.flink.datahub.config.DHConfig;
import charley.wu.flink.datahub.coordinate.config.ProducerConfig;
import charley.wu.flink.datahub.coordinate.producer.Producer;

/**
 * Datahub Producer Factory.
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class ProducerFactory {

  private DHConfig connectorConfig;
  private ProducerConfig producerConfig;

  private String project;
  private String topic;
  private String subId;

  public ProducerFactory(DHConfig connectorConfig) {
    this.connectorConfig = connectorConfig;
    this.producerConfig = connectorConfig.buildProducerConfig();
    this.project = connectorConfig.getSourceProject();
    this.topic = connectorConfig.getSourceTopic();
    this.subId = connectorConfig.getSubId();
  }

  public Producer create() {
    return new Producer(project, topic, producerConfig);
  }

}
