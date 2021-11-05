package charley.wu.flink.datahub.core;

import charley.wu.flink.datahub.DataHubSink;
import charley.wu.flink.datahub.DataHubSource;
import charley.wu.flink.datahub.config.DataHubConfig;
import charley.wu.flink.datahub.util.ConfigUtil;
import java.util.Properties;
import org.apache.commons.lang3.Validate;

/**
 * Connector Factory.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class ConnectorManager {

  private final Properties props;

  public ConnectorManager(Properties props) {
    this.props = props;
  }

  /**
   * DataHub Source
   *
   * @return new DataHubSource.
   */
  public DataHubSource<Input> createDataHubSource() {
    // Validate common params.
    Validate.isTrue(props.containsKey(DataHubConfig.ENDPOINT),
        "DataHub endpoint can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_ID),
        "DataHub accessId can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_KEY),
        "DataHub accessKey can not be null");

    // Validate source params.
    Validate.isTrue(props.containsKey(DataHubConfig.SOURCE_PROJECT),
        "DataHub project can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SOURCE_TOPIC),
        "DataHub topic can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SOURCE_SUBID),
        "DataHub subId can not be null");

    // Return new DataHub Source.
    return new DataHubSource<>(props, new DataHubSourceDeserializer());
  }

  public DataHubSink<Output> createDataHubSink() {
    Validate.isTrue(props.containsKey(DataHubConfig.ENDPOINT), "DataHub endpoint can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_ID), "DataHub accessId can not be null");
    Validate
        .isTrue(props.containsKey(DataHubConfig.ACCESS_KEY), "DataHub accessKey can not be null");
    Validate
        .isTrue(props.containsKey(DataHubConfig.SINK_PROJECT), "DataHub project can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SINK_TOPIC), "DataHub topic can not be null");

    DataHubSink<Output> sink = new DataHubSink<>(props, new DataHubSinkSerializer(),
        new DataHubShardSelector());
    boolean batchEnable = ConfigUtil.getBoolean(props, DataHubConfig.BATCH_ENABLE,
        DataHubConfig.DEFAULT_BATCH_ENABLE);
    sink.setBatchFlushOnCheckpoint(batchEnable);
    return sink;
  }
}

