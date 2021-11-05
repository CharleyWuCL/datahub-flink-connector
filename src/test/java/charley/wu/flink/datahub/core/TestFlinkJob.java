package charley.wu.flink.datahub.core;

import charley.wu.flink.datahub.util.ConfigUtil;
import java.util.Properties;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class TestFlinkJob {

  public static void main(String[] args) throws Exception {
    Properties customParams = ConfigUtil.getPropertiesParams();
    ConnectorManager connector = new ConnectorManager(customParams);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint, default 180s.
    env.enableCheckpointing(180000);

    // Source data stream
    env.addSource(connector.createDataHubSource())
        .map(new Transfer())
        .addSink(connector.createDataHubSink());

    // Execute job.
    env.execute("job");
  }
}
