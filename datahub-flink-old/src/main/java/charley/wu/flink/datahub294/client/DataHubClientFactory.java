package charley.wu.flink.datahub294.client;


import charley.wu.flink.datahub294.config.DataHubConfig;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;

/**
 * DataHub Client Factory.
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class DataHubClientFactory {

  private final DataHubConfig config;

  public DataHubClientFactory(DataHubConfig config) {
    this.config = config;
  }

  public DatahubClient create() throws Exception {
    DatahubConfiguration datahubConfig = config.buildDatahubConfig();
    return new DatahubClient(datahubConfig);
  }

}
