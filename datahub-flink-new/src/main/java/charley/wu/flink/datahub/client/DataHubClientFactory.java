package charley.wu.flink.datahub.client;

import charley.wu.flink.datahub.config.DataHubConfig;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;

/**
 * DataHub Client Factory.
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class DataHubClientFactory {

  private DataHubConfig config;

  public DataHubClientFactory(DataHubConfig config) {
    this.config = config;
  }

  public DatahubClient create() throws Exception {
    DatahubConfig datahubConfig = config.buildDatahubConfig();
    HttpConfig httpConfig = config.buildHttpConfig();
    return DatahubClientBuilder.newBuilder()
        .setDatahubConfig(datahubConfig)
        .setHttpConfig(httpConfig)
        .build();
  }

}
