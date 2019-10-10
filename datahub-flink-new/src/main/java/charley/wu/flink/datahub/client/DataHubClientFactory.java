package charley.wu.flink.datahub.client;


import charley.wu.flink.datahub.config.DHConfig;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/7/11
 */
public class DataHubClientFactory {

  private DHConfig config;

  public DataHubClientFactory(DHConfig config) {
    this.config = config;
  }

  public DatahubClient create() {
    DatahubConfig datahubConfig = config.buildDatahubConfig();
    HttpConfig httpConfig = config.buildHttpConfig();
    return DatahubClientBuilder.newBuilder()
        .setDatahubConfig(datahubConfig)
        .setHttpConfig(httpConfig)
        .build();
  }

}
