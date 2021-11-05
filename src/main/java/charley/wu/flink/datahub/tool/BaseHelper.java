package charley.wu.flink.datahub.tool;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class BaseHelper {

  static final String ENDPOINT = "https://dh-cn-shanghai.aliyuncs.com";
  private final String accessId;
  private final String accessKey;
  protected boolean enablePb = false;
  protected DatahubClient client;

  public BaseHelper(String accessId, String accessKey) {
    this.accessId = accessId;
    this.accessKey = accessKey;
    this.createClient();
  }

  protected void createClient() {
    AliyunAccount account = new AliyunAccount(accessId, accessKey);

    this.client = DatahubClientBuilder.newBuilder()
        .setDatahubConfig(new DatahubConfig(ENDPOINT, account, enablePb)).build();
  }
}
