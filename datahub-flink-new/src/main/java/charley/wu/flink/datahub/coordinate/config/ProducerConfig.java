package charley.wu.flink.datahub.coordinate.config;

import charley.wu.flink.datahub.coordinate.interceptor.RecordInterceptor;
import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.auth.AliyunAccount;

public class ProducerConfig extends BaseConfig {

  public ProducerConfig(String endpoint, String accessId, String accessKey) {
    super(endpoint, new AliyunAccount(accessId, accessKey));
    datahubConfig.setEnableBinary(true);
  }

  public ProducerConfig(String endpoint, String accessId, String accessKey, String securityToken) {
    super(endpoint, new AliyunAccount(accessId, accessKey, securityToken));
    datahubConfig.setEnableBinary(true);
  }

  protected ProducerConfig(String endpoint, Account account, RecordInterceptor interceptor) {
    super(endpoint, account, interceptor);
    datahubConfig.setEnableBinary(true);
  }
}
