package charley.wu.flink.datahub.coordinate.config;


import charley.wu.flink.datahub.coordinate.interceptor.EmptyInterceptor;
import charley.wu.flink.datahub.coordinate.interceptor.RecordInterceptor;
import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;

public abstract class BaseConfig {

  protected DatahubConfig datahubConfig;
  protected HttpConfig httpConfig = new HttpConfig();
  protected RecordInterceptor interceptor;

  public BaseConfig(String endpoint, Account account) {
    this.datahubConfig = new DatahubConfig(endpoint, account);
    this.interceptor = EmptyInterceptor.emptyInterceptor;
  }

  public BaseConfig(String endpoint, Account account, RecordInterceptor interceptor) {
    this.datahubConfig = new DatahubConfig(endpoint, account);
    this.interceptor = interceptor;
  }

  public DatahubConfig getDatahubConfig() {
    return datahubConfig;
  }

  public HttpConfig getHttpConfig() {
    return httpConfig;
  }

  public RecordInterceptor getInterceptor() {
    return interceptor;
  }
}
