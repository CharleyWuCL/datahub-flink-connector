package charley.wu.flink.connector.datahub.config;

import charley.wu.flink.connector.datahub.utils.PropsUtils;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.http.HttpConfig.CompressType;
import java.io.Serializable;
import java.util.Properties;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/4/30
 */
public class DataHubConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ENDPOINT = "datahub.endpoint";  // DataHub服务地址
  public static final String ACCESS_ID = "datahub.accessId";  // 阿里云账号ID
  public static final String ACCESS_KEY = "datahub.accessKey";  // 阿里云账号Secret
  public static final String ENABLE_BINARY = "datahub.enableBinary"; // 是否采用二进制传输，服务端从2.12版本开始支持，之前版本需设置为false，专有云使用时出现错误’Parse body failed, Offset: 0’，尝试设置为false
  private static final boolean DEFAULT_ENABLE_BINARY = true;

  public static final String READ_TIMEOUT = "http.readTimeout";  // Socket读写超时时间，默认10s
  public static final String CONN_TIMEOUT = "http.connTimeout";  // TCP连接超时时间，默认10s
  public static final String MAX_CONN_PER_ROUTE = "http.maxConnPerRoute";  // 同一个Endpoint最大连接数，默认10
  public static final String MAX_CONN_TOTAL = "http.maxConnTotal"; // Client最大连接数，默认10
  public static final String MAX_RETRY_COUNT = "http.maxRetryCount"; // 请求失败重试，默认1，不建议修改，重试由上层业务层处理
  public static final String DEBUG_REQUEST = "http.debugRequest";  // 是否打印请求日志信息，默认false
  public static final String COMPRESS_TYPE = "http.compressType";  // 数据传输压缩方式，默认不压缩，支持lz4， deflate压缩
  public static final String PROXY_URI = "http.proxyUri";  // 代理服务器主机地址
  public static final String PROXY_USERNAME = "http.proxyUsername";  // 代理服务器验证的用户名
  public static final String PROXY_PASSWORD = "http.proxyPassword";  // 代理服务器验证的密码

  public static final String DEFAULT_PROJECT_KEY = "default.project";  // 默认Project名
  public static final String SOURCE_PROJECT_KEY = "source.project";  // Datahub作为数据源的Project
  public static final String SOURCE_TOPIC_KEY = "source.topic";  // Datahub作为数据源的Topic
  public static final String SOURCE_SUBID_KEY = "source.subid";  // Datahub作为数据源的SubId
  public static final String SINK_PROJECT_KEY = "sink.project";  // Datahub作为输出仓库的Project
  public static final String SINK_TOPIC_KEY = "sink.topic";  // Datahub作为输出仓库的Topic

  private Properties prop;

  private String defaultProject;
  private String sourceProject;
  private String sinkProject;
  private String sourceTopic;
  private String sinkTopic;

  public DataHubConfig(Properties properties) {
    this.prop = properties;

    this.defaultProject = properties.getProperty(DEFAULT_PROJECT_KEY);
    if (!Strings.isNullOrEmpty(this.defaultProject)) {
      this.sourceProject = this.defaultProject;
      this.sinkProject = this.defaultProject;
    }

    String configSourceProject = properties.getProperty(SOURCE_PROJECT_KEY);
    if (!Strings.isNullOrEmpty(configSourceProject)) {
      this.sourceProject = configSourceProject;
    }

    String configSinkProject = properties.getProperty(SINK_PROJECT_KEY);
    if (!Strings.isNullOrEmpty(configSinkProject)) {
      this.sinkProject = configSinkProject;
    }

    //TODO Check project name.

    this.sourceTopic = this.prop.getProperty(SOURCE_TOPIC_KEY);
    this.sinkTopic = this.prop.getProperty(SINK_TOPIC_KEY);
  }

  public DatahubConfig buildDatahubConfig() throws ConfigException {
    String endpoint = prop.getProperty(ENDPOINT);
    if (Strings.isNullOrEmpty(endpoint)) {
      throw new ConfigException("Endpoint can not be null.");
    }

    String accessId = prop.getProperty(ACCESS_ID);
    if (Strings.isNullOrEmpty(accessId)) {
      throw new ConfigException("AccessId can not be null.");
    }

    String accessKey = prop.getProperty(ACCESS_KEY);
    if (Strings.isNullOrEmpty(accessKey)) {
      throw new ConfigException("AccessKey can not be null.");
    }

    boolean enableBinary = PropsUtils.getBoolean(prop, ENABLE_BINARY, DEFAULT_ENABLE_BINARY);

    AliyunAccount account = new AliyunAccount(accessId, accessKey);
    return new DatahubConfig(endpoint, account, enableBinary);
  }

  public HttpConfig buildHttpConfig() {
    HttpConfig config = new HttpConfig();

    String readTimeout = prop.getProperty(READ_TIMEOUT);
    if (!Strings.isNullOrEmpty(readTimeout)) {
      config.setReadTimeout(Integer.parseInt(readTimeout));
    }

    String connTimeout = prop.getProperty(CONN_TIMEOUT);
    if (!Strings.isNullOrEmpty(connTimeout)) {
      config.setConnTimeout(Integer.parseInt(connTimeout));
    }

    String maxConnPerRoute = prop.getProperty(MAX_CONN_PER_ROUTE);
    if (!Strings.isNullOrEmpty(maxConnPerRoute)) {
      config.setMaxConnPerRoute(Integer.parseInt(maxConnPerRoute));
    }

    String maxConnTotal = prop.getProperty(MAX_CONN_TOTAL);
    if (!Strings.isNullOrEmpty(maxConnTotal)) {
      config.setMaxConnTotal(Integer.parseInt(maxConnTotal));
    }

    String maxRetryCount = prop.getProperty(MAX_RETRY_COUNT);
    if (!Strings.isNullOrEmpty(maxRetryCount)) {
      config.setMaxRetryCount(Integer.parseInt(maxRetryCount));
    }

    String debugRequest = prop.getProperty(DEBUG_REQUEST);
    if (!Strings.isNullOrEmpty(debugRequest)) {
      config.setDebugRequest(Boolean.parseBoolean(debugRequest));
    }

    String compressType = prop.getProperty(COMPRESS_TYPE);
    if (!Strings.isNullOrEmpty(debugRequest)) {
      config.setCompressType(CompressType.valueOf(compressType));
    }

    String proxyUri = prop.getProperty(PROXY_URI);
    if (!Strings.isNullOrEmpty(proxyUri)) {
      config.setProxyUri(proxyUri);
    }

    String proxyUsername = prop.getProperty(PROXY_USERNAME);
    if (!Strings.isNullOrEmpty(proxyUsername)) {
      config.setProxyUsername(proxyUsername);
    }

    String proxyPassword = prop.getProperty(PROXY_PASSWORD);
    if (!Strings.isNullOrEmpty(proxyPassword)) {
      config.setProxyPassword(proxyPassword);
    }
    return config;
  }

  public String getDefaultProject() {
    return defaultProject;
  }

  public void setDefaultProject(String defaultProject) {
    this.defaultProject = defaultProject;
  }

  public String getSourceProject() {
    return sourceProject;
  }

  public void setSourceProject(String sourceProject) {
    this.sourceProject = sourceProject;
  }

  public String getSinkProject() {
    return sinkProject;
  }

  public void setSinkProject(String sinkProject) {
    this.sinkProject = sinkProject;
  }

  public String getSourceTopic() {
    return sourceTopic;
  }

  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }

  public String getSinkTopic() {
    return sinkTopic;
  }

  public void setSinkTopic(String sinkTopic) {
    this.sinkTopic = sinkTopic;
  }
}
