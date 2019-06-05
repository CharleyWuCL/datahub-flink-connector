package charley.wu.flink.datahub294.config;

import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
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

  public static final String DEFAULT_PROJECT = "datahub.default.project";  // 默认Project名
  public static final String SOURCE_PROJECT = "datahub.source.project";  // Datahub作为数据源的Project
  public static final String SOURCE_TOPIC = "datahub.source.topic";  // Datahub作为数据源的Topic
  public static final String SOURCE_SUBID = "datahub.source.subId";  // Datahub作为数据源的SubId
  public static final String SINK_PROJECT = "datahub.sink.project";  // Datahub作为输出仓库的Project
  public static final String SINK_TOPIC = "datahub.sink.topic";  // Datahub作为输出仓库的Topic

  private Properties prop;

  private String defaultProject;
  private String sourceProject;
  private String sinkProject;
  private String sourceTopic;
  private String sinkTopic;

  public DataHubConfig(Properties properties) {
    this.prop = properties;

    this.defaultProject = properties.getProperty(DEFAULT_PROJECT);
    if (!Strings.isNullOrEmpty(this.defaultProject)) {
      this.sourceProject = this.defaultProject;
      this.sinkProject = this.defaultProject;
    }

    String configSourceProject = properties.getProperty(SOURCE_PROJECT);
    if (!Strings.isNullOrEmpty(configSourceProject)) {
      this.sourceProject = configSourceProject;
    }

    String configSinkProject = properties.getProperty(SINK_PROJECT);
    if (!Strings.isNullOrEmpty(configSinkProject)) {
      this.sinkProject = configSinkProject;
    }

    this.sourceTopic = this.prop.getProperty(SOURCE_TOPIC);
    this.sinkTopic = this.prop.getProperty(SINK_TOPIC);
  }

  public DatahubConfiguration buildDatahubConfig() throws ConfigException {
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

    AliyunAccount account = new AliyunAccount(accessId, accessKey);

    return new DatahubConfiguration(account, endpoint);
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
