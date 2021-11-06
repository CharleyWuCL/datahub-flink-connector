# DataHub Flink Connector

## 概述

GitHub：https://github.com/CharleyWuCL/datahub-flink-connector

Author: Charley Wu

E-Mail: charleywu@aliyun.com

## 简介

我们在阿里云上使用DataHub作为Flink程序输入输出的消息队列，使用成本比较低，但由于是阿里云的云产品，周边生态做的不是很好，Flink Stream的Connector并没有开源出来。因此本人参照RocketMQ Flink Connector写了DataHub的Flink Connector。

DataHub主要提供两个SDK，`aliyun-sdk-datahub`和`datahub-client-library`；前者是较为基础的SDK，提供了丰富的DataHub操作接口，使用较为复杂，需要对DataHub有较为深入的了解；后者在前者的基础上进行了封装，提供Consumer和Producer进行协同消费及生产。本SDK使用的是后者。

## 依赖介绍

- Flink：org.apache.flink:flink-streaming-java_2.12:1.13.1
- datahub-client-library: com.aliyun.datahub:datahub-client-library:1.2.0-public
- aliyun-sdk-datahub: com.aliyun.datahub:aliyun-sdk-datahub:2.21.6-public

```xml
 <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <java.version>1.8</java.version>

    <lombok.version>1.18.22</lombok.version>

    <flink.version>1.13.1</flink.version>
    <scala.binary.version>2.12</scala.binary.version>

    <datahub-sdk.version>2.21.6-public</datahub-sdk.version>
    <datahub-library.version>1.2.0-public</datahub-library.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.projectlombok</groupId>
      <artifactId>lombok</artifactId>
      <version>${lombok.version}</version>
    </dependency>

    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
      <version>${flink.version}</version>
      <scope>compile</scope>
    </dependency>

    <dependency>
      <groupId>com.aliyun.datahub</groupId>
      <artifactId>datahub-client-library</artifactId>
      <version>${datahub-library.version}</version>
      <exclusions>
        <exclusion>
          <groupId>com.aliyun.datahub</groupId>
          <artifactId>aliyun-sdk-datahub</artifactId>
        </exclusion>
      </exclusions>
    </dependency>

    <dependency>
      <groupId>com.aliyun.datahub</groupId>
      <artifactId>aliyun-sdk-datahub</artifactId>
      <version>${datahub-sdk.version}</version>
    </dependency>
  </dependencies>
```

## Connector使用

### 依赖

目前还没有上传中央仓库，需使用者自行下载代码进行编译。

```xml
<dependency>
    <groupId>charley.wu</groupId>
    <artifactId>datahub-flink-connector</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### Source

```java
  /**
   * DataHub Source
   *
   * @return new DataHubSource.
   */
  public DataHubSource<Input> createDataHubSource() {
    // Validate common params.
    Validate.isTrue(props.containsKey(DataHubConfig.ENDPOINT), "DataHub endpoint can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_ID), "DataHub accessId can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_KEY), "DataHub accessKey can not be null");

    // Validate source params.
    Validate.isTrue(props.containsKey(DataHubConfig.SOURCE_PROJECT), "DataHub project can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SOURCE_TOPIC), "DataHub topic can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SOURCE_SUBID), "DataHub subId can not be null");

    // Return new DataHub Source.
    return new DataHubSource<>(props, new DataHubSourceDeserializer());
  }
```

### Sink

```java
public DataHubSink<Output> createDataHubSink() {
    Validate.isTrue(props.containsKey(DataHubConfig.ENDPOINT), "DataHub endpoint can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_ID), "DataHub accessId can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.ACCESS_KEY), "DataHub accessKey can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SINK_PROJECT), "DataHub project can not be null");
    Validate.isTrue(props.containsKey(DataHubConfig.SINK_TOPIC), "DataHub topic can not be null");

    DataHubSink<Output> sink = new DataHubSink<>(props, new DataHubSinkSerializer(), new DataHubShardSelector());
    boolean batchEnable = ConfigUtil.getBoolean(props, DataHubConfig.BATCH_ENABLE, DataHubConfig.DEFAULT_BATCH_ENABLE);
    sink.setBatchFlushOnCheckpoint(batchEnable);
    return sink;
  }
```

### Job

```java
/**
 * Test Job
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
```

### 案例链接

https://github.com/CharleyWuCL/datahub-flink-connector/tree/master/src/test/java/charley/wu/flink/datahub/core

## DataHub工具

使用DataHub过程中，发现在控制台进行Topic创建是一个非常痛苦的过程，因此写了一个小工具，帮助创建Tpoic。

### 注解

将DataHub Topic进行实体化，提供两个注解定义Topic及Field。

#### @DataHubTopic

```java
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataHubTopic {

  // Topic名称
  String name();

  // Topic分片数量
  int shardNum();

  // Topic描述
  String comment();
}
```

#### @DataHubField

```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DataHubField {

  // 别名，对应下划线分割的字段名
  String alias();

  // 类型，DataHub字段类型
  FieldType type();

  // 字段描述
  String comment() default "";
}
```

### TopicCreator

```java
// 1、构造函数
// 主要传入访问DataHub的阿里云AccessID和AccessKey
public TopicCreator(String accessId, String accessKey) 

// 2、创建方法
/**
   *  Topic 创建方法
   *
   * @param project					Topic所在DataHub项目名称
   * @param topicClass			 Topic对应实体类Class
   * @param needEventTime	   是否增加event_time，Timestamp类型，微秒um
   * @param <T>					     实体类泛型	 
   * @throws Exception			 异常
   */
public <T> void createTopic(String project, Class<T> topicClass, boolean needEventTime)
```

### 使用

#### 定义实体类

```java
@Data
@DataHubTopic(name = "tp_test", shardNum = 4, comment = "DataHub测试Topic")
public class TestTopic implements Serializable {

  @DataHubField(alias = "name", type = FieldType.STRING)
  private String name;

  @DataHubField(alias = "age", type = FieldType.BIGINT)
  private Integer age;

  @DataHubField(alias = "money", type = FieldType.DOUBLE)
  private Double money;

  @DataHubField(alias = "create_time", type = FieldType.STRING)
  private String createTime;
}
```

#### 创建方法

```java
public class TopicCreatorTest{

  public static void main(String[] args) {
    try {
      TopicCreator creator = new TopicCreator("阿里云AccessId", "阿里云AccessKey");
      creator.createTopic("test_project", TestMessage.class, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
```

