package charley.wu.flink.datahub.core;

import charley.wu.flink.datahub.annotation.DataHubField;
import charley.wu.flink.datahub.annotation.DataHubTopic;
import com.aliyun.datahub.client.model.FieldType;
import java.io.Serializable;
import lombok.Data;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
@Data
@DataHubTopic(name = "tp_test", shardNum = 4, comment = "DataHub测试Topic")
public class Output implements Serializable {

  @DataHubField(alias = "name", type = FieldType.STRING)
  private String name;

  @DataHubField(alias = "age", type = FieldType.BIGINT)
  private Integer age;

  @DataHubField(alias = "money", type = FieldType.DOUBLE)
  private Double money;

  @DataHubField(alias = "create_time", type = FieldType.BIGINT)
  private Long createTime;
}
