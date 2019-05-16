package datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class CreateTopicExample {

  private DatahubClient datahubClient;

  public void TupleTopicCreate(String[] args) {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String comment = "test tuple topic";
    int shardCount = 1;
    int lifeCycle = 1;

    // topic schema
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.STRING));
    schema.addField(new Field("field2", FieldType.BIGINT));
    datahubClient
        .createTopic(projectName, topicName, shardCount, lifeCycle, RecordType.TUPLE, schema,
            comment);
  }

  public void BlobTopicCreate() {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String comment = "test blob topic";
    int shardCount = 1;
    int lifeCycle = 1;
    datahubClient
        .createTopic(projectName, topicName, shardCount, lifeCycle, RecordType.BLOB, comment);
  }

}
