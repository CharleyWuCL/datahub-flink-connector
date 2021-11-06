package charley.wu.flink.datahub.tool;

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class TopicCreator extends BaseHelper {

  public TopicCreator(String accessId, String accessKey) {
    super(accessId, accessKey);
  }

  /**
   *
   * @param project
   * @param topicClass
   * @param needEventTime
   * @param <T>
   * @throws Exception
   */
  public <T> void createTopic(String project, Class<T> topicClass, boolean needEventTime)
      throws Exception {
    Tuple3<String, Integer, String> topicInfo = TopicHelper.getTopicInfo(topicClass);
    String topic = topicInfo.f0;
    int shardNum = topicInfo.f1;
    String topicComment = topicInfo.f2;

    List<Tuple3<String, FieldType, String>> columnsInfo = TopicHelper.getFieldInfoList(topicClass);

    RecordSchema schema = new RecordSchema();
    columnsInfo.forEach(fieldInfo -> {
      String field = fieldInfo.f0;
      FieldType type = fieldInfo.f1;
      String fieldComment = fieldInfo.f2;
      schema.addField(new Field(field, type, fieldComment));
    });
    if (needEventTime && !schema.containsField("event_time")) {
      schema.addField(new Field("event_time", FieldType.TIMESTAMP, "事件时间"));
    }

    this.client.createTopic(
        project,
        topic,
        shardNum,
        3,
        RecordType.TUPLE,
        schema,
        topicComment);
  }
}
