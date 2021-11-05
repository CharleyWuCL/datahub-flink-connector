package charley.wu.flink.datahub.tool;

import charley.wu.flink.datahub.annotation.DataHubField;
import charley.wu.flink.datahub.annotation.DataHubTopic;
import com.aliyun.datahub.client.model.FieldType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
@Slf4j
public class TopicHelper {

  public static <T> Tuple3<String, Integer, String> getTopicInfo(Class<T> topicClass) throws Exception {
    DataHubTopic ann = topicClass.getAnnotation(DataHubTopic.class);
    if (Objects.isNull(ann)) {
      throw new Exception("DataHubTopic is null, " + topicClass.getName());
    }
    return new Tuple3<>(ann.name(), ann.shardNum(), ann.comment());
  }

  public static <T> List<Tuple3<String, FieldType, String>> getFieldInfoList(Class<T> topicClass)
      throws Exception {
    List<Tuple3<String, FieldType, String>> fieldInfoList = new ArrayList<>();

    Field[] fields = topicClass.getDeclaredFields();
    for (Field field : fields) {
      if (field.isSynthetic()) {
        continue;
      }
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }
      String fieldName = field.getName();
      DataHubField ann = field.getAnnotation(DataHubField.class);
      if (Objects.isNull(ann)) {
        throw new Exception("DataHubField is null, " + fieldName);
      }
      checkType(fieldName, field.getType().getName(), ann.type());
      fieldInfoList.add(new Tuple3<>(ann.alias(), ann.type(), ann.comment()));
    }
    return fieldInfoList;
  }

  private static void checkType(String field, String javaType, FieldType dhType) {
    switch (javaType) {
      case "java.lang.String":
        compare(field, FieldType.STRING, dhType);
        break;
      case "java.lang.Integer":
      case "java.lang.Long":
        compare(field, FieldType.BIGINT, dhType);
        break;
      case "java.lang.Double":
        compare(field, FieldType.DOUBLE, dhType);
        break;
      default:
        throw new RuntimeException("Unsupported type: " + javaType);
    }
  }

  private static void compare(String field, FieldType expect, FieldType actual) {
    if("eventTime".equals(field)){
      if(FieldType.TIMESTAMP == actual){
        return;
      }else{
        log.error("Field event_time type must be FieldType.TIMESTAMP, actual:{}", actual);
        throw new RuntimeException("Type not match.");
      }
    }

    if (!(expect == actual)) {
      log.error("Type not match, field:{}, expect:{}, actual:{}", field, expect, actual);
      throw new RuntimeException("Type not match.");
    }
  }
}
