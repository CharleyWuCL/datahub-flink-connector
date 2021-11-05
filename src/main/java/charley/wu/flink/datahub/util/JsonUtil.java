package charley.wu.flink.datahub.util;

import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json util.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class JsonUtil {

  private static final ObjectMapper mapper = createMapper();

  private static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }

  public static byte[] toByte(Object object) throws Exception {
    return mapper.writeValueAsBytes(object);
  }

  public static String toJson(Object obj) throws Exception {
    return mapper.writeValueAsString(obj);
  }

  public static <T> T toObject(byte[] value, Class<T> clazz) throws Exception {
    return mapper.readValue(value, clazz);
  }

  public static <T> T toObject(String value, Class<T> clazz) throws Exception {
    return mapper.readValue(value, clazz);
  }

  public static <T> T toObject(Map<String, Object> value, Class<T> clazz) throws Exception {
    return mapper.convertValue(value, clazz);
  }

  public static <T> T toObject(String value, TypeReference<T> reference) throws Exception {
    return mapper.readValue(value, reference);
  }
}
