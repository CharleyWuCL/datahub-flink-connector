package charley.wu.flink.datahub294.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json util.
 *
 * @author Charley Wu
 * @since 2019/4/15
 */
public class JsonUtil {

  private static ObjectMapper mapper = createMapper();

  private static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }

  public static byte[] toByte(Object object) throws Exception {
    return mapper.writeValueAsBytes(object);
  }

  public static <T> T toObject(byte[] value, Class<T> clazz) throws Exception {
    return mapper.readValue(value, clazz);
  }
}
