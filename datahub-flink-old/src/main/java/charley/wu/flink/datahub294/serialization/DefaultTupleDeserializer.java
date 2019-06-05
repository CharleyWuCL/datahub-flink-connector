package charley.wu.flink.datahub294.serialization;

import charley.wu.flink.datahub294.serialization.basic.DataHubDeserializer;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class DefaultTupleDeserializer implements DataHubDeserializer<Map> {

  private RecordSchema schema;

  public DefaultTupleDeserializer(RecordSchema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> deserializeKeys(RecordEntry record) {
    return record.getAttributes();
  }

  @Override
  public Map deserializeValue(RecordEntry record) {

    Map<String, Object> map = new HashMap<>();
    schema.getFields().forEach(field -> map.put(field.getName(), record.get(field.getName())));
    return map;
  }

  @Override
  public TypeInformation<Map> getProducedType() {
    return TypeInformation.of(Map.class);
  }
}
