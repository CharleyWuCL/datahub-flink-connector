package charley.wu.flink.datahub.serialization;

import charley.wu.flink.datahub.serialization.basic.DataHubDeserializer;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
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
    TupleRecordData data = (TupleRecordData) record.getRecordData();

    Map<String, Object> map = new HashMap<>();
    schema.getFields().forEach(field -> map.put(field.getName(), data.getField(field.getName())));
    return map;
  }

  @Override
  public TypeInformation<Map> getProducedType() {
    return TypeInformation.of(Map.class);
  }
}
