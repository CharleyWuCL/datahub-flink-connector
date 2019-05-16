package charley.wu.flink.connector.datahub.serialization;

import charley.wu.flink.connector.datahub.serialization.basic.DataHubDeserializer;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import java.util.Map;
import org.apache.commons.codec.Charsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class DefaultBlobDeserializer implements DataHubDeserializer<String> {

  private RecordSchema schema;

  public DefaultBlobDeserializer(RecordSchema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> deserializeKeys(RecordEntry record) {
    return record.getAttributes();
  }

  @Override
  public String deserializeValue(RecordEntry record) {
    BlobRecordData data = (BlobRecordData) record.getRecordData();
    return new String(data.getData(), Charsets.UTF_8);
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return TypeInformation.of(String.class);
  }
}
