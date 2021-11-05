package charley.wu.flink.datahub.core;

import charley.wu.flink.datahub.serialization.Serializer;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;

public class DataHubSinkSerializer implements Serializer<Output> {

  @Override
  public RecordEntry serializeAttrs(RecordEntry record, Output output) {
    // set attributes
    record.addAttribute(BIZ_KEY, output.getName());
    return record;
  }

  @Override
  public void setEventTime(TupleRecordData data, Output output) {
    data.setField(EVENT_TIME, output.getCreateTime() * 1000);
  }
}
