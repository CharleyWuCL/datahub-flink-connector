package datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.Charsets;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class WriteDateExample {

  private DatahubClient datahubClient;

  public void writeTupleData() {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String shardId = "0";

    // Tuple Topic schema
    RecordSchema schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.STRING));
    schema.addField(new Field("field2", FieldType.BIGINT));

    // generate 10 records
    List<RecordEntry> recordEntries = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      RecordEntry recordEntry = new RecordEntry();
      // set attributes
      recordEntry.addAttribute("key1", "value1");
      // set tuple data
      TupleRecordData data = new TupleRecordData(schema);
      data.setField("field1", "HelloWorld");
      data.setField("field2", 1234567);
      recordEntry.setRecordData(data);
      recordEntries.add(recordEntry);
    }

    // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
    datahubClient.putRecordsByShard(projectName, topicName, shardId, recordEntries);
  }

  public void writeBlobData() {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String shardId = "0";

    // generate 10 records
    List<RecordEntry> recordEntries = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      RecordEntry recordEntry = new RecordEntry();
      // set attributes
      recordEntry.addAttribute("key1", "value1");
      // set blob data
      BlobRecordData data = new BlobRecordData("123456".getBytes(Charsets.UTF_8));
      recordEntry.setRecordData(data);
      recordEntries.add(recordEntry);
    }

    // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
    datahubClient.putRecordsByShard(projectName, topicName, shardId, recordEntries);
  }

}
