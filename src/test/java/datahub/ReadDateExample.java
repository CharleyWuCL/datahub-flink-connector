package datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.apache.commons.codec.Charsets;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class ReadDateExample {

  private DatahubClient datahubClient;

  public void readTupleData() {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String shardId = "0";
    int recordLimit = 10;

    // Tuple Topic schema
    RecordSchema schema;
    schema = new RecordSchema();
    schema.addField(new Field("field1", FieldType.STRING));
    schema.addField(new Field("field2", FieldType.BIGINT));

    // 获取cursor, 这里获取最新的一条记录
    // 注: 正常情况下，getCursor只需在初始化时获取一次，然后使用getRecords的nextCursor进行下一次读取
    String cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.LATEST)
        .getCursor();

    // 读取数据
    GetRecordsResult result = datahubClient
        .getRecords(projectName, topicName, shardId, schema, cursor, recordLimit);

    // 如果有数据则处理，无数据需sleep后重新读取
    if (result.getRecordCount() > 0) {
      for (RecordEntry entry : result.getRecords()) {
        TupleRecordData data = (TupleRecordData) entry.getRecordData();
        System.out.println("field1:" + data.getField("field1"));
        System.out.println("field2:" + data.getField("field2"));
      }
    }
  }

  public void readBlobData() {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String shardId = "0";
    int recordLimit = 10;

    // 获取cursor, 这里获取最新的一条记录
    // 注: 正常情况下，getCursor只需在初始化时获取一次，然后使用getRecords的nextCursor进行下一次读取
    String cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.LATEST)
        .getCursor();

    // 读取数据
    GetRecordsResult result = datahubClient
        .getRecords(projectName, topicName, shardId, cursor, recordLimit);

    // 如果有数据则处理，无数据需sleep后重新读取
    if (result.getRecordCount() > 0) {
      for (RecordEntry entry : result.getRecords()) {
        BlobRecordData data = (BlobRecordData) entry.getRecordData();
        System.out.println("value:" + new String(data.getData(), Charsets.UTF_8));
      }
    }
  }
}
