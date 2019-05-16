package datahub;

import charley.wu.flink.connector.datahub.utils.JsonUtil;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import org.apache.commons.codec.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/5
 */
public class ClientExample {

  public static void main(String[] args) {

    // Endpoint以Region: 华东1为例，其他Region请按实际情况填写
    String endpoint = "";
    String accessId = "";
    String accessKey = "";

    // 创建DataHubClient实例
    DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
        .setDatahubConfig(
            new DatahubConfig(endpoint,
                // 是否开启二进制传输，服务端2.12版本开始支持
                new AliyunAccount(accessId, accessKey), true))
        //专有云使用出错尝试将参数设置为       false
        // HttpConfig可不设置，不设置时采用默认值
        .setHttpConfig(new HttpConfig().setConnTimeout(10000))
        .build();

    String projectName = "";
    String topicName = "";
    String shardId = "0";
    int recordLimit = 100;

    // 获取cursor, 这里获取最新的一条记录
    // 注: 正常情况下，getCursor只需在初始化时获取一次，然后使用getRecords的nextCursor进行下一次读取
    String cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.LATEST)
        .getCursor();

    while (true){
      try {
        // 读取数据
        GetRecordsResult result = datahubClient
            .getRecords(projectName, topicName, shardId, cursor, recordLimit);

        // 如果有数据则处理，无数据需sleep后重新读取
        if (result.getRecordCount() > 0) {
          for (RecordEntry entry : result.getRecords()) {
            BlobRecordData data = (BlobRecordData) entry.getRecordData();
            HeartBeat hb = JsonUtil.toObject(data.getData(), HeartBeat.class);
            if(Strings.isNullOrEmpty(hb.getCabinetMac())){
              System.out.println("value:" + new String(data.getData(), Charsets.UTF_8));
            }
          }
        }
      }catch (Exception e){
        e.printStackTrace();
      }
    }
  }
}
