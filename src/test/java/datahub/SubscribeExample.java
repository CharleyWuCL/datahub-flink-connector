package datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.SubscriptionOfflineException;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.OpenSubscriptionSessionResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/14
 */
public class SubscribeExample {

  private DatahubClient client;

  public void main(String[] args) {
    String projectName = "<YourProjectName>";
    String topicName = "<YourTopicName>";
    String subId = "<YourSubId>";
    final String shardId = "0";

    // 1. 如果使用点位服务，首先需要openSubscriptionSession获取订阅的sessionId和versionId信息，
    //    OpenSession只需初始化一次
    List<String> shardIds = new ArrayList<>();
    shardIds.add(shardId);
    OpenSubscriptionSessionResult openSubscriptionSessionResult = client
        .openSubscriptionSession(projectName, topicName, subId, shardIds);

    // 2. 获取对应shard的点位信息，并获取到用于下次读取数据的Cursor信息
    SubscriptionOffset subOffset = openSubscriptionSessionResult.getOffsets().get(shardId);
    String cursor = null;
    if (subOffset.getSequence() >= 0) {
      // 获取下一条记录的Cursor
      long nextSequence = subOffset.getSequence() + 1;
      // 备注：如果按照SEQUENCE getCursor报SeekOutOfRange错误，需要回退到按照SYSTEM_TIME或者OLDEST/LATEST进行getCursor
      cursor = client.getCursor(projectName, topicName, shardId, CursorType.SEQUENCE, nextSequence)
          .getCursor();
    } else {
      // 获取最旧数据的Cursor
      cursor = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
    }

    // 3. 读取并保存点位，这里以读取BLOB数据为例，并且每1000条记录保存一次点位
    int fetchNum = 10;
    long recordCount = 0L;
    RecordSchema schema = client.getTopic(projectName, topicName).getRecordSchema();
    while (true) {
      try {
        GetRecordsResult getRecordsResult = client
            .getRecords(projectName, topicName, shardId, schema, cursor, fetchNum);
        if (getRecordsResult.getRecordCount() <= 0) {
          // 无数据，sleep后读取
          Thread.sleep(1000);
          continue;
        }
        for (RecordEntry recordEntry : getRecordsResult.getRecords()) {
          // TODO: 处理数据
          // 处理数据完成后，设置点位
          ++recordCount;
          subOffset.setSequence(recordEntry.getSequence());
          subOffset.setTimestamp(recordEntry.getSystemTime());
          if (recordCount % 1000 == 0) {
            Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
            offsetMap.put(shardId, subOffset);
            client.commitSubscriptionOffset(projectName, topicName, subId, offsetMap);
          }
        }
        cursor = getRecordsResult.getNextCursor();
      } catch (SubscriptionOfflineException | OffsetSessionChangedException e) {
        // 退出. Offline: 订阅下线; SessionChange: 表示订阅被其他客户端同时消费
        break;
      } catch (OffsetResetedException e) {
        // 表示点位被重置，重新获取SubscriptionOffset信息，这里以Sequence重置为例
        // 如果以Timestamp重置，需要通过CursorType.SYSTEM_TIME获取cursor
        subOffset = client.getSubscriptionOffset(projectName, topicName, subId, shardIds)
            .getOffsets().get(shardId);
        long nextSequence = subOffset.getSequence() + 1;
        cursor = client
            .getCursor(projectName, topicName, shardId, CursorType.SEQUENCE, nextSequence)
            .getCursor();
      } catch (DatahubClientException e) {
        // TODO: 针对不同异常决定是否退出
      } catch (Exception e) {
        break;
      }
    }
  }
}
