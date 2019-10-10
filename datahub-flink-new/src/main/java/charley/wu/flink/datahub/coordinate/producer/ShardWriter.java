package charley.wu.flink.datahub.coordinate.producer;

import charley.wu.flink.datahub.coordinate.common.ClientManager;
import charley.wu.flink.datahub.coordinate.common.ClientManagerFactory;
import charley.wu.flink.datahub.coordinate.config.ProducerConfig;
import com.aliyun.datahub.client.model.RecordEntry;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShardWriter {

  private ClientManager clientManager;
  private String projectName;
  private String topicName;
  private String shardId;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  ShardWriter(String projectName, String topicName, String shardId, ProducerConfig config) {
    this.clientManager = ClientManagerFactory
        .getClientManager(projectName, topicName, config.getDatahubConfig());
    this.projectName = projectName;
    this.topicName = topicName;
    this.shardId = shardId;
  }

  void write(List<RecordEntry> records) {
    clientManager.getClient(shardId).putRecordsByShard(projectName, topicName, shardId, records);
  }

  void close() {
    if (closed.compareAndSet(false, true)) {
      if (clientManager != null) {
        clientManager.close();
      }
    }
  }
}
