package charley.wu.flink.datahub.coordinate.models;

import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ShardMeta {

  private Set<String> activeShardIds = new HashSet<>();
  private Map<String, String> addressMap = new HashMap<>();
  private boolean finished = true;

  public ShardMeta(ListShardResult listShardResult) {
    for (ShardEntry shardEntry : listShardResult.getShards()) {
      if (ShardState.ACTIVE.equals(shardEntry.getState())) {
        activeShardIds.add(shardEntry.getShardId());
      } else if (ShardState.OPENING.equals(shardEntry.getState()) ||
          ShardState.CLOSING.equals(shardEntry.getState())) {
        finished = false;
      }
      addressMap.put(shardEntry.getShardId(), shardEntry.getAddress());
    }
  }

  public Set<String> getActiveShardIds() {
    return activeShardIds;
  }

  public Map<String, String> getAddressMap() {
    return addressMap;
  }

  public boolean isFinished() {
    return finished;
  }
}
