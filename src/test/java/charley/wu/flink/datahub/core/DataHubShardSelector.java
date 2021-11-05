package charley.wu.flink.datahub.core;

import charley.wu.flink.datahub.selector.ShardSelector;
import java.util.List;

/**
 * Default shard selector.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class DataHubShardSelector implements ShardSelector<Output> {

  private List<String> shards;
  private int shardNum;

  @Override
  public void setShardList(List<String> shards) {
    this.shards = shards;
    this.shardNum = shards.size();
  }

  @Override
  public String getShard(Output status) {
    String mac = status.getName();
    int index = Math.abs(mac.hashCode() % shardNum);
    return String.valueOf(shards.get(index));
  }
}
