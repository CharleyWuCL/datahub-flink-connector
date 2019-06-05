package charley.wu.flink.datahub294.selector;

import java.util.List;

/**
 * Default shard selector.
 *
 * @author Charley Wu
 * @since 2019/4/30
 */
public class DefaultShardSelector<T> implements ShardSelector<T> {

  @Override
  public void setShardList(List<String> shards) {
  }

  @Override
  public String getShard(T value) {
    return "0";
  }
}
