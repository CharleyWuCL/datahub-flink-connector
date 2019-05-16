package charley.wu.flink.connector.datahub.selector;

/**
 * Default shard selector.
 *
 * @author Charley Wu
 * @since 2019/4/30
 */
public class DefaultShardSelector<T> implements ShardSelector<T> {

  @Override
  public String getShard(T value) {
    return "0";
  }
}
