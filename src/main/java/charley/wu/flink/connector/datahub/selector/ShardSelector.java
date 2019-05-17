package charley.wu.flink.connector.datahub.selector;

import java.io.Serializable;
import java.util.List;

/**
 * Shard selector.
 *
 * @author Charley Wu
 * @since 2019/4/30
 */
public interface ShardSelector<T> extends Serializable {

  void setShardList(List<String> shards);

  String getShard(T value);

}
