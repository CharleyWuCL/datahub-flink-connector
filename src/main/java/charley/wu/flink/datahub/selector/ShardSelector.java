package charley.wu.flink.datahub.selector;

import java.io.Serializable;
import java.util.List;

/**
 * Shard selector.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public interface ShardSelector<T> extends Serializable {

  void setShardList(List<String> shards);

  String getShard(T value);

}
