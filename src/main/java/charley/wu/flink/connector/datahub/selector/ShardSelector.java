package charley.wu.flink.connector.datahub.selector;

import java.io.Serializable;

/**
 * Shard selector.
 *
 * @author Charley Wu
 * @since 2019/4/30
 */
public interface ShardSelector<T> extends Serializable {

  String getShard(T value);

}
