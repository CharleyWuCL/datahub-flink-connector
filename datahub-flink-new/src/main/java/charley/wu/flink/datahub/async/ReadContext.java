package charley.wu.flink.datahub.async;

import com.aliyun.datahub.client.model.SubscriptionOffset;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public class ReadContext {

  private final String shardId;
  private final SubscriptionOffset offset;
  private long readNextDelayTimeMillis = 100;


  public ReadContext(final String shardId, final SubscriptionOffset offset) {
    this.shardId = shardId;
    this.offset = offset;
  }

  public String getShardId() {
    return shardId;
  }

  public SubscriptionOffset getOffset() {
    return offset;
  }

  public long getReadNextDelayTimeMillis() {
    return readNextDelayTimeMillis;
  }

  public void setReadNextDelayTimeMillis(long readNextDelayTimeMillis) {
    this.readNextDelayTimeMillis = readNextDelayTimeMillis;
  }
}
