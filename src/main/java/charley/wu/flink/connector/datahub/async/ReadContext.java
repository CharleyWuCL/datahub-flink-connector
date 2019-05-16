package charley.wu.flink.connector.datahub.async;

import com.aliyun.datahub.client.model.SubscriptionOffset;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public class ReadContext {

  private boolean isFirst = true;
  private final String shardId;
  private final SubscriptionOffset offset;
  private long readNextDelayTimeMillis = 100;


  public ReadContext(final String shardId, final SubscriptionOffset offset) {
    this.shardId = shardId;
    this.offset = offset;
  }

  public boolean isFirst() {
    return isFirst;
  }

  public void setFirst(boolean first) {
    isFirst = first;
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
