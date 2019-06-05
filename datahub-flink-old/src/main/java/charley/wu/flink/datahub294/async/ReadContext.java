package charley.wu.flink.datahub294.async;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.model.OffsetContext;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public class ReadContext {

  private DatahubClient client;

  private boolean isFirst = true;
  private final String shardId;
  private OffsetContext offsetCtx;
  private long readNextDelayTimeMillis = 100;


  public ReadContext(final String shardId) {
    this.shardId = shardId;
  }

  public DatahubClient getClient() {
    return client;
  }

  public void setClient(DatahubClient client) {
    this.client = client;
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

  public OffsetContext getOffsetCtx() {
    return offsetCtx;
  }

  public void setOffsetCtx(OffsetContext offsetCtx) {
    this.offsetCtx = offsetCtx;
  }

  public long getReadNextDelayTimeMillis() {
    return readNextDelayTimeMillis;
  }

  public void setReadNextDelayTimeMillis(long readNextDelayTimeMillis) {
    this.readNextDelayTimeMillis = readNextDelayTimeMillis;
  }
}
