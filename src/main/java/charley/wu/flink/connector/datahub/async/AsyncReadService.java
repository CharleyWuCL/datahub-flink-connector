package charley.wu.flink.connector.datahub.async;

import charley.wu.flink.connector.datahub.exception.DataHubException;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public class AsyncReadService {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncReadService.class);

  private int threadNum = 10;

  private final Map<String, SubscriptionOffset> offsets;
  private ExecutorService threadPool;
  private ReadCallback callback;

  public AsyncReadService(Map<String, SubscriptionOffset> offsets) {
    this.offsets = offsets;
  }

  public void start() throws DataHubException {
    this.threadPool = Executors.newFixedThreadPool(this.threadNum);
    putTask();
    LOG.info("DataHub AsyncReadService start OK.");
  }

  public void shutdown() {
    if (this.threadPool != null) {
      this.threadPool.shutdown();
    }
  }

  private void putTask() {
    Iterator<Entry<String, SubscriptionOffset>> it = offsets.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, SubscriptionOffset> next = it.next();
      String shardId = next.getKey();
      SubscriptionOffset offset = next.getValue();
      ReadContext context = new ReadContext(shardId, offset);
      ReadTask task = new ReadTask(context);
      this.threadPool.execute(task);
    }
  }

  public int getThreadNum() {
    return threadNum;
  }

  public void setThreadNum(int threadNum) {
    this.threadNum = threadNum;
  }

  public void registerCallback(ReadCallback callback) {
    this.callback = callback;
  }

  class ReadTask implements Runnable {

    private volatile boolean cancelled = false;
    private final ReadContext context;

    public ReadTask(ReadContext context) {
      this.context = context;
    }

    @Override
    public void run() {
      while (!this.isCancelled()) {
        if (callback != null) {
          try {
            callback.execute(context);
          } catch (Throwable e) {
            context.setReadNextDelayTimeMillis(1000);
            LOG.error("doPullTask Exception", e);
          }
        } else {
          LOG.error("Read Task Callback not set.");
          cancelled = true;
        }
      }
      LOG.warn("The Read Task is cancelled, {}", context.getShardId());
    }

    public boolean isCancelled() {
      return cancelled;
    }

    public void setCancelled(boolean cancelled) {
      this.cancelled = cancelled;
    }
  }
}
