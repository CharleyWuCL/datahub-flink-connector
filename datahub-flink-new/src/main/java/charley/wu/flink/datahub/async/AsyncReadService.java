package charley.wu.flink.datahub.async;

import charley.wu.flink.datahub.client.DataHubClientFactory;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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

  private final DataHubClientFactory factory;
  private final int threadNum;

  private final Map<String, SubscriptionOffset> offsets;
  private ExecutorService threadPool;
  private ReadCallback callback;
  private Map<String, ReadTask> tasks;

  public AsyncReadService(DataHubClientFactory factory, Map<String, SubscriptionOffset> offsets) {
    this.factory = factory;
    this.offsets = offsets;
    this.threadNum = offsets.size() + 1;
  }

  public void start() throws Exception {
    this.threadPool = Executors.newFixedThreadPool(this.threadNum);
    this.tasks = new ConcurrentHashMap<>();
    putTask(tasks);
    LOG.info("DataHub AsyncReadService start OK.");
  }

  public void shutdown() {
    if (tasks != null) {
      tasks.forEach((shardId, task) -> {
        task.cancel();
      });
    }

    if (this.threadPool != null) {
      this.threadPool.shutdown();
    }
  }

  private void putTask(Map<String, ReadTask> tasks) throws Exception {
    Iterator<Entry<String, SubscriptionOffset>> it = offsets.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, SubscriptionOffset> next = it.next();
      String shardId = next.getKey();
      SubscriptionOffset offset = next.getValue();
      ReadContext context = new ReadContext(shardId, offset);
      ReadTask task = new ReadTask(factory, context);
      tasks.put(shardId, task);
      this.threadPool.execute(task);
    }
  }

  public void registerCallback(ReadCallback callback) {
    this.callback = callback;
  }

  class ReadTask implements Runnable {

    private final DatahubClient client;

    private volatile boolean cancelled = false;
    private final ReadContext context;

    private ReadTask(final DataHubClientFactory factory, ReadContext context) throws Exception {
      this.client = factory.create();
      this.context = context;
    }

    @Override
    public void run() {
      while (!cancelled) {
        if (callback != null) {
          try {
            callback.execute(client, context, cancelled);
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

    private void cancel() {
      this.cancelled = true;
    }
  }
}
