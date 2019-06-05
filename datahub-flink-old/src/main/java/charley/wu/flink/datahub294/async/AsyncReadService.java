package charley.wu.flink.datahub294.async;

import charley.wu.flink.datahub294.client.DataHubClientFactory;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.model.ShardEntry;
import java.util.List;
import java.util.Map;
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

  private final List<ShardEntry> shardEntries;
  private final int threadNum;

  private ExecutorService threadPool;
  private ReadCallback callback;
  private Map<String, ReadTask> tasks;

  public AsyncReadService(DataHubClientFactory factory, List<ShardEntry> shardEntries) {
    this.factory = factory;
    this.shardEntries = shardEntries;
    this.threadNum = shardEntries.size() + 1;
  }

  public void start() throws Exception {
    this.threadPool = Executors.newFixedThreadPool(this.threadNum);
    this.tasks = new ConcurrentHashMap<>();
    putTask(tasks);
    LOG.info("DataHub AsyncReadService start OK.");
  }

  private void putTask(Map<String, ReadTask> tasks) throws Exception {
    for (ShardEntry entry : shardEntries) {
      String shardId = entry.getShardId();
      ReadContext context = new ReadContext(shardId);
      ReadTask task = new ReadTask(factory, context);
      tasks.put(shardId, task);
      this.threadPool.submit(task);
    }
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

  public void registerCallback(ReadCallback callback) {
    this.callback = callback;
  }

  class ReadTask implements Runnable {

    private final DatahubClient client;

    private ReadContext context;
    private volatile boolean cancelled = false;

    private ReadTask(final DataHubClientFactory factory, ReadContext context)
        throws Exception {
      this.client = factory.create();
      this.context = context;
    }

    @Override
    public void run() {
      if (callback != null) {
        while (!cancelled) {
          try {
            callback.execute(client, context);
            Thread.sleep(context.getReadNextDelayTimeMillis());
          } catch (Throwable e) {
            context.setReadNextDelayTimeMillis(1000);
            LOG.error("doPullTask Exception", e);
          }
        }
      } else {
        LOG.error("Read Task Callback not set.");
        cancelled = true;
      }

      LOG.warn("The Read Task is cancelled, {}", context.getShardId());
    }

    private void cancel() {
      this.cancelled = true;
    }
  }
}
