package charley.wu.flink.connector.datahub.async;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DataHub Thread Factory.
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public class DataHubThreadFactory implements ThreadFactory {

  private final AtomicLong threadIndex = new AtomicLong(0);
  private final String threadNamePrefix;
  private final boolean daemon;

  public DataHubThreadFactory(String threadNamePrefix) {
    this(threadNamePrefix, false);
  }

  public DataHubThreadFactory(String threadNamePrefix, boolean daemon) {
    this.threadNamePrefix = threadNamePrefix;
    this.daemon = daemon;
  }

  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r, this.threadNamePrefix + this.threadIndex.incrementAndGet());
    thread.setDaemon(this.daemon);
    return thread;
  }

}
