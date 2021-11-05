package charley.wu.flink.datahub.metrics;

import org.apache.flink.metrics.Gauge;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class DelayGauge implements Gauge<Long> {

  private long value;

  public void setValue(long value) {
    this.value = value;
  }

  @Override
  public Long getValue() {
    return value;
  }
}
