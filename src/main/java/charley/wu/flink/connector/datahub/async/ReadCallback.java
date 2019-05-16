package charley.wu.flink.connector.datahub.async;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public interface ReadCallback {

  void execute(ReadContext context);
}
