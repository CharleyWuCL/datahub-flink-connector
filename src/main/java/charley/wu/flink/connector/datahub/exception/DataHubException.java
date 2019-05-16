package charley.wu.flink.connector.datahub.exception;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/4/15
 */
public class DataHubException extends Exception {

  public DataHubException(String message) {
    super(message);
  }

  public DataHubException(String message, Throwable cause) {
    super(message, cause);
  }

}
