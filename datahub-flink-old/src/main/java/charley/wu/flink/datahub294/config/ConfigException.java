package charley.wu.flink.datahub294.config;

/**
 * DataHub Config Exception.
 *
 * @author Charley Wu
 * @since 2019/4/15
 */
public class ConfigException extends Exception {

  public ConfigException(String message) {
    super(message);
  }

  public ConfigException(String message, Throwable cause) {
    super(message, cause);
  }

  private String buildMessage(String message) {
    return "[DataHub Config Error] " + message;
  }
}
