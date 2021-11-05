package charley.wu.flink.datahub.config;

/**
 * DataHub Config Exception.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class ConfigException extends RuntimeException {

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
