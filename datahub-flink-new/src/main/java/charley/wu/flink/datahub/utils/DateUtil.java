package charley.wu.flink.datahub.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang3.Validate;

/**
 * Data util.
 *
 * @author Charley Wu
 * @since 2019/5/28
 */
public class DateUtil {

  private static DateTimeFormatter TO_MILLI = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  private static DateTimeFormatter TO_MONTH = DateTimeFormatter.ofPattern("yyyy-MM");

  /**
   * Convert long time to data.
   *
   * @param time Long time.
   * @return Date string.
   */
  public static String convertTimeToString(Long time) {
    Validate.notNull(time, "time is null");
    return TO_MILLI
        .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
  }

  /**
   * Convert localDate to month
   *
   * @param date LocalDate
   * @return Month String.
   */
  public static String convertTimeToMonth(LocalDate date) {
    Validate.notNull(date, "date is null");
    return date.format(TO_MONTH);
  }

}
