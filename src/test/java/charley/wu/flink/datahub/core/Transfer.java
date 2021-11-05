package charley.wu.flink.datahub.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Filter not effective data.
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
@Slf4j
public class Transfer extends RichMapFunction<Input, Output> {

  @Override
  public Output map(Input input) throws Exception {
    return new Output();
  }
}
