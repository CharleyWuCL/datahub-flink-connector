package charley.wu.flink.datahub.core;

import charley.wu.flink.datahub.serialization.Deserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
@Slf4j
public class DataHubSourceDeserializer implements Deserializer<Input> {

  @Override
  public TypeInformation<Input> getProducedType() {
    return TypeInformation.of(Input.class);
  }

  @Override
  public Class<Input> getType() {
    return Input.class;
  }
}

