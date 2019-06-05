/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package charley.wu.flink.datahub294.serialization;

import charley.wu.flink.datahub294.serialization.basic.DataHubSerializer;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import java.math.BigDecimal;
import java.util.Map;

public class DefaultTupleSerializer implements DataHubSerializer<Map<String, Object>> {

  private RecordSchema schema;

  public DefaultTupleSerializer(RecordSchema schema) {
    this.schema = schema;
  }

  @Override
  public RecordEntry serializeAttrs(RecordEntry record, Map<String, Object> tuple) {
    // set attributes
    tuple.forEach((k, v) -> record.putAttribute(k, String.valueOf(v)));
    return record;
  }

  @Override
  public RecordEntry serializeData(RecordEntry record, Map<String, Object> tuple) {
    // set blob data
    schema.getFields().forEach(field -> {
      String fieldName = field.getName();
      Object value = tuple.get(fieldName);
      switch (field.getType()) {
        case BIGINT:
          record.setBigint(fieldName, (Long) value);
          break;
        case DOUBLE:
          record.setDouble(fieldName, (Double) value);
          break;
        case STRING:
          record.setString(fieldName, (String) value);
          break;
        case BOOLEAN:
          record.setBoolean(fieldName, (Boolean) value);
          break;
        case DECIMAL:
          record.setDecimal(fieldName, (BigDecimal) value);
          break;
        case TIMESTAMP:
          record.setTimeStampInMs(fieldName, (Long) value);
          break;
        default:
          break;
      }
    });
    return record;
  }
}
