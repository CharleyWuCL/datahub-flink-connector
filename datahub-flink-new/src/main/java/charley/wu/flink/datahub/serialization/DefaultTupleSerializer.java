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

package charley.wu.flink.datahub.serialization;

import charley.wu.flink.datahub.serialization.basic.DataHubSerializer;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import java.util.Map;

public class DefaultTupleSerializer implements DataHubSerializer<Map<String, Object>> {

  private RecordSchema schema;

  public DefaultTupleSerializer(RecordSchema schema) {
    this.schema = schema;
  }

  @Override
  public RecordEntry serializeAttrs(RecordEntry record, Map<String, Object> tuple) {
    // set attributes
    tuple.forEach((k, v) -> record.addAttribute(k, String.valueOf(v)));
    return record;
  }

  @Override
  public RecordEntry serializeData(RecordEntry record, Map<String, Object> tuple) {
    // set blob data
    TupleRecordData data = new TupleRecordData(schema);
    tuple.forEach(data::setField);
    record.setRecordData(data);
    return record;
  }
}
