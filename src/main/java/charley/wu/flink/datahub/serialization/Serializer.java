/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package charley.wu.flink.datahub.serialization;

import charley.wu.flink.datahub.annotation.DataHubField;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public interface Serializer<T> extends Serializable {

  String BIZ_KEY = "biz_key";

  String EVENT_TIME = "event_time";

  default RecordEntry serializeAttrs(RecordEntry record, T tuple) {
    return record;
  }

  default RecordEntry serializeData(RecordEntry record, T tuple) {
    return record;
  }

  default RecordEntry serializeData(RecordEntry record, RecordSchema schema, T tuple) {
    try {
      TupleRecordData data = new TupleRecordData(schema);
      Map<String, Object> kvMap = getKV(tuple);
      kvMap.forEach(data::setField);
      setEventTime(data, tuple);
      record.setRecordData(data);
      return record;
    } catch (Exception e) {
      throw new RuntimeException("Serialize object error.", e);
    }
  }

  default Map<String, Object> getKV(T tuple) throws Exception {
    Class<?> clazz = tuple.getClass();
    Map<String, Object> kvMap = new HashMap<>();

    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      if (field.isSynthetic()) {
        continue;
      }
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }
      String fieldName = field.getName();
      DataHubField ann = field.getAnnotation(DataHubField.class);
      if (Objects.isNull(ann)) {
        throw new Exception("DataHubField is null, " + fieldName);
      }
      kvMap.put(ann.alias(), field.get(tuple));
    }
    return kvMap;
  }

  default void setEventTime(TupleRecordData data, T tuple) {
  }
}
