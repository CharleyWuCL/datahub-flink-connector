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

import charley.wu.flink.datahub.util.JsonUtil;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public interface Deserializer<T> extends ResultTypeQueryable<T>, Serializable {

  default Map<String, String> deserializeKeys(RecordEntry record){
    return null;
  }

  default T deserializeValue(RecordEntry record){
    try {
      TupleRecordData data = (TupleRecordData) record.getRecordData();
      RecordSchema schema = data.getRecordSchema();
      List<Field> fields = schema.getFields();

      Map<String, Object> kvMap = new HashMap<>();
      fields.forEach(field -> {
        String key = field.getName();
        Object value = data.getField(key);
        kvMap.put(key, value);
      });
      return JsonUtil.toObject(kvMap, getProducedType().getTypeClass());
    } catch (Exception e) {
      throw new RuntimeException("Deserialize object error.", e);
    }
  }

}
