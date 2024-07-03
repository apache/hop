/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.execution;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.json.HopJson;

public class ExecutionDataSetMetaDeserializer
    extends JsonDeserializer<Map<String, ExecutionDataSetMeta>> {
  @Override
  public Map<String, ExecutionDataSetMeta> deserialize(
      JsonParser parser, DeserializationContext context) throws IOException {
    JsonDeserializer<Object> deserializer =
        context.findRootValueDeserializer(context.constructType(Map.class));

    Map<String, Map<String, Object>> rawMap =
        (Map<String, Map<String, Object>>) deserializer.deserialize(parser, context);
    Map<String, ExecutionDataSetMeta> map = Collections.synchronizedMap(new HashMap<>());
    ObjectMapper mapper = HopJson.newMapper();
    for (String key : rawMap.keySet()) {
      Map<String, Object> rawMetaMap = rawMap.get(key);
      ExecutionDataSetMeta setMeta =
          mapper.readValue(mapper.writeValueAsString(rawMetaMap), ExecutionDataSetMeta.class);
      map.put(key, setMeta);
    }
    return map;
  }
}
