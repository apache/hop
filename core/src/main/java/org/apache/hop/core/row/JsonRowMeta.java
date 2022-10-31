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

package org.apache.hop.core.row;

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class JsonRowMeta {

  /**
   * Convert only the basic row metadata properties to JSON Only what we need in Beam
   *
   * @param rowMeta The row to convert to JSON
   * @return
   */
  public static String toJson(IRowMeta rowMeta) {
    try {
      JSONObject jRowMeta = new JSONObject();

      JSONArray jValues = new JSONArray();
      jRowMeta.put("values", jValues);

      for (int v = 0; v < rowMeta.size(); v++) {
        IValueMeta valueMeta = rowMeta.getValueMeta(v);
        JSONObject jValue = new JSONObject();
        valueMeta.storeMetaInJson(jValue);
        jValues.add(jValue);
      }

      return jRowMeta.toJSONString();
    } catch (Exception e) {
      throw new RuntimeException("Error converting row metadata to JSON", e);
    }
  }

  public static IRowMeta fromJson(String rowMetaJson) {
    try {
      JSONParser parser = new JSONParser();
      JSONObject jRowMeta = (JSONObject) parser.parse(rowMetaJson);

      IRowMeta rowMeta = new RowMeta();

      JSONArray jValues = (JSONArray) jRowMeta.get("values");
      for (Object value : jValues) {
        JSONObject jValue = (JSONObject) value;
        IValueMeta valueMeta = ValueMetaFactory.loadValueMetaFromJson(jValue);
        rowMeta.addValueMeta(valueMeta);
      }

      return rowMeta;
    } catch (Exception e) {
      throw new RuntimeException("Error converting row metadata JSON to row metadata", e);
    }
  }
}
