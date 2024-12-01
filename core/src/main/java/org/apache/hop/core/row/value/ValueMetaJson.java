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
 */

package org.apache.hop.core.row.value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.json.HopJson;

@ValueMetaPlugin(id = "11", name = "JSON", description = "JSON object", image = "images/json.svg")
public class ValueMetaJson extends ValueMetaBase {

  /** Do String serialization using pretty printing? */
  private boolean prettyPrinting;

  @Override
  public int compare(Object data1, Object data2) throws HopValueException {
    JsonNode json1 = (JsonNode) data1;
    JsonNode json2 = (JsonNode) data2;

    String string1 = convertJsonToString(json1);
    String string2 = convertJsonToString(json2);

    return string1.compareTo(string2);
  }

  public ValueMetaJson() {
    this(null);
  }

  public ValueMetaJson(String name) {
    super(name, TYPE_JSON);
    prettyPrinting = true;
  }

  @Override
  public String getString(Object object) throws HopValueException {
    return convertJsonToString(getJson(object));
  }

  @Override
  public String convertJsonToString(JsonNode jsonNode) throws HopValueException {
    try {
      ObjectMapper objectMapper = HopJson.newMapper();
      if (prettyPrinting) {
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
      } else {
        return objectMapper.writeValueAsString(jsonNode);
      }
    } catch (Exception e) {
      throw new HopValueException("Error converting JSON value to String", e);
    }
  }

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    JsonNode jsonNode = getJson(object);
    if (jsonNode == null) {
      return null;
    }

    try {
      String jsonString = convertJsonToString(jsonNode);
      return convertStringToJson(jsonString);
    } catch (Exception e) {
      throw new HopValueException("Unable to clone JSON value", e);
    }
  }

  @Override
  public Object getNativeDataType(Object object) throws HopValueException {
    return getJson(object);
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return JsonNode.class;
  }

  /**
   * Gets prettyPrinting
   *
   * @return value of prettyPrinting
   */
  public boolean isPrettyPrinting() {
    return prettyPrinting;
  }

  /**
   * @param prettyPrinting The prettyPrinting to set
   */
  public void setPrettyPrinting(boolean prettyPrinting) {
    this.prettyPrinting = prettyPrinting;
  }
}
