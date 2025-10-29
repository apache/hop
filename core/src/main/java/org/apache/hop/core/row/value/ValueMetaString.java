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
import java.util.Comparator;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.JsonUtil;

@ValueMetaPlugin(id = "2", name = "String", description = "String", image = "images/string.svg")
public class ValueMetaString extends ValueMetaBase {

  public ValueMetaString() {
    this(null);
  }

  public ValueMetaString(String name) {
    super(name, IValueMeta.TYPE_STRING);
  }

  public ValueMetaString(String name, Comparator<Object> comparator) {
    super(name, IValueMeta.TYPE_STRING, comparator);
  }

  public ValueMetaString(String name, int length, int precision) {
    super(name, IValueMeta.TYPE_STRING, length, precision);
  }

  @Override
  // For backward compatibility Hop often reads JSON as String, this optimizes String/byte[] into
  // JsonNode
  public JsonNode getJson(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }

    try {
      if (type == TYPE_STRING) {
        switch (storageType) {
          case STORAGE_TYPE_INDEXED:
            // Resolve the value, then fall through to NORMAL handling
            object = index[(Integer) object];
          case STORAGE_TYPE_NORMAL, STORAGE_TYPE_BINARY_STRING:
            // JsonUtil handles both NORMAL and BINARY_STRING cases
            return JsonUtil.parseTextValue(object);
          default:
            break;
        }
      }
    } catch (Exception e) {
      throw new HopValueException("Error converting value to JSON (" + object + ")", e);
    }
    // delegate
    return super.getJson(object);
  }

  @Override
  public Object getNativeDataType(Object object) throws HopValueException {
    return getString(object);
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return String.class;
  }
}
