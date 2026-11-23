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

package org.apache.hop.pipeline.transforms.jsonoutput;

import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Writes the JSON of the JSON output transform one row at a time, with exactly the text json-simple
 * gives for a {@code JSONObject} that holds a {@code JSONArray} of one {@code JSONObject} per row.
 *
 * <p>Building those objects for every row keeps the whole document in memory until it is written.
 * Here only the text is produced. To keep that text the same, two habits of json-simple are
 * reproduced: it keeps an object in a hash map, so the element names come out in that map's order
 * and not in the order of the fields, and a name that is used twice keeps the value that was put
 * last. Values and names are written by json-simple itself, so escaping does not change.
 */
final class JsonRowsText {

  private static final String END = "]}";

  /** The element names, quoted and escaped and followed by a colon, in the order to write them. */
  private final String[] keys;

  /** For each key: the position of its value in the row's values. */
  private final int[] valueIndexes;

  /**
   * @param elementNames the element name of each value of a row, in the order of the values
   */
  JsonRowsText(List<String> elementNames) {
    // What json-simple does with these names: its map decides the order, the last one wins.
    JSONObject order = new JSONObject();
    for (int i = 0; i < elementNames.size(); i++) {
      order.put(elementNames.get(i), i);
    }
    keys = new String[order.size()];
    valueIndexes = new int[order.size()];
    int k = 0;
    for (Object object : order.entrySet()) {
      Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
      Object name = entry.getKey();
      // json-simple writes String.valueOf(key), also for a null name
      keys[k] = '"' + JSONValue.escape(String.valueOf(name)) + "\":";
      valueIndexes[k] = (Integer) entry.getValue();
      k++;
    }
  }

  /** Starts a document: an object with one element, the array of rows. */
  static void appendStart(StringBuilder text, String blockName) {
    text.append("{\"").append(JSONValue.escape(String.valueOf(blockName))).append("\":[");
  }

  /** Ends the document that {@link #appendStart} started. */
  static void appendEnd(StringBuilder text) {
    text.append(END);
  }

  /**
   * Adds one row to the array.
   *
   * @param first true for the first row of the document
   * @param values the values of the row, one for each element name given to the constructor
   */
  void appendRow(StringBuilder text, boolean first, Object[] values) {
    if (!first) {
      text.append(',');
    }
    text.append('{');
    for (int k = 0; k < keys.length; k++) {
      if (k > 0) {
        text.append(',');
      }
      text.append(keys[k]).append(JSONValue.toJSONString(values[valueIndexes[k]]));
    }
    text.append('}');
  }
}
