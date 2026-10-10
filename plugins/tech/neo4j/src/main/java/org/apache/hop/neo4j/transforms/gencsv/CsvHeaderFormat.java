/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.transforms.gencsv;

/** The header line of the generated CSV files. */
public enum CsvHeaderFormat {
  /**
   * The headers of neo4j-admin import: id:ID, property:type, :LABEL, :START_ID, :END_ID and :TYPE.
   */
  NEO4J_ADMIN("neo4j-admin import"),

  /**
   * Just the names: the id field, the properties, label, start_id, end_id and type. For loading the
   * files into other graph databases, for example with LOAD CSV.
   */
  PLAIN("Plain column names");

  private final String description;

  CsvHeaderFormat(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public static String[] getDescriptions() {
    String[] descriptions = new String[values().length];
    for (int i = 0; i < descriptions.length; i++) {
      descriptions[i] = values()[i].description;
    }
    return descriptions;
  }

  /** The format with the given description or name, neo4j-admin import if there is none. */
  public static CsvHeaderFormat lookup(String descriptionOrName) {
    for (CsvHeaderFormat format : values()) {
      if (format.description.equalsIgnoreCase(descriptionOrName)
          || format.name().equalsIgnoreCase(descriptionOrName)) {
        return format;
      }
    }
    return NEO4J_ADMIN;
  }
}
