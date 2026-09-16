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
package org.apache.hop.pipeline.transforms.plugincatalog;

/** Output granularity for the Plugin Catalog transform. */
public enum DetailLevel {
  /** One row per plugin; properties are emitted as a JSON array column. */
  PER_PLUGIN,
  /** One row per plugin property; plugin-level columns are repeated. */
  PER_PROPERTY;

  public static DetailLevel fromString(String value) {
    if (value == null) {
      return PER_PLUGIN;
    }
    for (DetailLevel level : values()) {
      if (level.name().equalsIgnoreCase(value.trim())) {
        return level;
      }
    }
    return PER_PLUGIN;
  }
}
