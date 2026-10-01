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

package org.apache.hop.redis.transforms.redisoutput;

/**
 * How stream fields are mapped when writing to Redis.
 *
 * <ul>
 *   <li>{@link #KEY_VALUE} — pick key / value (or hash key / hash value) fields explicitly
 *   <li>{@link #STREAM_FIELDS} — per-row mapping: stream field, data structure, key, codecs, and
 *       optional hash key (Get fields fills defaults)
 * </ul>
 */
public enum RedisOutputWriteMode {
  KEY_VALUE,
  STREAM_FIELDS;

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = values()[i].name();
    }
    return names;
  }

  public static RedisOutputWriteMode fromCode(String code) {
    if (code == null || code.isBlank()) {
      return KEY_VALUE;
    }
    try {
      return RedisOutputWriteMode.valueOf(code.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return KEY_VALUE;
    }
  }
}
