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
package org.apache.hop.databases.cassandra.util;

/** @author Tatsiana_Kasiankova */
public enum CQLFunctions {
  TOKEN(false, "org.apache.cassandra.db.marshal.LongType"),
  COUNT(false, "org.apache.cassandra.db.marshal.LongType"),
  WRITETIME(false, "org.apache.cassandra.db.marshal.LongType"),
  TTL(false, "org.apache.cassandra.db.marshal.Int32Type"),
  DATEOF(true, "org.apache.cassandra.db.marshal.TimestampType"),
  UNIXTIMESTAMPOF(true, "org.apache.cassandra.db.marshal.LongType");

  private final boolean isCaseSensitive;
  private final String validator;

  private CQLFunctions(boolean isCaseSensetive, String validator) {
    this.isCaseSensitive = isCaseSensetive;
    this.validator = validator;
  }

  /**
   * Indicates if the name of the function should be processed as case sensitive or not.
   *
   * @return isCaseSensitive true if the name of the function should be processed as case sensitive
   *     or not.
   */
  public boolean isCaseSensitive() {
    return isCaseSensitive;
  }

  /**
   * Returns the Cassandra validator class for the function.
   *
   * @return the Cassandra validator class for the function.
   */
  public String getValidator() {
    return validator;
  }

  /**
   * Returns CQLFunction by the string representation of it.
   *
   * @param input the string representation of CQLFunction
   * @return the CQLFunction if the input string contains this one, otherwise null.
   */
  public static CQLFunctions getFromString(String input) {
    if (input != null) {
      input = input.trim().toUpperCase();
      for (CQLFunctions fs : CQLFunctions.values()) {
        if (isFunction(fs, input)) {
          return fs;
        }
      }
    }
    return null;
  }

  private static boolean isFunction(CQLFunctions fs, String input) {
    return input.equals(fs.name());
  }
}
