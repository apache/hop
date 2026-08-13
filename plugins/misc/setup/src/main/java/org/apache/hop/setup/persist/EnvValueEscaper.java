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
 */

package org.apache.hop.setup.persist;

import org.apache.hop.setup.HopSetupException;

/** Quote / validate values written into shell, cmd, and PowerShell snippets. */
public final class EnvValueEscaper {

  private EnvValueEscaper() {}

  public static String shellSingleQuoted(String name, String value) throws HopSetupException {
    rejectControlOrQuote(name, value, '\'');
    return "'" + value + "'";
  }

  public static String cmdQuoted(String name, String value) throws HopSetupException {
    rejectNewlines(name, value);
    if (value.indexOf('"') >= 0
        || value.indexOf('%') >= 0
        || value.indexOf('!') >= 0
        || value.indexOf('&') >= 0
        || value.indexOf('|') >= 0
        || value.indexOf('<') >= 0
        || value.indexOf('>') >= 0
        || value.indexOf('^') >= 0) {
      throw new HopSetupException(
          "Value for "
              + name
              + " contains characters that cannot be written safely to a Windows cmd script");
    }
    return "\"" + value + "\"";
  }

  public static String powershellSingleQuoted(String name, String value) throws HopSetupException {
    rejectNewlines(name, value);
    return "'" + value.replace("'", "''") + "'";
  }

  public static void rejectNewlines(String name, String value) throws HopSetupException {
    if (value != null && (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0)) {
      throw new HopSetupException("Value for " + name + " must not contain a newline");
    }
  }

  private static void rejectControlOrQuote(String name, String value, char quote)
      throws HopSetupException {
    rejectNewlines(name, value);
    if (value != null && value.indexOf(quote) >= 0) {
      throw new HopSetupException(
          "Value for " + name + " must not contain the character '" + quote + "'");
    }
  }
}
