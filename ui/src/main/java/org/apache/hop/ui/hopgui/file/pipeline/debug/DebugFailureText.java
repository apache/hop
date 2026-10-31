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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import org.apache.hop.core.Const;

/** The error lines from a pipeline log. A failed init explains itself there. */
public final class DebugFailureText {

  private DebugFailureText() {}

  /**
   * Keeps log lines that record an error. When the log has none, returns {@code fallback}.
   *
   * @param logText pipeline log, including transform channels
   * @param fallback text used when the log has no error line
   */
  public static String fromLog(String logText, String fallback) {
    StringBuilder errors = new StringBuilder();
    if (logText != null) {
      for (String line : logText.split("\\R")) {
        if (line.contains("ERROR")) {
          if (!errors.isEmpty()) {
            errors.append(Const.CR);
          }
          errors.append(line.trim());
        }
      }
    }
    if (errors.isEmpty()) {
      return fallback == null ? "" : fallback;
    }
    return errors.toString();
  }
}
