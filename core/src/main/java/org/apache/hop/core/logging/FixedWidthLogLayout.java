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

package org.apache.hop.core.logging;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import org.apache.hop.core.Const;

public class FixedWidthLogLayout {
  private static final ThreadLocal<SimpleDateFormat> LOCAL_SIMPLE_DATE_PARSER =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"));

  private static final int MAX_LOG_LEVEL_LENGTH =
      Arrays.stream(LogLevel.getLogLevelCodes()).map(String::length).reduce(0, Integer::max);

  private boolean timeAdded;

  public FixedWidthLogLayout() {
    this(true);
  }

  public FixedWidthLogLayout(boolean addTime) {
    this.timeAdded = addTime;
  }

  public String format(HopLoggingEvent event) {
    StringBuilder line = new StringBuilder();

    String dateTimeString = "";

    if (timeAdded) {
      dateTimeString = LOCAL_SIMPLE_DATE_PARSER.get().format(new Date(event.timeStamp)) + " ";
    }

    Object object = event.getMessage();

    if (object instanceof LogMessage message) {

      String logLevel = getLogLevelPadded(message.getLevel());

      String[] parts =
          message.getMessage() == null ? new String[] {} : message.getMessage().split(Const.CR);

      for (int i = 0; i < parts.length; i++) {
        if (!message.isSimplified()) {
          line.append(dateTimeString);
          line.append(logLevel);

          // Include the subject too on every line...
          if (message.getSubject() != null) {
            line.append(message.getSubject());
            if (message.getCopy() != null) {
              line.append(".").append(message.getCopy());
            }
            line.append(" - ");
          }
        }

        line.append(parts[i]);
        if (i < parts.length - 1) {
          line.append(Const.CR); // put the CR's back in there!
        }
      }
    } else {
      line.append(dateTimeString);
      line.append((object != null ? object.toString() : "<null>"));
    }

    return line.toString();
  }

  private String getLogLevelPadded(LogLevel logLevel) {
    String code = logLevel.getCode();
    return "[" + code + "] " + " ".repeat(Math.max(MAX_LOG_LEVEL_LENGTH - code.length(), 0));
  }
}
