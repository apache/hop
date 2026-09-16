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

package org.apache.hop.core.logging;

import static org.apache.hop.core.logging.LoggingObjectType.PIPELINE;
import static org.apache.hop.core.logging.LoggingObjectType.WORKFLOW;

import java.util.Iterator;
import java.util.LinkedList;
import org.apache.hop.core.util.Utils;

/** Helper to derive a filename-qualified, human readable name from a logging object hierarchy. */
public final class LoggingObjectNameHelper {

  private LoggingObjectNameHelper() {
    // utility
  }

  /**
   * Walks the logging object parents and collects pipeline/workflow filenames to build a detailed
   * subject, e.g. {@code parent.hpl child.ktr}.
   *
   * @param loggingObject the object to inspect, may be {@code null}
   * @return the detailed subject, possibly empty when nothing usable was found
   */
  public static String getDetailedSubject(ILoggingObject loggingObject) {
    LinkedList<String> subjects = new LinkedList<>();
    while (loggingObject != null) {
      if (loggingObject.getObjectType() == PIPELINE || loggingObject.getObjectType() == WORKFLOW) {
        String filename = loggingObject.getFilename();
        if (!Utils.isEmpty(filename)) {
          subjects.add(filename);
        }
      }
      loggingObject = loggingObject.getParent();
    }
    if (!subjects.isEmpty()) {
      return subjects.size() > 1 ? formatDetailedSubject(subjects) : subjects.get(0);
    }
    return "";
  }

  private static String formatDetailedSubject(LinkedList<String> subjects) {
    StringBuilder string = new StringBuilder();
    for (Iterator<String> it = subjects.descendingIterator(); it.hasNext(); ) {
      string.append(it.next());
      if (it.hasNext()) {
        string.append('.');
      }
    }
    return string.toString();
  }
}
