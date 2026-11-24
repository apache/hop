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
package org.apache.hop.airflow.engine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds the start and the end of the actions of a workflow in its log, as Hop writes them at the
 * Basic log level in English. The canvas shows them while the workflow runs on Airflow; lines it
 * does not recognize are left alone.
 */
public class AirflowActionLog {

  /** What a log line says about an action. */
  public record Event(String actionName, boolean finished, boolean success) {}

  private final Pattern starting;
  private final Pattern finished;

  /**
   * @param workflowName the workflow whose actions to follow: lines of the workflows it runs name
   *     those, and are not about its own actions
   */
  public AirflowActionLog(String workflowName) {
    String prefix = " - " + Pattern.quote(workflowName) + " - ";
    starting = Pattern.compile(prefix + "Starting action \\[(.*)\\]$");
    finished =
        Pattern.compile(prefix + "Finished action \\[(.*)\\] \\(result=\\[(true|false)\\]\\)$");
  }

  /**
   * @return the event of the line, or null when the line says nothing about an action
   */
  public Event parse(String line) {
    Matcher matcher = finished.matcher(line);
    if (matcher.find()) {
      return new Event(matcher.group(1), true, Boolean.parseBoolean(matcher.group(2)));
    }
    matcher = starting.matcher(line);
    if (matcher.find()) {
      return new Event(matcher.group(1), false, false);
    }
    return null;
  }
}
