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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hop.core.logging.LoggingObjectType.ACTION;
import static org.apache.hop.core.logging.LoggingObjectType.DATABASE;
import static org.apache.hop.core.logging.LoggingObjectType.PIPELINE;
import static org.apache.hop.core.logging.LoggingObjectType.TRANSFORM;
import static org.apache.hop.core.logging.LoggingObjectType.WORKFLOW;

import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Function;
import org.apache.hop.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jLoggingEventListener implements IHopLoggingEventListener {

  @VisibleForTesting
  Logger pipelineLogger = LoggerFactory.getLogger("org.apache.hop.pipeline.Pipeline");

  @VisibleForTesting
  Logger workflowLogger = LoggerFactory.getLogger("org.apache.hop.workflow.Workflow");

  @VisibleForTesting Logger hopLogger = LoggerFactory.getLogger("org.apache.hop");

  @VisibleForTesting
  Function<String, ILoggingObject> logObjProvider =
      objId -> LoggingRegistry.getInstance().getLoggingObject(objId);

  public Slf4jLoggingEventListener() {
    // Do nothing
  }

  @Override
  public void eventAdded(HopLoggingEvent event) {
    Object messageObject = event.getMessage();
    checkNotNull(messageObject, "Expected log message to be defined.");
    if (messageObject instanceof LogMessage message) {
      ILoggingObject loggingObject = logObjProvider.apply(message.getLogChannelId());

      if (loggingObject == null) {
        // this can happen if logObject has been discarded while log events are still in flight.
        logToLogger(
            hopLogger, message.getLevel(), message.getSubject() + " " + message.getMessage());
      } else if (loggingObject.getObjectType() == PIPELINE
          || loggingObject.getObjectType() == TRANSFORM
          || loggingObject.getObjectType() == DATABASE) {
        logToLogger(pipelineLogger, message.getLevel(), loggingObject, message);
      } else if (loggingObject.getObjectType() == WORKFLOW
          || loggingObject.getObjectType() == ACTION) {
        logToLogger(workflowLogger, message.getLevel(), loggingObject, message);
      } else {
        logToLogger(hopLogger, message.getLevel(), loggingObject, message);
      }
    }
  }

  private void logToLogger(
      Logger logger, LogLevel logLevel, ILoggingObject loggingObject, LogMessage message) {
    logToLogger(
        logger, logLevel, "[" + getDetailedSubject(loggingObject) + "]  " + message.getMessage());
  }

  private void logToLogger(Logger logger, LogLevel logLevel, String message) {
    switch (logLevel) {
      case NOTHING:
        break;
      case ERROR:
        logger.error(message);
        break;
      case MINIMAL:
        logger.warn(message);
        break;
      case BASIC, DETAILED:
        logger.info(message);
        break;
      case DEBUG:
        logger.debug(message);
        break;
      case ROWLEVEL:
        logger.trace(message);
        break;
      default:
        break;
    }
  }

  private String getDetailedSubject(ILoggingObject loggingObject) {
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
    } else {
      return "";
    }
  }

  private String formatDetailedSubject(LinkedList<String> subjects) {
    StringBuilder string = new StringBuilder();
    for (Iterator<String> it = subjects.descendingIterator(); it.hasNext(); ) {
      string.append(it.next());
      if (it.hasNext()) {
        string.append("  ");
      }
    }
    return string.toString();
  }
}
