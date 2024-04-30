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

package org.apache.hop.workflow.actions.writetolog;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.ClassRule;

public class WorkflowActionWriteToLogLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionWriteToLog> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionWriteToLog> getActionClass() {
    return ActionWriteToLog.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList("logmessage", "loglevel", "logsubject");
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
        "logmessage", "getLogMessage",
        "loglevel", "getActionLogLevel",
        "logsubject", "getLogSubject");
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
        "logmessage", "setLogMessage",
        "loglevel", "setActionLogLevel",
        "logsubject", "setLogSubject");
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    EnumSet<LogLevel> logLevels = EnumSet.allOf(LogLevel.class);
    LogLevel random = (LogLevel) logLevels.toArray()[new Random().nextInt(logLevels.size())];
    return toMap("loglevel", new EnumLoadSaveValidator<>(random));
  }
}
