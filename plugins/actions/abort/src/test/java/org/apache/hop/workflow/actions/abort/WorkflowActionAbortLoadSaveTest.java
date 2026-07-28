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
package org.apache.hop.workflow.actions.abort;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;

class WorkflowActionAbortLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionAbort> {

  @Override
  protected Class<ActionAbort> getActionClass() {
    return ActionAbort.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList("messageAbort", "messageLogLevel");
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
        "messageAbort", "getMessageAbort",
        "messageLogLevel", "getMessageLogLevel");
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
        "messageAbort", "setMessageAbort",
        "messageLogLevel", "setMessageLogLevel");
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    EnumSet<LogLevel> logLevels = EnumSet.allOf(LogLevel.class);
    LogLevel random = (LogLevel) logLevels.toArray()[new Random().nextInt(logLevels.size())];
    return toMap("messageLogLevel", new EnumLoadSaveValidator<>(random));
  }
}
