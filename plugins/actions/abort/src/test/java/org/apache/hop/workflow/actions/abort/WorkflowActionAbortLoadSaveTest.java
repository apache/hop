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
package org.apache.hop.workflow.actions.abort;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WorkflowActionAbortLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionAbort> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionAbort> getActionClass() {
    return ActionAbort.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Collections.singletonList( "message" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return Collections.singletonMap( "message", "getMessageAbort" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return Collections.singletonMap( "message", "setMessageAbort" );
  }
}
