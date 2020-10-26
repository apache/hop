/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.workflow.actions.sql;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionSqlTest extends WorkflowActionLoadSaveTestSupport<ActionSql> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionSql> getActionClass() {
    return ActionSql.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "sql",
      "useVariableSubstitution",
      "sqlfromfile",
      "sqlfilename",
      "sendOneStatement",
      "database" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "sql", "getSql",
      "useVariableSubstitution", "getUseVariableSubstitution",
      "sqlfromfile", "getSqlFromFile",
      "sqlfilename", "getSqlFilename",
      "sendOneStatement", "isSendOneStatement",
      "database", "getDatabase" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "sql", "setSql",
      "useVariableSubstitution", "setUseVariableSubstitution",
      "sqlfromfile", "setSqlFromFile",
      "sqlfilename", "setSqlFilename",
      "sendOneStatement", "setSendOneStatement",
      "database", "setDatabase" );
  }

}
