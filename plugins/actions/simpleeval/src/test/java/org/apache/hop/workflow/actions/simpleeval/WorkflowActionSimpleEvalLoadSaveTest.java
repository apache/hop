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
package org.apache.hop.workflow.actions.simpleeval;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionSimpleEvalLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionSimpleEval> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();


  @Override
  protected Class<ActionSimpleEval> getActionClass() {
    return ActionSimpleEval.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "fieldname",
      "variablename",
      "mask",
      "comparevalue",
      "minvalue",
      "maxvalue",
      "successwhenvarset"
    );
  }

  @Override
  protected List<String> listXmlAttributes() {
    return Arrays.asList( "name", "description" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "fieldname", "getFieldName",
      "variablename", "getVariableName",
      "comparevalue", "getCompareValue",
      "minvalue", "getMinValue",
      "maxvalue", "getMaxValue",
      "successwhenvarset", "isSuccessWhenVarSet"
    );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "fieldname", "setFieldName",
      "variablename", "setVariableName",
      "comparevalue", "setCompareValue",
      "minvalue", "setMinValue",
      "maxvalue", "setMaxValue",
      "successwhenvarset", "setSuccessWhenVarSet"
    );
  }
}
