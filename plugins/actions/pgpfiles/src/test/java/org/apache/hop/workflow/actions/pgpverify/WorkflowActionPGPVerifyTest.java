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
package org.apache.hop.workflow.actions.pgpverify;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.ClassRule;

public class WorkflowActionPGPVerifyTest extends WorkflowActionLoadSaveTestSupport<ActionPGPVerify> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionPGPVerify> getActionClass() {
    return ActionPGPVerify.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "gpglocation",
      "filename",
      "detachedfilename",
      "useDetachedSignature" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "gpglocation", "getGPGLocation",
      "filename", "getFilename",
      "detachedfilename", "getDetachedfilename",
      "useDetachedSignature", "useDetachedfilename" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "gpglocation", "setGPGLocation",
      "filename", "setFilename",
      "detachedfilename", "setDetachedfilename",
      "useDetachedSignature", "setUseDetachedfilename" );
  }

}
