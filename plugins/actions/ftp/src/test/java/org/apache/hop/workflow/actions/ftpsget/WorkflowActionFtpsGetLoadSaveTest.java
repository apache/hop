/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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

package org.apache.hop.workflow.actions.ftpsget;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkflowActionFtpsGetLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionFtpsGet> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionFtpsGet> getActionClass() {
    return ActionFtpsGet.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "port", "serverName", "userName", "password", "FTPSDirectory",
      "targetDirectory", "wildcard", "binaryMode", "timeout", "remove", "onlyGettingNewFiles",
      "activeConnection", "moveFiles", "moveToDirectory", "dateInFilename", "timeInFilename",
      "specifyFormat", "dateTimeFormat", "addDateBeforeExtension", "addToResult", "createMoveFolder",
      "proxy_host", "proxy_port", "proxy_username", "proxy_password", "ifFileExists", "limit",
      "success_condition", "connection_type" } );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<String, IFieldLoadSaveValidator<?>>();
    validators.put( "connection_type", new IntLoadSaveValidator( FtpsConnection.connectionTypeCode.length ) );
    validators.put( "ifFileExists", new IntLoadSaveValidator( ActionFtpsGet.FILE_EXISTS_ACTIONS.length ) );

    return validators;
  }

}
