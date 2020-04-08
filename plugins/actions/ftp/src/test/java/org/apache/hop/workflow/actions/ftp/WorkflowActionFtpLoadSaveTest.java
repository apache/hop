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

package org.apache.hop.workflow.actions.ftp;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;

public class WorkflowActionFtpLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionFtp> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionFtp> getActionClass() {
    return ActionFtp.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "port",
      "serverName",
      "userName",
      "password",
      "ftpDirectory",
      "targetDirectory",
      "wildcard",
      "binaryMode",
      "timeout",
      "remove",
      "onlyGettingNewFiles",
      "activeConnection",
      "controlEncoding",
      "moveFiles",
      "moveToDirectory",
      "dateInFilename",
      "timeInFilename",
      "specifyFormat",
      "date_time_format",
      "addDateBeforeExtension",
      "addToResult",
      "createMoveFolder",
      "proxyHost",
      "proxyPort",
      "proxyUsername",
      "proxyPassword",
      "socksProxyHost",
      "socksProxyPort",
      "socksProxyUsername",
      "socksProxyPassword",
      "SifFileExists",
      "limit",
      "success_condition" } );
  }

}
