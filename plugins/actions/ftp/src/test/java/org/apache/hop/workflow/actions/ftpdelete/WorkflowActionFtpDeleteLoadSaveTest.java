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

package org.apache.hop.workflow.actions.ftpdelete;

import org.apache.hop.workflow.actions.ftpsget.FtpsConnection;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkflowActionFtpDeleteLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionFtpDelete> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionFtpDelete> getActionClass() {
    return ActionFtpDelete.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "protocol", "serverName", "port", "userName", "password",
      "ftpDirectory", "wildcard", "timeout", "activeConnection", "useProxy", "proxyHost", "proxyPort",
      "proxyUsername", "proxyPassword", "usePublicKey", "keyFilename", "keyFilePass", "limitSuccess",
      "successCondition", "copyPrevious", "FtpsConnectionType", "socksProxyHost", "socksProxyPort",
      "socksProxyUsername", "socksProxyPassword" } );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<String, IFieldLoadSaveValidator<?>>();
    validators.put( "FtpsConnectionType", new IntLoadSaveValidator( FtpsConnection.connectionTypeCode.length ) );

    return validators;
  }

}
