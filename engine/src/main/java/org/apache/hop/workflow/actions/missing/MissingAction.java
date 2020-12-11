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

package org.apache.hop.workflow.actions.missing;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

public class MissingAction extends ActionBase implements IAction {

  /** Action unique identifier" */
  public static final String ID = "MISSING";
    
  private final String missingPluginId;

  public MissingAction() {
      this(null, null);
  }
  
  public MissingAction( String name, String missingPluginId ) {
    super( name, "" );
    setPluginId( ID );
    this.missingPluginId = missingPluginId;
  }

  @Override
  public Result execute( Result previousResult, int nr ) throws HopWorkflowException {
    previousResult.setResult( false );
    previousResult.setNrErrors( previousResult.getNrErrors() + 1 );
    getLogChannel().logError( BaseMessages.getString( MissingAction.class, "MissingAction.Log.CannotRunWorkflow" ) );
    return previousResult;
  }

  public String getMissingPluginId() {
    return this.missingPluginId;
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
  }
}
