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

package org.apache.hop.workflow.actions.telnet;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.SocketUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines a Telnet action.
 *
 * @author Samatar
 * @since 05-11-2003
 */

@Action(
  id = "TELNET",
  name = "i18n::ActionTelnet.Name",
  description = "i18n::ActionTelnet.Description",
  image = "Telnet.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/telnet.html"
)
public class ActionTelnet extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionTelnet.class; // For Translator

  private String hostname;
  private String port;
  private String timeout;

  public static final int DEFAULT_TIME_OUT = 3000;
  public static final int DEFAULT_PORT = 23;

  public ActionTelnet( String n ) {
    super( n, "" );
    hostname = null;
    port = String.valueOf( DEFAULT_PORT );
    timeout = String.valueOf( DEFAULT_TIME_OUT );
  }

  public ActionTelnet() {
    this( "" );
  }

  public Object clone() {
    ActionTelnet je = (ActionTelnet) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "hostname", hostname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "port", port ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "timeout", timeout ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      hostname = XmlHandler.getTagValue( entrynode, "hostname" );
      port = XmlHandler.getTagValue( entrynode, "port" );
      timeout = XmlHandler.getTagValue( entrynode, "timeout" );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'Telnet' from XML node", xe );
    }
  }

  public String getPort() {
    return port;
  }

  public String getRealPort() {
    return resolve( getPort() );
  }

  public void setPort( String port ) {
    this.port = port;
  }

  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  public String getHostname() {
    return hostname;
  }

  public String getRealHostname() {
    return resolve( getHostname() );
  }

  public String getTimeOut() {
    return timeout;
  }

  public String getRealTimeOut() {
    return resolve( getTimeOut() );
  }

  public void setTimeOut( String timeout ) {
    this.timeout = timeout;
  }

  public Result execute( Result previousResult, int nr ) {

    Result result = previousResult;

    result.setNrErrors( 1 );
    result.setResult( false );

    String hostname = getRealHostname();
    int port = Const.toInt( getRealPort(), DEFAULT_PORT );
    int timeoutInt = Const.toInt( getRealTimeOut(), -1 );

    if ( Utils.isEmpty( hostname ) ) {
      // No Host was specified
      logError( BaseMessages.getString( PKG, "JobTelnet.SpecifyHost.Label" ) );
      return result;
    }

    try {

      SocketUtil.connectToHost( hostname, port, timeoutInt );

      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobTelnet.OK.Label", hostname, port ) );
      }

      result.setNrErrors( 0 );
      result.setResult( true );

    } catch ( Exception ex ) {
      logError( BaseMessages.getString( PKG, "JobTelnet.NOK.Label", hostname, String.valueOf( port ) ) );
      logError( BaseMessages.getString( PKG, "JobTelnet.Error.Label" ) + ex.getMessage() );
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( !Utils.isEmpty( hostname ) ) {
      String realServername = resolve( hostname );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realServername, ResourceType.SERVER ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "hostname", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }
}
