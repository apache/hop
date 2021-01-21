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

package org.apache.hop.pipeline.transforms.salesforce;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

public abstract class SalesforceTransformMeta<Main extends SalesforceTransform, Data extends SalesforceTransformData>
  extends BaseTransformMeta
  implements ITransformMeta<Main, Data> {

  private static Class<?> PKG = SalesforceTransformMeta.class; // For Translator

  /** The Salesforce Target URL */
  @Injection( name = "SALESFORCE_URL" )
  private String targetUrl;

  /** The userName */
  @Injection( name = "SALESFORCE_USERNAME" )
  private String username;

  /** The password */
  @Injection( name = "SALESFORCE_PASSWORD" )
  private String password;

  /** The time out */
  @Injection( name = "TIME_OUT" )
  private String timeout;

  /** The connection compression */
  @Injection( name = "USE_COMPRESSION" )
  private boolean compression;

  /** The Salesforce module */
  @Injection( name = "MODULE" )
  private String module;

  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " ).append( XmlHandler.addTagValue( "targeturl", getTargetUrl() ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "username", getUsername() ) );
    retval.append( "    " ).append(
      XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( getPassword() ) ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "timeout", getTimeout() ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "useCompression", isCompression() ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "module", getModule() ) );
    return retval.toString();
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    setTargetUrl( XmlHandler.getTagValue( transformNode, "targeturl" ) );
    setUsername( XmlHandler.getTagValue( transformNode, "username" ) );
    setPassword( Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( transformNode, "password" ) ) );
    setTimeout( XmlHandler.getTagValue( transformNode, "timeout" ) );
    setCompression( "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "useCompression" ) ) );
    setModule( XmlHandler.getTagValue( transformNode, "module" ) );
  }

  public Object clone() {
    SalesforceTransformMeta retval = (SalesforceTransformMeta) super.clone();
    return retval;
  }

  public void setDefault() {
    setTargetUrl( SalesforceConnectionUtils.TARGET_DEFAULT_URL );
    setUsername( "" );
    setPassword( "" );
    setTimeout( "60000" );
    setCompression( false );
    setModule( "Account" );
  }

  @Override public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                               IHopMetadataProvider metadataProvider ) {
    CheckResult cr;

    // check URL
    if ( Utils.isEmpty( getTargetUrl() ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SalesforceTransformMeta.CheckResult.NoURL" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SalesforceTransformMeta.CheckResult.URLOk" ), transformMeta );
    }
    remarks.add( cr );

    // check user name
    if ( Utils.isEmpty( getUsername() ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SalesforceTransformMeta.CheckResult.NoUsername" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SalesforceTransformMeta.CheckResult.UsernameOk" ), transformMeta );
    }
    remarks.add( cr );

    // check module
    if ( Utils.isEmpty( getModule() ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SalesforceTransformMeta.CheckResult.NoModule" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SalesforceTransformMeta.CheckResult.ModuleOk" ), transformMeta );
    }
    remarks.add( cr );
  }

  /**
   * @return Returns the Target URL.
   */
  public String getTargetUrl() {
    return targetUrl;
  }

  /**
   * @param targetUrl
   *          The Target URL to set.
   */
  public void setTargetUrl( String targetUrl ) {
    this.targetUrl = targetUrl;
  }

  /**
   * @return Returns the UserName.
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username
   *          The Username to set.
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  @Deprecated
  public String getUserName() {
    return getUsername();
  }

  @Deprecated
  public void setUserName( String username ) {
    setUsername( username );
  }

  /**
   * @return Returns the Password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password
   *          The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Returns the connection timeout.
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * @param timeOut
   *          The connection timeout to set.
   */
  public void setTimeout( String timeout ) {
    this.timeout = timeout;
  }

  @Deprecated
  public String getTimeOut() {
    return getTimeout();
  }

  @Deprecated
  public void setTimeOut( String timeOut ) {
    setTimeout( timeOut );
  }

  public boolean isCompression() {
    return compression;
  }

  public void setCompression( boolean compression ) {
    this.compression = compression;
  }

  /**
   * @return Returns the useCompression.
   */
  @Deprecated
  public boolean isUsingCompression() {
    return isCompression();
  }

  /**
   * @param useCompression
   *          The useCompression to set.
   */
  @Deprecated
  public void setUseCompression( boolean useCompression ) {
    setCompression( useCompression );
  }

  public String getModule() {
    return module;
  }

  public void setModule( String module ) {
    this.module = module;
  }
}
