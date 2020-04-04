/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.ssh;

import com.trilead.ssh2.Connection;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

public class SSHMeta extends BaseTransformMeta implements TransformMetaInterface {
  static Class<?> PKG = SSHMeta.class; // for i18n purposes, needed by Translator!!
  private static int DEFAULT_PORT = 22;

  private String command;
  private boolean dynamicCommandField;
  /**
   * dynamic command fieldname
   */
  private String commandfieldname;

  private String serverName;
  private String port;
  private String userName;
  private String password;
  // key
  private boolean usePrivateKey;
  private String keyFileName;
  private String passPhrase;

  private String stdOutFieldName;
  private String stdErrFieldName;
  private String timeOut;
  // Proxy
  private String proxyHost;
  private String proxyPort;
  private String proxyUsername;
  private String proxyPassword;

  public SSHMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    SSHMeta retval = (SSHMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    dynamicCommandField = false;
    command = null;
    commandfieldname = null;
    port = String.valueOf( DEFAULT_PORT );
    serverName = null;
    userName = null;
    password = null;
    usePrivateKey = true;
    keyFileName = null;
    stdOutFieldName = "stdOut";
    stdErrFieldName = "stdErr";
    timeOut = "0";
    proxyHost = null;
    proxyPort = null;
    proxyUsername = null;
    proxyPassword = null;
  }

  /**
   * @return Returns the serverName.
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * @param serverName The serverName to set.
   */
  public void setServerName( String serverName ) {
    this.serverName = serverName;
  }

  /**
   * @return Returns the userName.
   */
  public String getuserName() {
    return userName;
  }

  /**
   * @param userName The userName to set.
   */
  public void setuserName( String userName ) {
    this.userName = userName;
  }

  /**
   * @param password The password to set.
   */
  public void setpassword( String password ) {
    this.password = password;
  }

  /**
   * @return Returns the password.
   */
  public String getpassword() {
    return password;
  }

  /**
   * @param commandfieldname The commandfieldname to set.
   */
  public void setcommandfieldname( String commandfieldname ) {
    this.commandfieldname = commandfieldname;
  }

  /**
   * @return Returns the commandfieldname.
   */
  public String getcommandfieldname() {
    return commandfieldname;
  }

  /**
   * @param command The commandfieldname to set.
   */
  public void setCommand( String command ) {
    this.command = command;
  }

  /**
   * @return Returns the command.
   */
  public String getCommand() {
    return command;
  }

  /**
   * @param value The dynamicCommandField to set.
   */
  public void setDynamicCommand( boolean value ) {
    this.dynamicCommandField = value;
  }

  /**
   * @return Returns the dynamicCommandField.
   */
  public boolean isDynamicCommand() {
    return dynamicCommandField;
  }

  /**
   * @return Returns the port.
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port The port to set.
   */
  public void setPort( String port ) {
    this.port = port;
  }

  public void usePrivateKey( boolean value ) {
    this.usePrivateKey = value;
  }

  /**
   * @return Returns the usePrivateKey.
   */
  public boolean isusePrivateKey() {
    return usePrivateKey;
  }

  /**
   * @param value The keyFileName to set.
   */
  public void setKeyFileName( String value ) {
    this.keyFileName = value;
  }

  /**
   * @return Returns the keyFileName.
   */
  public String getKeyFileName() {
    return keyFileName;
  }

  /**
   * @param value The passPhrase to set.
   */
  public void setPassphrase( String value ) {
    this.passPhrase = value;
  }

  /**
   * @return Returns the passPhrase.
   */
  public String getPassphrase() {
    return passPhrase;
  }

  /*
   * @param timeOut The timeOut to set.
   */
  public void setTimeOut( String timeOut ) {
    this.timeOut = timeOut;
  }

  /**
   * @return Returns the timeOut.
   */
  public String getTimeOut() {
    return timeOut;
  }

  /**
   * @param value The stdOutFieldName to set.
   */
  public void setstdOutFieldName( String value ) {
    this.stdOutFieldName = value;
  }

  /**
   * @return Returns the stdOutFieldName.
   */
  public String getStdOutFieldName() {
    return stdOutFieldName;
  }

  /**
   * @param value The stdErrFieldName to set.
   */
  public void setStdErrFieldName( String value ) {
    this.stdErrFieldName = value;
  }

  /**
   * @return Returns the stdErrFieldName.
   */
  public String getStdErrFieldName() {
    return stdErrFieldName;
  }

  /**
   * @param value The proxyHost to set.
   */
  public void setProxyHost( String value ) {
    this.proxyHost = value;
  }

  /**
   * @return Returns the proxyHost.
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * @param value The proxyPort to set.
   */
  public void setProxyPort( String value ) {
    this.proxyPort = value;
  }

  /**
   * @return Returns the proxyPort.
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param value The proxyUsername to set.
   */
  public void setProxyUsername( String value ) {
    this.proxyUsername = value;
  }

  /**
   * @return Returns the proxyUsername.
   */
  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * @param value The proxyPassword to set.
   */
  public void setProxyPassword( String value ) {
    this.proxyPassword = value;
  }

  /**
   * @return Returns the proxyPassword.
   */
  public String getProxyPassword() {
    return proxyPassword;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " ).append( XMLHandler.addTagValue( "dynamicCommandField", dynamicCommandField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "command", command ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "commandfieldname", commandfieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "port", port ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "servername", serverName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "userName", userName ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "usePrivateKey", usePrivateKey ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "keyFileName", keyFileName ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "passPhrase", Encr.encryptPasswordIfNotUsingVariables( passPhrase ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "stdOutFieldName", stdOutFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "stdErrFieldName", stdErrFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "timeOut", timeOut ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "proxyHost", proxyHost ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "proxyPort", proxyPort ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "proxyUsername", proxyUsername ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "proxyPassword", Encr.encryptPasswordIfNotUsingVariables( proxyPassword ) ) );
    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      dynamicCommandField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "dynamicCommandField" ) );
      command = XMLHandler.getTagValue( transformNode, "command" );
      commandfieldname = XMLHandler.getTagValue( transformNode, "commandfieldname" );
      port = XMLHandler.getTagValue( transformNode, "port" );
      serverName = XMLHandler.getTagValue( transformNode, "servername" );
      userName = XMLHandler.getTagValue( transformNode, "userName" );
      password = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "password" ) );

      usePrivateKey = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "usePrivateKey" ) );
      keyFileName = XMLHandler.getTagValue( transformNode, "keyFileName" );
      passPhrase =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "passPhrase" ) );
      stdOutFieldName = XMLHandler.getTagValue( transformNode, "stdOutFieldName" );
      stdErrFieldName = XMLHandler.getTagValue( transformNode, "stdErrFieldName" );
      timeOut = XMLHandler.getTagValue( transformNode, "timeOut" );
      proxyHost = XMLHandler.getTagValue( transformNode, "proxyHost" );
      proxyPort = XMLHandler.getTagValue( transformNode, "proxyPort" );
      proxyUsername = XMLHandler.getTagValue( transformNode, "proxyUsername" );
      proxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "proxyPassword" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "SSHMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;
    String error_message = "";

    // Target hostname
    if ( Utils.isEmpty( getServerName() ) ) {
      error_message = BaseMessages.getString( PKG, "SSHMeta.CheckResult.TargetHostMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "SSHMeta.CheckResult.TargetHostOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( isusePrivateKey() ) {
      String keyfilename = pipelineMeta.environmentSubstitute( getKeyFileName() );
      if ( Utils.isEmpty( keyfilename ) ) {
        error_message = BaseMessages.getString( PKG, "SSHMeta.CheckResult.PrivateKeyFileNameMissing" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } else {
        error_message = BaseMessages.getString( PKG, "SSHMeta.CheckResult.PrivateKeyFileNameOK" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
        remarks.add( cr );
        boolean keyFileExists = false;
        try {
          keyFileExists = HopVFS.fileExists( keyfilename );
        } catch ( Exception e ) { /* Ignore */
        }
        if ( !keyFileExists ) {
          error_message = BaseMessages.getString( PKG, "SSHMeta.CheckResult.PrivateKeyFileNotExist", keyfilename );
          cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
          remarks.add( cr );
        } else {
          error_message = BaseMessages.getString( PKG, "SSHMeta.CheckResult.PrivateKeyFileExists", keyfilename );
          cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
          remarks.add( cr );
        }
      }
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SSHMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SSHMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    if ( !isDynamicCommand() ) {
      row.clear();
    }
    IValueMeta v =
      new ValueMetaString( variables.environmentSubstitute( getStdOutFieldName() ) );
    v.setOrigin( name );
    row.addValueMeta( v );

    String stderrfield = variables.environmentSubstitute( getStdErrFieldName() );
    if ( !Utils.isEmpty( stderrfield ) ) {
      v = new ValueMetaBoolean( stderrfield );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SSH( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new SSHData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * @param serveur
   * @param port
   * @param username
   * @param password
   * @param useKey
   * @param keyFilename
   * @param passPhrase
   * @param timeOut
   * @param variables
   * @param proxyhost
   * @param proxyport
   * @param proxyusername
   * @param proxypassword
   * @return
   * @throws HopException
   * @deprecated Use {@link SSHData#OpenConnection(String, int, String, String, boolean, String, String, int, iVariables, String, int, String, String)} instead
   */
  @Deprecated
  public static Connection OpenConnection( String serveur, int port, String username, String password,
                                           boolean useKey, String keyFilename, String passPhrase, int timeOut, iVariables variables, String proxyhost,
                                           int proxyport, String proxyusername, String proxypassword ) throws HopException {
    return SSHData.OpenConnection( serveur, port, username, password, useKey, keyFilename, passPhrase, timeOut,
      variables, proxyhost, proxyport, proxyusername, proxypassword );
  }

  /**
   * Returns the Input/Output metadata for this transform.
   */
  @Override
  public TransformIOMetaInterface getTransformIOMeta() {
    return new TransformIOMeta( isDynamicCommand(), true, false, false, false, false );
  }
}
