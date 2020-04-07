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

package org.apache.hop.pipeline.transforms.sftpput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.actions.sftpput.JobEntrySFTPPUT;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Send file to SFTP host.
 *
 * @author Samatar Hassan
 * @since 30-April-2012
 */

public class SFTPPutMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = SFTPPutMeta.class; // for i18n purposes, needed by Translator!!

  private String serverName;
  private String serverPort;
  private String userName;
  private String password;
  private String sourceFileFieldName;
  private String remoteDirectoryFieldName;
  private boolean addFilenameResut;
  private boolean inputIsStream;
  private boolean usekeyfilename;
  private String keyfilename;
  private String keyfilepass;
  private String compression;
  private boolean createRemoteFolder;
  // proxy
  private String proxyType;
  private String proxyHost;
  private String proxyPort;
  private String proxyUsername;
  private String proxyPassword;

  private String destinationfolderFieldName;
  private boolean createDestinationFolder;
  private int afterFTPS;
  private String remoteFilenameFieldName;

  public SFTPPutMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      serverName = XMLHandler.getTagValue( transformNode, "servername" );
      serverPort = XMLHandler.getTagValue( transformNode, "serverport" );
      userName = XMLHandler.getTagValue( transformNode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "password" ) );
      sourceFileFieldName = XMLHandler.getTagValue( transformNode, "sourceFileFieldName" );
      remoteDirectoryFieldName = XMLHandler.getTagValue( transformNode, "remoteDirectoryFieldName" );

      inputIsStream = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "inputIsStream" ) );
      addFilenameResut = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addFilenameResut" ) );

      usekeyfilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "usekeyfilename" ) );
      keyfilename = XMLHandler.getTagValue( transformNode, "keyfilename" );
      keyfilepass = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "keyfilepass" ) );
      compression = XMLHandler.getTagValue( transformNode, "compression" );
      proxyType = XMLHandler.getTagValue( transformNode, "proxyType" );
      proxyHost = XMLHandler.getTagValue( transformNode, "proxyHost" );
      proxyPort = XMLHandler.getTagValue( transformNode, "proxyPort" );
      proxyUsername = XMLHandler.getTagValue( transformNode, "proxyUsername" );
      proxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "proxyPassword" ) );

      createRemoteFolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "createRemoteFolder" ) );

      boolean remove = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "remove" ) );
      setAfterFTPS( JobEntrySFTPPUT.getAfterSFTPPutByCode( Const.NVL( XMLHandler.getTagValue(
        transformNode, "aftersftpput" ), "" ) ) );
      if ( remove && getAfterFTPS() == JobEntrySFTPPUT.AFTER_FTPSPUT_NOTHING ) {
        setAfterFTPS( JobEntrySFTPPUT.AFTER_FTPSPUT_DELETE );
      }
      destinationfolderFieldName = XMLHandler.getTagValue( transformNode, "destinationfolderFieldName" );
      createDestinationFolder =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "createdestinationfolder" ) );
      remoteFilenameFieldName = XMLHandler.getTagValue( transformNode, "remoteFilenameFieldName" );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void setDefault() {
    serverName = null;
    serverPort = "22";
    inputIsStream = false;
    addFilenameResut = false;
    usekeyfilename = false;
    keyfilename = null;
    keyfilepass = null;
    compression = "none";
    proxyType = null;
    proxyHost = null;
    proxyPort = null;
    proxyUsername = null;
    proxyPassword = null;
    createRemoteFolder = false;
    afterFTPS = JobEntrySFTPPUT.AFTER_FTPSPUT_NOTHING;
    destinationfolderFieldName = null;
    createDestinationFolder = false;
    remoteFilenameFieldName = null;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "serverport", serverPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "username", userName ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sourceFileFieldName", sourceFileFieldName ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "remoteDirectoryFieldName", remoteDirectoryFieldName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "inputIsStream", inputIsStream ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "addFilenameResut", addFilenameResut ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "usekeyfilename", usekeyfilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "keyfilename", keyfilename ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "keyfilepass", Encr.encryptPasswordIfNotUsingVariables( keyfilepass ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "compression", compression ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyType", proxyType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyHost", proxyHost ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyPort", proxyPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyUsername", proxyUsername ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "proxyPassword", Encr.encryptPasswordIfNotUsingVariables( proxyPassword ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createRemoteFolder", createRemoteFolder ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "aftersftpput", JobEntrySFTPPUT.getAfterSFTPPutCode( getAfterFTPS() ) ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "destinationfolderFieldName", destinationfolderFieldName ) );
    retval
      .append( "      " ).append( XMLHandler.addTagValue( "createdestinationfolder", createDestinationFolder ) );
    retval
      .append( "      " ).append( XMLHandler.addTagValue( "remoteFilenameFieldName", remoteFilenameFieldName ) );

    return retval.toString();
  }

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Default: nothing changes to rowMeta
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.TransformRecevingData2" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new SFTPPut( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new SFTPPutData();
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
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
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName The userName to set.
   */
  public void setUserName( String userName ) {
    this.userName = userName;
  }

  /**
   * @return Returns the afterFTPS.
   */
  public int getAfterFTPS() {
    return afterFTPS;
  }

  /**
   * @param value The afterFTPS to set.
   */
  public void setAfterFTPS( int value ) {
    this.afterFTPS = value;
  }

  public String getServerPort() {
    return serverPort;
  }

  public void setServerPort( String serverPort ) {
    this.serverPort = serverPort;
  }

  public boolean isUseKeyFile() {
    return usekeyfilename;
  }

  public void setUseKeyFile( boolean value ) {
    this.usekeyfilename = value;
  }

  public String getKeyFilename() {
    return keyfilename;
  }

  public void setKeyFilename( String value ) {
    this.keyfilename = value;
  }

  public String getKeyPassPhrase() {
    return keyfilepass;
  }

  public void setKeyPassPhrase( String value ) {
    this.keyfilepass = value;
  }

  /**
   * @return Returns the compression.
   */
  public String getCompression() {
    return compression;
  }

  /**
   * @param compression The compression to set.
   */
  public void setCompression( String compression ) {
    this.compression = compression;
  }

  public String getProxyType() {
    return proxyType;
  }

  public void setProxyType( String value ) {
    this.proxyType = value;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost( String value ) {
    this.proxyHost = value;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort( String value ) {
    this.proxyPort = value;
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public void setProxyUsername( String value ) {
    this.proxyUsername = value;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  public void setProxyPassword( String value ) {
    this.proxyPassword = value;
  }

  public boolean isCreateRemoteFolder() {
    return this.createRemoteFolder;
  }

  public void setCreateRemoteFolder( boolean value ) {
    this.createRemoteFolder = value;
  }

  public boolean isAddFilenameResut() {
    return addFilenameResut;
  }

  public void setAddFilenameResut( boolean addFilenameResut ) {
    this.addFilenameResut = addFilenameResut;
  }

  public boolean isInputStream() {
    return inputIsStream;
  }

  public void setInputStream( boolean value ) {
    this.inputIsStream = value;
  }

  /**
   * @param createDestinationFolder The create destination folder flag to set.
   */
  public void setCreateDestinationFolder( boolean createDestinationFolder ) {
    this.createDestinationFolder = createDestinationFolder;
  }

  /**
   * @return Returns the create destination folder flag
   */
  public boolean isCreateDestinationFolder() {
    return createDestinationFolder;
  }

  public String getRemoteDirectoryFieldName() {
    return remoteDirectoryFieldName;
  }

  public void setRemoteDirectoryFieldName( String value ) {
    this.remoteDirectoryFieldName = value;
  }

  public void setSourceFileFieldName( String value ) {
    this.sourceFileFieldName = value;
  }

  public String getSourceFileFieldName() {
    return sourceFileFieldName;
  }

  public void setDestinationFolderFieldName( String value ) {
    this.destinationfolderFieldName = value;
  }

  public String getDestinationFolderFieldName() {
    return destinationfolderFieldName;
  }

  public String getRemoteFilenameFieldName() {
    return remoteFilenameFieldName;
  }

  public void setRemoteFilenameFieldName( String value ) {
    remoteFilenameFieldName = value;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
