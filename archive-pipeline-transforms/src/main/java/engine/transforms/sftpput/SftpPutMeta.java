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
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.actions.sftpput.JobEntrySFTPPUT;
import org.apache.hop.metadata.api.IHopMetadataProvider;
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

public class SftpPutMeta extends BaseTransformMeta implements ITransform {
  private static final Class<?> PKG = SFTPPutMeta.class; // for i18n purposes, needed by Translator!!

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
  private int afterFtps;
  private String remoteFilenameFieldName;

  public SftpPutMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      serverName = XmlHandler.getTagValue( transformNode, "servername" );
      serverPort = XmlHandler.getTagValue( transformNode, "serverport" );
      userName = XmlHandler.getTagValue( transformNode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( transformNode, "password" ) );
      sourceFileFieldName = XmlHandler.getTagValue( transformNode, "sourceFileFieldName" );
      remoteDirectoryFieldName = XmlHandler.getTagValue( transformNode, "remoteDirectoryFieldName" );

      inputIsStream = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "inputIsStream" ) );
      addFilenameResut = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "addFilenameResut" ) );

      usekeyfilename = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "usekeyfilename" ) );
      keyfilename = XmlHandler.getTagValue( transformNode, "keyfilename" );
      keyfilepass = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( transformNode, "keyfilepass" ) );
      compression = XmlHandler.getTagValue( transformNode, "compression" );
      proxyType = XmlHandler.getTagValue( transformNode, "proxyType" );
      proxyHost = XmlHandler.getTagValue( transformNode, "proxyHost" );
      proxyPort = XmlHandler.getTagValue( transformNode, "proxyPort" );
      proxyUsername = XmlHandler.getTagValue( transformNode, "proxyUsername" );
      proxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( transformNode, "proxyPassword" ) );

      createRemoteFolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "createRemoteFolder" ) );

      boolean remove = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "remove" ) );
      setAfterFtps( JobEntrySFTPPUT.getAfterSFTPPutByCode( Const.NVL( XmlHandler.getTagValue(
        transformNode, "aftersftpput" ), "" ) ) );
      if ( remove && getAfterFtps() == JobEntrySFTPPUT.AFTER_FTPSPUT_NOTHING ) {
        setAfterFtps( JobEntrySFTPPUT.AFTER_FTPSPUT_DELETE );
      }
      destinationfolderFieldName = XmlHandler.getTagValue( transformNode, "destinationfolderFieldName" );
      createDestinationFolder =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "createdestinationfolder" ) );
      remoteFilenameFieldName = XmlHandler.getTagValue( transformNode, "remoteFilenameFieldName" );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from XML", e );
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
    afterFtps = JobEntrySFTPPUT.AFTER_FTPSPUT_NOTHING;
    destinationfolderFieldName = null;
    createDestinationFolder = false;
    remoteFilenameFieldName = null;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "      " ).append( XmlHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "serverport", serverPort ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "username", userName ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sourceFileFieldName", sourceFileFieldName ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "remoteDirectoryFieldName", remoteDirectoryFieldName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "inputIsStream", inputIsStream ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addFilenameResut", addFilenameResut ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "usekeyfilename", usekeyfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "keyfilename", keyfilename ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "keyfilepass", Encr.encryptPasswordIfNotUsingVariables( keyfilepass ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "compression", compression ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxyType", proxyType ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxyHost", proxyHost ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxyPort", proxyPort ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxyUsername", proxyUsername ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "proxyPassword", Encr.encryptPasswordIfNotUsingVariables( proxyPassword ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createRemoteFolder", createRemoteFolder ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "aftersftpput", JobEntrySFTPPUT.getAfterSFTPPutCode( getAfterFtps() ) ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "destinationfolderFieldName", destinationfolderFieldName ) );
    retval
      .append( "      " ).append( XmlHandler.addTagValue( "createdestinationfolder", createDestinationFolder ) );
    retval
      .append( "      " ).append( XmlHandler.addTagValue( "remoteFilenameFieldName", remoteFilenameFieldName ) );

    return retval.toString();
  }

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // Default: nothing changes to rowMeta
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SFTPPutMeta.CheckResult.TransformRecevingData2" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
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
  public int getAfterFtps() {
    return afterFtps;
  }

  /**
   * @param value The afterFTPS to set.
   */
  public void setAfterFtps(int value ) {
    this.afterFtps = value;
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
