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

package org.apache.hop.pipeline.transforms.ldapoutput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.ldapinput.LdapMeta;
import org.apache.hop.pipeline.transforms.ldapinput.LdapProtocolFactory;
import org.w3c.dom.Node;

import java.util.List;

public class LDAPOutputMeta extends BaseTransformMeta implements LdapMeta {
  private static Class<?> PKG = LDAPOutputMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * Flag indicating that we use authentication for connection
   */
  private boolean useAuthentication;

  /**
   * The Host name
   */
  private String Host;

  /**
   * The User name
   */
  private String userName;

  /**
   * The Password to use in LDAP authentication
   */
  private String password;

  /**
   * The Port
   */
  private String port;

  /**
   * The name of DN field
   */
  private String dnFieldName;

  private boolean failIfNotExist;

  /**
   * Field value to update
   */
  private String[] updateLookup;

  /**
   * Stream name to update value with
   */
  private String[] updateStream;

  /**
   * boolean indicating if field needs to be updated
   */
  private Boolean[] update;

  /**
   * Operations type
   */
  private String searchBase;

  /**
   * Multi valued separator
   **/
  private String multiValuedSeparator;

  private int operationType;

  private String oldDnFieldName;
  private String newDnFieldName;
  private boolean deleteRDN;

  /**
   * The operations description
   */
  public static final String[] operationTypeDesc = {
    BaseMessages.getString( PKG, "LDAPOutputMeta.operationType.Insert" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.operationType.Upsert" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.operationType.Update" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.operationType.Add" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.operationType.Delete" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.operationType.Rename" ) };

  /**
   * The operations type codes
   */
  public static final String[] operationTypeCode = { "insert", "upsert", "update", "add", "delete", "rename" };

  public static final int OPERATION_TYPE_INSERT = 0;

  public static final int OPERATION_TYPE_UPSERT = 1;

  public static final int OPERATION_TYPE_UPDATE = 2;

  public static final int OPERATION_TYPE_ADD = 3;

  public static final int OPERATION_TYPE_DELETE = 4;

  public static final int OPERATION_TYPE_RENAME = 5;

  private int referralType;

  /**
   * The referrals description
   */
  public static final String[] referralTypeDesc = {
    BaseMessages.getString( PKG, "LDAPOutputMeta.referralType.Follow" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.referralType.Ignore" ) };

  /**
   * The referrals type codes
   */
  public static final String[] referralTypeCode = { "follow", "ignore" };

  public static final int REFERRAL_TYPE_FOLLOW = 0;

  public static final int REFERRAL_TYPE_IGNORE = 1;

  private int derefAliasesType;

  /**
   * The derefAliasess description
   */
  public static final String[] derefAliasesTypeDesc = {
    BaseMessages.getString( PKG, "LDAPOutputMeta.derefAliasesType.Always" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.derefAliasesType.Never" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.derefAliasesType.Searching" ),
    BaseMessages.getString( PKG, "LDAPOutputMeta.derefAliasesType.Finding" ) };

  /**
   * The derefAliasess type codes
   */
  public static final String[] derefAliasesTypeCode = { "always", "never", "searching", "finding" };

  public static final int DEREFALIASES_TYPE_ALWAYS = 0;

  public static final int DEREFALIASES_TYPE_NEVER = 1;

  public static final int DEREFALIASES_TYPE_SEARCHING = 2;

  public static final int DEREFALIASES_TYPE_FINDING = 3;

  /**
   * Protocol
   **/
  private String protocol;

  /**
   * Trust store
   **/
  private boolean useCertificate;
  private String trustStorePath;
  private String trustStorePassword;
  private boolean trustAllCertificates;

  public LDAPOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the input useCertificate.
   */
  public boolean isUseCertificate() {
    return useCertificate;
  }

  /**
   * @return Returns the useCertificate.
   */
  public void setUseCertificate( boolean value ) {
    this.useCertificate = value;
  }

  /**
   * @return Returns the input trustAllCertificates.
   */
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  /**
   * @return Returns the input trustAllCertificates.
   */
  public void setTrustAllCertificates( boolean value ) {
    this.trustAllCertificates = value;
  }

  /**
   * @return Returns the trustStorePath.
   */
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * @param value the trustStorePassword to set.
   */
  public void setTrustStorePassword( String value ) {
    this.trustStorePassword = value;
  }

  /**
   * @return Returns the trustStorePath.
   */
  public String getTrustStorePath() {
    return trustStorePath;
  }

  /**
   * @param value the trustStorePath to set.
   */
  public void setTrustStorePath( String value ) {
    this.trustStorePath = value;
  }

  /**
   * @return Returns the protocol.
   */
  public String getProtocol() {
    return protocol;
  }

  /**
   * @param value the protocol to set.
   */
  public void setProtocol( String value ) {
    this.protocol = value;
  }

  public Boolean[] getUpdate() {
    return update;
  }

  public void setUpdate( Boolean[] update ) {
    this.update = update;
  }

  public int getOperationType() {
    return operationType;
  }

  public int getReferralType() {
    return referralType;
  }

  public int getDerefAliasesType() {
    return derefAliasesType;
  }

  public static int getOperationTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < operationTypeDesc.length; i++ ) {
      if ( operationTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getOperationTypeByCode( tt );
  }

  public static int getReferralTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < referralTypeDesc.length; i++ ) {
      if ( referralTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getReferralTypeByCode( tt );
  }

  public static int getDerefAliasesTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < derefAliasesTypeDesc.length; i++ ) {
      if ( derefAliasesTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getReferralTypeByCode( tt );
  }

  private static int getOperationTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < operationTypeCode.length; i++ ) {
      if ( operationTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getReferralTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < referralTypeCode.length; i++ ) {
      if ( referralTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getDerefAliasesTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < derefAliasesTypeCode.length; i++ ) {
      if ( derefAliasesTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public void setOperationType( int operationType ) {
    this.operationType = operationType;
  }

  public void setReferralType( int value ) {
    this.referralType = value;
  }

  public void setDerefAliasesType( int value ) {
    this.derefAliasesType = value;
  }

  public static String getOperationTypeDesc( int i ) {
    if ( i < 0 || i >= operationTypeDesc.length ) {
      return operationTypeDesc[ 0 ];
    }
    return operationTypeDesc[ i ];
  }

  public static String getReferralTypeDesc( int i ) {
    if ( i < 0 || i >= referralTypeDesc.length ) {
      return referralTypeDesc[ 0 ];
    }
    return referralTypeDesc[ i ];
  }

  public static String getDerefAliasesTypeDesc( int i ) {
    if ( i < 0 || i >= derefAliasesTypeDesc.length ) {
      return derefAliasesTypeDesc[ 0 ];
    }
    return derefAliasesTypeDesc[ i ];
  }

  /**
   * @return Returns the updateStream.
   */
  public String[] getUpdateStream() {
    return updateStream;
  }

  /**
   * @param updateStream The updateStream to set.
   */
  public void setUpdateStream( String[] updateStream ) {
    this.updateStream = updateStream;
  }

  /**
   * @return Returns the updateLookup.
   */
  public String[] getUpdateLookup() {
    return updateLookup;
  }

  /**
   * @param updateLookup The updateLookup to set.
   */
  public void setUpdateLookup( String[] updateLookup ) {
    this.updateLookup = updateLookup;
  }

  /**
   * @return Returns the input useAuthentication.
   * Deprecated as it doesn't follow standards
   */
  @Deprecated
  public boolean UseAuthentication() {
    return useAuthentication;
  }

  /**
   * @return Returns the input useAuthentication.
   */
  public boolean getUseAuthentication() {
    return useAuthentication;
  }

  /**
   * @param useAuthentication The useAuthentication to set.
   */
  public void setUseAuthentication( boolean useAuthentication ) {
    this.useAuthentication = useAuthentication;
  }

  /**
   * @return Returns the host name.
   */
  public String getHost() {
    return Host;
  }

  /**
   * @param host The host to set.
   */
  public void setHost( String host ) {
    this.Host = host;
  }

  /**
   * @return Returns the user name.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName The username to set.
   */
  public void setUserName( String userName ) {
    this.userName = userName;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  public void setDnField( String value ) {
    this.dnFieldName = value;
  }

  public String getDnField() {
    return this.dnFieldName;
  }

  /**
   * @return Returns the Port.
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

  /**
   * @return Returns the failIfNotExist.
   */
  public boolean isFailIfNotExist() {
    return failIfNotExist;
  }

  /**
   * @param failIfNotExist The failIfNotExist to set.
   */
  public void setFailIfNotExist( boolean failIfNotExist ) {
    this.failIfNotExist = failIfNotExist;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    LDAPOutputMeta retval = (LDAPOutputMeta) super.clone();
    int nrvalues = updateLookup.length;

    retval.allocate( nrvalues );
    System.arraycopy( updateLookup, 0, retval.updateLookup, 0, nrvalues );
    System.arraycopy( updateStream, 0, retval.updateStream, 0, nrvalues );
    System.arraycopy( update, 0, retval.update, 0, nrvalues );

    return retval;
  }

  /**
   * @param value The deleteRDN filed.
   */
  public void setDeleteRDN( boolean value ) {
    this.deleteRDN = value;
  }

  /**
   * @return Returns the deleteRDN.
   */
  public boolean isDeleteRDN() {
    return deleteRDN;
  }

  /**
   * @param value The newDnFieldName filed.
   */
  public void setNewDnFieldName( String value ) {
    this.newDnFieldName = value;
  }

  /**
   * @return Returns the newDnFieldName.
   */
  public String getNewDnFieldName() {
    return newDnFieldName;
  }

  /**
   * @param value The oldDnFieldName filed.
   */
  public void setOldDnFieldName( String value ) {
    this.oldDnFieldName = value;
  }

  /**
   * @return Returns the oldDnFieldName.
   */
  public String getOldDnFieldName() {
    return oldDnFieldName;
  }

  /**
   * @param searchBase The searchBase filed.
   */
  public void setSearchBaseDN( String searchBase ) {
    this.searchBase = searchBase;
  }

  /**
   * @return Returns the searchBase.
   */
  public String getSearchBaseDN() {
    return searchBase;
  }

  /**
   * @param multiValuedSeparator The multi-valued separator filed.
   */
  public void setMultiValuedSeparator( String multiValuedSeparator ) {
    this.multiValuedSeparator = multiValuedSeparator;
  }

  /**
   * @return Returns the multi valued separator.
   */
  public String getMultiValuedSeparator() {
    return multiValuedSeparator;
  }

  public void allocate( int nrvalues ) {
    updateLookup = new String[ nrvalues ];
    updateStream = new String[ nrvalues ];
    update = new Boolean[ nrvalues ];
  }

  private static String getOperationTypeCode( int i ) {
    if ( i < 0 || i >= operationTypeCode.length ) {
      return operationTypeCode[ 0 ];
    }
    return operationTypeCode[ i ];
  }

  public static String getReferralTypeCode( int i ) {
    if ( i < 0 || i >= referralTypeCode.length ) {
      return referralTypeCode[ 0 ];
    }
    return referralTypeCode[ i ];
  }

  public static String getDerefAliasesCode( int i ) {
    if ( i < 0 || i >= derefAliasesTypeCode.length ) {
      return derefAliasesTypeCode[ 0 ];
    }
    return derefAliasesTypeCode[ i ];
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "useauthentication", useAuthentication ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "host", Host ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "username", userName ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "port", port ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dnFieldName", dnFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "failIfNotExist", failIfNotExist ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "operationType", getOperationTypeCode( operationType ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "multivaluedseparator", multiValuedSeparator ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "searchBase", searchBase ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "referralType", getReferralTypeCode( referralType ) ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "derefAliasesType", getDerefAliasesCode( derefAliasesType ) ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "oldDnFieldName", oldDnFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "newDnFieldName", newDnFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "deleteRDN", deleteRDN ) );

    retval.append( "    <fields>" + Const.CR );

    for ( int i = 0; i < updateLookup.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", updateLookup[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "field", updateStream[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "update", update[ i ].booleanValue() ) );
      retval.append( "      </field>" ).append( Const.CR );
    }

    retval.append( "      </fields>" + Const.CR );
    retval.append( "    " ).append( XMLHandler.addTagValue( "protocol", protocol ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "trustStorePath", trustStorePath ) );
    retval.append( "    " ).append(
      XMLHandler
        .addTagValue( "trustStorePassword", Encr.encryptPasswordIfNotUsingVariables( trustStorePassword ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "trustAllCertificates", trustAllCertificates ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "useCertificate", useCertificate ) );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {

      useAuthentication = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "useauthentication" ) );
      Host = XMLHandler.getTagValue( transformNode, "host" );
      userName = XMLHandler.getTagValue( transformNode, "username" );
      setPassword( Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "password" ) ) );

      port = XMLHandler.getTagValue( transformNode, "port" );
      dnFieldName = XMLHandler.getTagValue( transformNode, "dnFieldName" );
      failIfNotExist = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "failIfNotExist" ) );
      operationType =
        getOperationTypeByCode( Const.NVL( XMLHandler.getTagValue( transformNode, "operationType" ), "" ) );
      multiValuedSeparator = XMLHandler.getTagValue( transformNode, "multivaluedseparator" );
      searchBase = XMLHandler.getTagValue( transformNode, "searchBase" );
      referralType = getReferralTypeByCode( Const.NVL( XMLHandler.getTagValue( transformNode, "referralType" ), "" ) );
      derefAliasesType =
        getDerefAliasesTypeByCode( Const.NVL( XMLHandler.getTagValue( transformNode, "derefAliasesType" ), "" ) );

      oldDnFieldName = XMLHandler.getTagValue( transformNode, "oldDnFieldName" );
      newDnFieldName = XMLHandler.getTagValue( transformNode, "newDnFieldName" );
      deleteRDN = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "deleteRDN" ) );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        updateLookup[ i ] = XMLHandler.getTagValue( fnode, "name" );
        updateStream[ i ] = XMLHandler.getTagValue( fnode, "field" );
        if ( updateStream[ i ] == null ) {
          updateStream[ i ] = updateLookup[ i ]; // default: the same name!
        }
        String updateValue = XMLHandler.getTagValue( fnode, "update" );
        if ( updateValue == null ) {
          // default TRUE
          update[ i ] = Boolean.TRUE;
        } else {
          if ( updateValue.equalsIgnoreCase( "Y" ) ) {
            update[ i ] = Boolean.TRUE;
          } else {
            update[ i ] = Boolean.FALSE;
          }
        }
      }

      protocol = XMLHandler.getTagValue( transformNode, "protocol" );
      trustStorePath = XMLHandler.getTagValue( transformNode, "trustStorePath" );
      trustStorePassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "trustStorePassword" ) );
      trustAllCertificates = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "trustAllCertificates" ) );
      useCertificate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "useCertificate" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "LDAPOutputMeta.UnableToLoadFromXML" ), e );
    }
  }

  public void setDefault() {
    useAuthentication = false;
    Host = "";
    userName = "";
    password = "";
    port = "389";
    dnFieldName = null;
    failIfNotExist = true;
    multiValuedSeparator = ";";
    searchBase = null;
    oldDnFieldName = null;
    newDnFieldName = null;
    deleteRDN = true;

    int nrFields = 0;
    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      updateLookup[ i ] = "name" + ( i + 1 );
      updateStream[ i ] = "field" + ( i + 1 );
      update[ i ] = Boolean.TRUE;
    }
    operationType = OPERATION_TYPE_INSERT;
    referralType = REFERRAL_TYPE_FOLLOW;
    derefAliasesType = DEREFALIASES_TYPE_ALWAYS;
    this.trustStorePath = null;
    this.trustStorePassword = null;
    this.trustAllCertificates = false;
    this.protocol = LdapProtocolFactory.getConnectionTypes( log ).get( 0 );
    this.useCertificate = false;
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;

    // See if we get input...
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "LDAPOutputMeta.CheckResult.NoInputExpected" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "LDAPOutputMeta.CheckResult.NoInput" ), transformMeta );
    }
    remarks.add( cr );

    // Check hostname
    if ( Utils.isEmpty( Host ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "LDAPOutputMeta.CheckResult.HostnameMissing" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "LDAPOutputMeta.CheckResult.HostnameOk" ), transformMeta );
    }
    remarks.add( cr );

    // check return fields
    if ( updateLookup.length == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "LDAPOutputUpdateMeta.CheckResult.NoFields" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "LDAPOutputUpdateMeta.CheckResult.FieldsOk" ), transformMeta );
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new LDAPOutput( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new LDAPOutputData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  public String toString() {
    return "LDAPConnection " + getName();
  }

  @Override
  public String getDerefAliases() {
    return LDAPOutputMeta.getDerefAliasesCode( getDerefAliasesType() );
  }

  @Override
  public String getReferrals() {
    return getReferralTypeCode( getReferralType() );
  }
}
