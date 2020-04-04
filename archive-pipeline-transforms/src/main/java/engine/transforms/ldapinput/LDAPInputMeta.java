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

package org.apache.hop.pipeline.transforms.ldapinput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
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
import org.w3c.dom.Node;

import java.util.List;

public class LDAPInputMeta extends BaseTransformMeta implements LdapMeta {
  private static Class<?> PKG = LDAPInputMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * Flag indicating that we use authentication for connection
   */
  private boolean useAuthentication;

  /**
   * Flag indicating that we use paging
   */
  private boolean usePaging;

  /**
   * page size
   */
  private String pagesize;

  /**
   * Flag indicating that a row number field should be included in the output
   */
  private boolean includeRowNumber;

  /**
   * The name of the field in the output containing the row number
   */
  private String rowNumberField;

  /**
   * The maximum number or lines to read
   */
  private int rowLimit;

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
   * The Filter string
   */
  private String filterString;

  /**
   * The Search Base
   */
  private String searchBase;

  /**
   * The fields to import...
   */
  private LDAPInputField[] inputFields;

  /**
   * The Time limit
   **/
  private int timeLimit;

  /**
   * Multi valued separator
   **/
  private String multiValuedSeparator;

  private static final String YES = "Y";

  private boolean dynamicSearch;
  private String dynamicSeachFieldName;

  private boolean dynamicFilter;
  private String dynamicFilterFieldName;

  /**
   * Search scope
   */
  private int searchScope;

  /**
   * The search scopes description
   */
  public static final String[] searchScopeDesc = {
    BaseMessages.getString( PKG, "LDAPInputMeta.SearchScope.Object" ),
    BaseMessages.getString( PKG, "LDAPInputMeta.SearchScope.OneLevel" ),
    BaseMessages.getString( PKG, "LDAPInputMeta.SearchScope.Subtree" ) };

  /**
   * The search scope codes
   */
  public static final String[] searchScopeCode = { "object", "onelevel", "subtree" };

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

  public LDAPInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the input useCertificate.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public String getProtocol() {
    return protocol;
  }

  /**
   * @param value the protocol to set.
   */
  public void setProtocol( String value ) {
    this.protocol = value;
  }

  /**
   * @return Returns the input dynamicSearch.
   */
  public boolean isDynamicSearch() {
    return dynamicSearch;
  }

  /**
   * @return Returns the input dynamicSearch.
   */
  public void setDynamicSearch( boolean dynamicSearch ) {
    this.dynamicSearch = dynamicSearch;
  }

  /**
   * @return Returns the input dynamicSeachFieldName.
   */
  public String getDynamicSearchFieldName() {
    return dynamicSeachFieldName;
  }

  /**
   * @return Returns the input dynamicSeachFieldName.
   */
  public void setDynamicSearchFieldName( String dynamicSeachFieldName ) {
    this.dynamicSeachFieldName = dynamicSeachFieldName;
  }

  /**
   * @return Returns the input dynamicFilter.
   */
  public boolean isDynamicFilter() {
    return dynamicFilter;
  }

  /**
   * @param dynamicFilter the dynamicFilter to set.
   */
  public void setDynamicFilter( boolean dynamicFilter ) {
    this.dynamicFilter = dynamicFilter;
  }

  /**
   * @return Returns the input dynamicFilterFieldName.
   */
  public String getDynamicFilterFieldName() {
    return dynamicFilterFieldName;
  }

  /**
   * param dynamicFilterFieldName the dynamicFilterFieldName to set.
   */
  public void setDynamicFilterFieldName( String dynamicFilterFieldName ) {
    this.dynamicFilterFieldName = dynamicFilterFieldName;
  }

  /**
   * @return Returns the input useAuthentication.
   */
  public boolean UseAuthentication() {
    return useAuthentication;
  }

  /**
   * @param useAuthentication The useAuthentication to set.
   */
  public void setUseAuthentication( boolean useAuthentication ) {
    this.useAuthentication = useAuthentication;
  }

  /**
   * @return Returns the input usePaging.
   */
  public boolean isPaging() {
    return usePaging;
  }

  /**
   * @param usePaging The usePaging to set.
   */
  public void setPaging( boolean usePaging ) {
    this.usePaging = usePaging;
  }

  /**
   * @return Returns the input fields.
   */
  public LDAPInputField[] getInputFields() {
    return inputFields;
  }

  /**
   * @param inputFields The input fields to set.
   */
  public void setInputFields( LDAPInputField[] inputFields ) {
    this.inputFields = inputFields;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /**
   * @param includeRowNumber The includeRowNumber to set.
   */
  public void setIncludeRowNumber( boolean includeRowNumber ) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * @return Returns the host name.
   */
  @Override
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

  /**
   * @return Returns the Port.
   */
  @Override
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
   * @return Returns the filter string.
   */
  public String getFilterString() {
    return filterString;
  }

  /**
   * @param filterString The filter string to set.
   */
  public void setFilterString( String filterString ) {
    this.filterString = filterString;
  }

  /**
   * @return Returns the search string.
   */
  public String getSearchBase() {
    return searchBase;
  }

  /**
   * @param searchBase The filter Search Base to set.
   */
  public void setSearchBase( String searchBase ) {
    this.searchBase = searchBase;
  }

  /**
   * @return Returns the rowLimit.
   */
  public int getRowLimit() {
    return rowLimit;
  }

  /**
   * @param timeLimit The timeout time limit to set.
   */
  public void setTimeLimit( int timeLimit ) {
    this.timeLimit = timeLimit;
  }

  /**
   * @return Returns the time limit.
   */
  public int getTimeLimit() {
    return timeLimit;
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

  /**
   * @param pagesize The pagesize.
   */
  public void setPageSize( String pagesize ) {
    this.pagesize = pagesize;
  }

  /**
   * @return Returns the pagesize.
   */
  public String getPageSize() {
    return pagesize;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit( int rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the rowNumberField.
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /**
   * @param rowNumberField The rowNumberField to set.
   */
  public void setRowNumberField( String rowNumberField ) {
    this.rowNumberField = rowNumberField;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    LDAPInputMeta retval = (LDAPInputMeta) super.clone();

    int nrFields = inputFields.length;

    retval.allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      if ( inputFields[ i ] != null ) {
        retval.inputFields[ i ] = (LDAPInputField) inputFields[ i ].clone();
      }
    }

    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "usepaging", usePaging ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "pagesize", pagesize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "useauthentication", useAuthentication ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownum", includeRowNumber ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownum_field", rowNumberField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "host", Host ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "username", userName ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "port", port ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "filterstring", filterString ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "searchbase", searchBase ) );

    /*
     * Describe the fields to read
     */
    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", inputFields[ i ].getName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "attribute", inputFields[ i ].getAttribute() ) );
      retval.append( "        " ).append(
        XMLHandler.addTagValue( "attribute_fetch_as", inputFields[ i ].getFetchAttributeAsCode() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "sorted_key", inputFields[ i ].isSortedKey() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", inputFields[ i ].getTypeDesc() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "format", inputFields[ i ].getFormat() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "length", inputFields[ i ].getLength() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "precision", inputFields[ i ].getPrecision() ) );
      retval
        .append( "        " ).append( XMLHandler.addTagValue( "currency", inputFields[ i ].getCurrencySymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "decimal", inputFields[ i ].getDecimalSymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "group", inputFields[ i ].getGroupSymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "trim_type", inputFields[ i ].getTrimTypeCode() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "repeat", inputFields[ i ].isRepeated() ) );

      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );

    retval.append( "    " ).append( XMLHandler.addTagValue( "limit", rowLimit ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "timelimit", timeLimit ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "multivaluedseparator", multiValuedSeparator ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dynamicsearch", dynamicSearch ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dynamicseachfieldname", dynamicSeachFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dynamicfilter", dynamicFilter ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dynamicfilterfieldname", dynamicFilterFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "searchScope", getSearchScopeCode( searchScope ) ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "protocol", protocol ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "trustStorePath", trustStorePath ) );
    retval.append( "    " ).append(
      XMLHandler
        .addTagValue( "trustStorePassword", Encr.encryptPasswordIfNotUsingVariables( trustStorePassword ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "trustAllCertificates", trustAllCertificates ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "useCertificate", useCertificate ) );

    return retval.toString();
  }

  private static String getSearchScopeCode( int i ) {
    if ( i < 0 || i >= searchScopeCode.length ) {
      return searchScopeCode[ 0 ];
    }
    return searchScopeCode[ i ];
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {

      usePaging = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "usepaging" ) );
      pagesize = XMLHandler.getTagValue( transformNode, "pagesize" );
      useAuthentication = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "useauthentication" ) );
      includeRowNumber = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "rownum" ) );
      rowNumberField = XMLHandler.getTagValue( transformNode, "rownum_field" );
      Host = XMLHandler.getTagValue( transformNode, "host" );
      userName = XMLHandler.getTagValue( transformNode, "username" );
      setPassword( Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "password" ) ) );

      port = XMLHandler.getTagValue( transformNode, "port" );
      filterString = XMLHandler.getTagValue( transformNode, "filterstring" );
      searchBase = XMLHandler.getTagValue( transformNode, "searchbase" );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        inputFields[ i ] = new LDAPInputField();

        inputFields[ i ].setName( XMLHandler.getTagValue( fnode, "name" ) );
        inputFields[ i ].setAttribute( XMLHandler.getTagValue( fnode, "attribute" ) );
        inputFields[ i ].setFetchAttributeAs( LDAPInputField.getFetchAttributeAsByCode( XMLHandler.getTagValue(
          fnode, "attribute_fetch_as" ) ) );
        String sortedkey = XMLHandler.getTagValue( fnode, "sorted_key" );
        if ( sortedkey != null ) {
          inputFields[ i ].setSortedKey( YES.equalsIgnoreCase( sortedkey ) );
        } else {
          inputFields[ i ].setSortedKey( false );
        }
        inputFields[ i ].setType( ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "type" ) ) );
        inputFields[ i ].setLength( Const.toInt( XMLHandler.getTagValue( fnode, "length" ), -1 ) );
        inputFields[ i ].setPrecision( Const.toInt( XMLHandler.getTagValue( fnode, "precision" ), -1 ) );
        String srepeat = XMLHandler.getTagValue( fnode, "repeat" );
        if ( srepeat != null ) {
          inputFields[ i ].setRepeated( YES.equalsIgnoreCase( srepeat ) );
        } else {
          inputFields[ i ].setRepeated( false );
        }
        inputFields[ i ].setTrimType( ValueMetaString.getTrimTypeByCode( XMLHandler.getTagValue( fnode, "trim_type" ) ) );

        inputFields[ i ].setFormat( XMLHandler.getTagValue( fnode, "format" ) );
        inputFields[ i ].setCurrencySymbol( XMLHandler.getTagValue( fnode, "currency" ) );
        inputFields[ i ].setDecimalSymbol( XMLHandler.getTagValue( fnode, "decimal" ) );
        inputFields[ i ].setGroupSymbol( XMLHandler.getTagValue( fnode, "group" ) );

      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toInt( XMLHandler.getTagValue( transformNode, "limit" ), 0 );
      timeLimit = Const.toInt( XMLHandler.getTagValue( transformNode, "timelimit" ), 0 );
      multiValuedSeparator = XMLHandler.getTagValue( transformNode, "multivaluedseparator" );
      dynamicSearch = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "dynamicsearch" ) );
      dynamicSeachFieldName = XMLHandler.getTagValue( transformNode, "dynamicseachfieldname" );
      dynamicFilter = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "dynamicfilter" ) );
      dynamicFilterFieldName = XMLHandler.getTagValue( transformNode, "dynamicfilterfieldname" );
      searchScope =
        getSearchScopeByCode( Const.NVL(
          XMLHandler.getTagValue( transformNode, "searchScope" ),
          getSearchScopeCode( LDAPConnection.SEARCH_SCOPE_SUBTREE_SCOPE ) ) );

      protocol = XMLHandler.getTagValue( transformNode, "protocol" );
      trustStorePath = XMLHandler.getTagValue( transformNode, "trustStorePath" );
      trustStorePassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "trustStorePassword" ) );
      trustAllCertificates = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "trustAllCertificates" ) );
      useCertificate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "useCertificate" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "LDAPInputMeta.UnableToLoadFromXML" ), e );
    }
  }

  private static int getSearchScopeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < searchScopeCode.length; i++ ) {
      if ( searchScopeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public void allocate( int nrFields ) {

    inputFields = new LDAPInputField[ nrFields ];
  }

  @Override
  public void setDefault() {
    this.usePaging = false;
    this.pagesize = "1000";
    this.useAuthentication = false;
    this.includeRowNumber = false;
    this.rowNumberField = "";
    this.Host = "";
    this.userName = "";
    this.password = "";
    this.port = "389";
    this.filterString = LDAPConnection.DEFAUL_FILTER_STRING;
    this.searchBase = "";
    this.multiValuedSeparator = ";";
    this.dynamicSearch = false;
    this.dynamicSeachFieldName = null;
    this.dynamicFilter = false;
    this.dynamicFilterFieldName = null;
    int nrFields = 0;

    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      this.inputFields[ i ] = new LDAPInputField( "field" + ( i + 1 ) );
    }

    this.rowLimit = 0;
    this.timeLimit = 0;
    this.searchScope = LDAPConnection.SEARCH_SCOPE_SUBTREE_SCOPE;
    this.trustStorePath = null;
    this.trustStorePassword = null;
    this.trustAllCertificates = false;
    this.protocol = LdapProtocolFactory.getConnectionTypes( log ).get( 0 );
    this.useCertificate = false;
  }

  @Override
  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    int i;
    for ( i = 0; i < inputFields.length; i++ ) {
      LDAPInputField field = inputFields[ i ];

      int type = field.getType();
      if ( type == IValueMeta.TYPE_NONE ) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v =
          ValueMetaFactory.createValueMeta( variables.environmentSubstitute( field.getName() ), type );
        v.setLength( field.getLength(), field.getPrecision() );
        v.setOrigin( name );
        r.addValueMeta( v );
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }

    }

    if ( includeRowNumber ) {
      IValueMeta v = new ValueMetaInteger( variables.environmentSubstitute( rowNumberField ) );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
  }

  public static String getSearchScopeDesc( int i ) {
    if ( i < 0 || i >= searchScopeDesc.length ) {
      return searchScopeDesc[ 0 ];
    }
    return searchScopeDesc[ i ];
  }

  public static int getSearchScopeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < searchScopeDesc.length; i++ ) {
      if ( searchScopeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getSearchScopeByCode( tt );
  }

  public void setSearchScope( int value ) {
    this.searchScope = value;
  }

  public int getSearchScope() {
    return searchScope;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;

    // Check output fields
    if ( inputFields.length == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "LDAPInputMeta.CheckResult.NoOutputFields" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "LDAPInputMeta.CheckResult.OutputFieldsOk" ), transformMeta );
    }
    remarks.add( cr );

    // See if we get input...
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "LDAPInputMeta.CheckResult.NoInputExpected" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "LDAPInputMeta.CheckResult.NoInput" ), transformMeta );
    }
    remarks.add( cr );

    // Check hostname
    if ( Utils.isEmpty( Host ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "LDAPInputMeta.CheckResult.HostnameMissing" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "LDAPInputMeta.CheckResult.HostnameOk" ), transformMeta );
    }
    remarks.add( cr );

    if ( isDynamicSearch() ) {
      if ( Utils.isEmpty( dynamicSeachFieldName ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.DynamicSearchBaseFieldNameMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.DynamicSearchBaseFieldNameOk" ), transformMeta );
      }
      remarks.add( cr );
    } else {
      // Check search base
      if ( Utils.isEmpty( searchBase ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.SearchBaseMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.SearchBaseOk" ), transformMeta );
      }
      remarks.add( cr );
    }
    if ( isDynamicFilter() ) {
      if ( Utils.isEmpty( dynamicFilterFieldName ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.DynamicFilterFieldNameMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.DynamicFilterFieldNameOk" ), transformMeta );
      }
      remarks.add( cr );
    } else {
      // Check filter String
      if ( Utils.isEmpty( filterString ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.FilterStringMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "LDAPInputMeta.CheckResult.FilterStringOk" ), transformMeta );
      }
      remarks.add( cr );
    }

  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new LDAPInput( transformMeta, this, data, cnr, tr, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new LDAPInputData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public String toString() {
    return "LDAPConnection " + getName();
  }

  @Override
  public String getDerefAliases() {
    return "always";
  }

  @Override
  public String getReferrals() {
    return "follow";
  }
}
