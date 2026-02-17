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
package org.apache.hop.pipeline.transforms.ldapinput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "LDAPInput",
    name = "i18n::LdapInput.Name",
    description = "i18n::LdapInput.Description",
    image = "ldapinput.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::LdapInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/ldapinput.html")
public class LdapInputMeta extends BaseTransformMeta<LdapInput, LdapInputData>
    implements ILdapMeta {
  private static final Class<?> PKG = LdapInputMeta.class;
  public static final String CONST_SPACES = "        ";
  public static final String CONST_FIELD = "field";

  /** Flag indicating that we use authentication for connection */
  @HopMetadataProperty(key = "useauthentication")
  private boolean useAuthentication;

  /** Flag indicating that we use paging */
  @HopMetadataProperty(key = "usepaging")
  private boolean usePaging;

  /** page size */
  @HopMetadataProperty(key = "pagesize")
  private String pageSize;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key = "rownum")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key = "rownum_field")
  private String rowNumberField;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "limit")
  private int rowLimit;

  /** The Host name */
  @HopMetadataProperty(key = "host")
  private String host;

  /** The User name */
  @HopMetadataProperty(key = "username")
  private String userName;

  /** The Password to use in LDAP authentication */
  @HopMetadataProperty(key = "password", password = true)
  private String password;

  /** The Port */
  @HopMetadataProperty(key = "port")
  private String port;

  /** The Filter string */
  @HopMetadataProperty(key = "filterstring")
  private String filterString;

  /** The Search Base */
  @HopMetadataProperty(key = "searchbase")
  private String searchBase;

  /** The fields to import... */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<LdapInputField> inputFields;

  /** The Time limit */
  @HopMetadataProperty(key = "timelimit")
  private int timeLimit;

  /** Multi valued separator */
  @HopMetadataProperty(key = "multivaluedseparator")
  private String multiValuedSeparator;

  private static final String YES = "Y";

  @HopMetadataProperty(key = "dynamicsearch")
  private boolean dynamicSearch;

  @HopMetadataProperty(key = "dynamicseachfieldname")
  private String dynamicSearchFieldName;

  @HopMetadataProperty(key = "dynamicfilter")
  private boolean dynamicFilter;

  @HopMetadataProperty(key = "dynamicfilterfieldname")
  private String dynamicFilterFieldName;

  /** Search scope */
  @HopMetadataProperty(key = "searchScope", intCodeConverter = LdapInputSearchScopeConverter.class)
  private int searchScope;

  /** The search scopes description */
  public static final String[] searchScopeDesc = {
    BaseMessages.getString(PKG, "LdapInputMeta.SearchScope.Object"),
    BaseMessages.getString(PKG, "LdapInputMeta.SearchScope.OneLevel"),
    BaseMessages.getString(PKG, "LdapInputMeta.SearchScope.Subtree")
  };

  /** The search scope codes */
  public static final String[] searchScopeCode = {"object", "onelevel", "subtree"};

  /** Protocol */
  @HopMetadataProperty(key = "protocol")
  private String protocol;

  /** Trust store */
  @HopMetadataProperty(key = "useCertificate")
  private boolean useCertificate;

  @HopMetadataProperty(key = "trustStorePath")
  private String trustStorePath;

  @HopMetadataProperty(key = "trustStorePassword", password = true)
  private String trustStorePassword;

  @HopMetadataProperty(key = "trustAllCertificates")
  private boolean trustAllCertificates;

  public LdapInputMeta() {
    super(); // allocate BaseTransformMeta
    this.inputFields = new ArrayList<>();
  }

  /**
   * @return Returns the input useCertificate.
   */
  @Override
  public boolean isUseCertificate() {
    return useCertificate;
  }

  public void setUseCertificate(boolean value) {
    this.useCertificate = value;
  }

  /**
   * @return Returns the input trustAllCertificates.
   */
  @Override
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  public void setTrustAllCertificates(boolean value) {
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
  public void setTrustStorePassword(String value) {
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
  public void setTrustStorePath(String value) {
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
  public void setProtocol(String value) {
    this.protocol = value;
  }

  /**
   * @return Returns the input dynamicSearch.
   */
  public boolean isDynamicSearch() {
    return dynamicSearch;
  }

  public void setDynamicSearch(boolean dynamicSearch) {
    this.dynamicSearch = dynamicSearch;
  }

  /**
   * @return Returns the input dynamicSeachFieldName.
   */
  public String getDynamicSearchFieldName() {
    return dynamicSearchFieldName;
  }

  public void setDynamicSearchFieldName(String dynamicSeachFieldName) {
    this.dynamicSearchFieldName = dynamicSeachFieldName;
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
  public void setDynamicFilter(boolean dynamicFilter) {
    this.dynamicFilter = dynamicFilter;
  }

  /**
   * @return Returns the input dynamicFilterFieldName.
   */
  public String getDynamicFilterFieldName() {
    return dynamicFilterFieldName;
  }

  /** param dynamicFilterFieldName the dynamicFilterFieldName to set. */
  public void setDynamicFilterFieldName(String dynamicFilterFieldName) {
    this.dynamicFilterFieldName = dynamicFilterFieldName;
  }

  /**
   * @return Returns the input useAuthentication.
   */
  public boolean isUseAuthentication() {
    return useAuthentication;
  }

  /**
   * @param useAuthentication The useAuthentication to set.
   */
  public void setUseAuthentication(boolean useAuthentication) {
    this.useAuthentication = useAuthentication;
  }

  /**
   * @return Returns the input usePaging.
   */
  public boolean isUsePaging() {
    return usePaging;
  }

  /**
   * @param usePaging The usePaging to set.
   */
  public void setUsePaging(boolean usePaging) {
    this.usePaging = usePaging;
  }

  /**
   * @return Returns the input fields as a List (for reflection-based serialization).
   */
  public List<LdapInputField> getInputFields() {
    if (inputFields == null) {
      inputFields = new ArrayList<>();
    }
    return inputFields;
  }

  /**
   * @param inputFields The input fields to set (for reflection-based serialization).
   */
  public void setInputFields(List<LdapInputField> inputFields) {
    this.inputFields = inputFields != null ? new ArrayList<>(inputFields) : new ArrayList<>();
  }

  /**
   * @return Returns the input fields as an array (for backward compatibility).
   */
  public LdapInputField[] getInputFieldsArray() {
    if (inputFields == null) return new LdapInputField[0];
    return inputFields.toArray(new LdapInputField[0]);
  }

  /**
   * @param inputFields The input fields to set (for backward compatibility).
   */
  public void setInputFields(LdapInputField[] inputFields) {
    this.inputFields = inputFields != null ? Arrays.asList(inputFields) : new ArrayList<>();
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean isIncludeRowNumber() {
    return includeRowNumber;
  }

  /**
   * @param includeRowNumber The includeRowNumber to set.
   */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * @return Returns the host name.
   */
  @Override
  public String getHost() {
    return host;
  }

  /**
   * @param host The host to set.
   */
  public void setHost(String host) {
    this.host = host;
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
  public void setUserName(String userName) {
    this.userName = userName;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword(String password) {
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
  public void setPort(String port) {
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
  public void setFilterString(String filterString) {
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
  public void setSearchBase(String searchBase) {
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
  public void setTimeLimit(int timeLimit) {
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
  public void setMultiValuedSeparator(String multiValuedSeparator) {
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
  public void setPageSize(String pagesize) {
    this.pageSize = pagesize;
  }

  /**
   * @return Returns the pagesize.
   */
  public String getPageSize() {
    return pageSize;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit(int rowLimit) {
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
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
  }

  @Override
  public Object clone() {
    LdapInputMeta retval = (LdapInputMeta) super.clone();
    if (inputFields != null) {
      retval.inputFields = new ArrayList<>();
      for (LdapInputField field : inputFields) {
        if (field != null) {
          retval.inputFields.add((LdapInputField) field.clone());
        }
      }
    } else {
      retval.inputFields = new ArrayList<>();
    }
    return retval;
  }

  public static int getSearchScopeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < searchScopeCode.length; i++) {
      if (searchScopeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public void allocate(int nrFields) {
    inputFields = new ArrayList<>(nrFields);
    for (int i = 0; i < nrFields; i++) {
      inputFields.add(new LdapInputField());
    }
  }

  @Override
  public void setDefault() {
    this.usePaging = false;
    this.pageSize = "1000";
    this.useAuthentication = false;
    this.includeRowNumber = false;
    this.rowNumberField = "";
    this.host = "";
    this.userName = "";
    this.password = "";
    this.port = "389";
    this.filterString = LdapConnection.DEFAUL_FILTER_STRING;
    this.searchBase = "";
    this.multiValuedSeparator = ";";
    this.dynamicSearch = false;
    this.dynamicSearchFieldName = null;
    this.dynamicFilter = false;
    this.dynamicFilterFieldName = null;
    this.inputFields = new ArrayList<>();
    this.rowLimit = 0;
    this.timeLimit = 0;
    this.searchScope = LdapConnection.SEARCH_SCOPE_SUBTREE_SCOPE;
    this.trustStorePath = null;
    this.trustStorePassword = null;
    this.trustAllCertificates = false;
    this.protocol = LdapProtocolFactory.getConnectionTypes(log).get(0);
    this.useCertificate = false;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    int i;
    if (inputFields == null) {
      return;
    }
    for (i = 0; i < inputFields.size(); i++) {
      LdapInputField field = inputFields.get(i);

      int type = field.getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(variables.resolve(field.getName()), type);
        v.setLength(field.getLength(), field.getPrecision());
        v.setOrigin(name);
        r.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }

    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public static String getSearchScopeDesc(int i) {
    if (i < 0 || i >= searchScopeDesc.length) {
      return searchScopeDesc[0];
    }
    return searchScopeDesc[i];
  }

  public static int getSearchScopeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < searchScopeDesc.length; i++) {
      if (searchScopeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getSearchScopeByCode(tt);
  }

  public void setSearchScope(int value) {
    this.searchScope = value;
  }

  public int getSearchScope() {
    return searchScope;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;

    // Check output fields
    if (inputFields == null || inputFields.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.NoOutputFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.OutputFieldsOk"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we get input...
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // Check hostname
    if (Utils.isEmpty(host)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.HostnameMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.HostnameOk"),
              transformMeta);
    }
    remarks.add(cr);

    if (isDynamicSearch()) {
      if (Utils.isEmpty(dynamicSearchFieldName)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "LdapInputMeta.CheckResult.DynamicSearchBaseFieldNameMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "LdapInputMeta.CheckResult.DynamicSearchBaseFieldNameOk"),
                transformMeta);
      }
      remarks.add(cr);
    } else {
      // Check search base
      if (Utils.isEmpty(searchBase)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.SearchBaseMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.SearchBaseOk"),
                transformMeta);
      }
      remarks.add(cr);
    }
    if (isDynamicFilter()) {
      if (Utils.isEmpty(dynamicFilterFieldName)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "LdapInputMeta.CheckResult.DynamicFilterFieldNameMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.DynamicFilterFieldNameOk"),
                transformMeta);
      }
      remarks.add(cr);
    } else {
      // Check filter String
      if (Utils.isEmpty(filterString)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.FilterStringMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.FilterStringOk"),
                transformMeta);
      }
      remarks.add(cr);
    }
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
