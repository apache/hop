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
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
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

@Getter
@Setter
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

  /**
   * Flag indicating that we use authentication for connection -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the input useAuthentication.
   * @param useAuthentication The useAuthentication to set.
   */
  @HopMetadataProperty(key = "useauthentication")
  private boolean useAuthentication;

  /**
   * Flag indicating that we use paging -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the input usePaging.
   * @param usePaging The usePaging to set.
   */
  @HopMetadataProperty(key = "usepaging")
  private boolean usePaging;

  /**
   * page size -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param pagesize The pagesize.
   * @return Returns the pagesize.
   */
  @HopMetadataProperty(key = "pagesize")
  private String pageSize;

  /**
   * Flag indicating that a row number field should be included in the output -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the includeRowNumber.
   * @param includeRowNumber The includeRowNumber to set.
   */
  @HopMetadataProperty(key = "rownum")
  private boolean includeRowNumber;

  /**
   * The name of the field in the output containing the row number -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the rowNumberField.
   * @param rowNumberField The rowNumberField to set.
   */
  @HopMetadataProperty(key = "rownum_field")
  private String rowNumberField;

  /**
   * The maximum number or lines to read -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the rowLimit.
   * @param rowLimit The rowLimit to set.
   */
  @HopMetadataProperty(key = "limit")
  private int rowLimit;

  /**
   * The Host name -- SETTER --
   *
   * @param host The host to set.
   */
  @HopMetadataProperty(key = "host")
  private String host;

  /**
   * The User name -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the user name.
   * @param userName The username to set.
   */
  @HopMetadataProperty(key = "username")
  private String userName;

  /**
   * The Password to use in LDAP authentication -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param password The password to set.
   * @return Returns the password.
   */
  @HopMetadataProperty(key = "password", password = true)
  private String password;

  /**
   * The Port -- SETTER --
   *
   * @param port The port to set.
   */
  @HopMetadataProperty(key = "port")
  private String port;

  /**
   * The Filter string -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the filter string.
   * @param filterString The filter string to set.
   */
  @HopMetadataProperty(key = "filterstring")
  private String filterString;

  /**
   * The Search Base -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the search string.
   * @param searchBase The filter Search Base to set.
   */
  @HopMetadataProperty(key = "searchbase")
  private String searchBase;

  /** The fields to import... */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<LdapInputField> inputFields;

  /**
   * The Time limit -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param timeLimit The timeout time limit to set.
   * @return Returns the time limit.
   */
  @HopMetadataProperty(key = "timelimit")
  private int timeLimit;

  /**
   * Multi valued separator -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param multiValuedSeparator The multi-valued separator filed.
   * @return Returns the multi valued separator.
   */
  @HopMetadataProperty(key = "multivaluedseparator")
  private String multiValuedSeparator;

  private static final String YES = "Y";

  /**
   * -- GETTER --
   *
   * @return Returns the input dynamicSearch.
   */
  @HopMetadataProperty(key = "dynamicsearch")
  private boolean dynamicSearch;

  /**
   * -- GETTER --
   *
   * @return Returns the input dynamicSeachFieldName.
   */
  @HopMetadataProperty(key = "dynamicseachfieldname")
  private String dynamicSearchFieldName;

  /**
   * -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the input dynamicFilter.
   * @param dynamicFilter the dynamicFilter to set.
   */
  @HopMetadataProperty(key = "dynamicfilter")
  private boolean dynamicFilter;

  /**
   * -- GETTER --
   *
   * <p>-- SETTER -- param dynamicFilterFieldName the dynamicFilterFieldName to set.
   *
   * @return Returns the input dynamicFilterFieldName.
   */
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

  /**
   * Protocol -- SETTER --
   *
   * @param value the protocol to set.
   */
  @HopMetadataProperty(key = "protocol")
  private String protocol;

  /** Trust store */
  @HopMetadataProperty(key = "useCertificate")
  private boolean useCertificate;

  /**
   * -- SETTER --
   *
   * @param value the trustStorePath to set.
   */
  @HopMetadataProperty(key = "trustStorePath")
  private String trustStorePath;

  /**
   * -- SETTER --
   *
   * @param value the trustStorePassword to set.
   */
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

  /**
   * @return Returns the input trustAllCertificates.
   */
  @Override
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  /**
   * @return Returns the trustStorePath.
   */
  @Override
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * @return Returns the trustStorePath.
   */
  @Override
  public String getTrustStorePath() {
    return trustStorePath;
  }

  /**
   * @return Returns the protocol.
   */
  @Override
  public String getProtocol() {
    return protocol;
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
   * @return Returns the host name.
   */
  @Override
  public String getHost() {
    return host;
  }

  /**
   * @return Returns the Port.
   */
  @Override
  public String getPort() {
    return port;
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
