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

import static org.apache.hop.core.ICheckResult.*;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "LDAPInput",
    name = "i18n::LdapInput.Name",
    description = "i18n::LdapInput.Description",
    image = "ldapinput.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = {"ldap", "input"},
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/ldapinput.html")
public class LdapInputMeta extends BaseTransformMeta
    implements ILdapMeta, ITransformMeta<LdapInput, LdapInputData> {
  private static final Class<?> PKG = LdapInputMeta.class; // For Translator

  /** Flag indicating that we use authentication for connection */
  private boolean useAuthentication;

  /** Flag indicating that we use paging */
  private boolean usePaging;

  /** page size */
  private String pagesize;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  /** The maximum number or lines to read */
  private int rowLimit;

  /** The Host name */
  private String Host;

  /** The User name */
  private String userName;

  /** The Password to use in LDAP authentication */
  private String password;

  /** The Port */
  private String port;

  /** The Filter string */
  private String filterString;

  /** The Search Base */
  private String searchBase;

  /** The fields to import... */
  private LdapInputField[] inputFields;

  /** The Time limit */
  private int timeLimit;

  /** Multi valued separator */
  private String multiValuedSeparator;

  private static final String YES = "Y";

  private boolean dynamicSearch;
  private String dynamicSeachFieldName;

  private boolean dynamicFilter;
  private String dynamicFilterFieldName;

  /** Search scope */
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
  private String protocol;

  /** Trust store */
  private boolean useCertificate;

  private String trustStorePath;
  private String trustStorePassword;
  private boolean trustAllCertificates;

  public LdapInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the input useCertificate. */
  @Override
  public boolean isUseCertificate() {
    return useCertificate;
  }

  /** @return Returns the useCertificate. */
  public void setUseCertificate(boolean value) {
    this.useCertificate = value;
  }

  /** @return Returns the input trustAllCertificates. */
  @Override
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  /** @return Returns the input trustAllCertificates. */
  public void setTrustAllCertificates(boolean value) {
    this.trustAllCertificates = value;
  }

  /** @return Returns the trustStorePath. */
  @Override
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /** @param value the trustStorePassword to set. */
  public void setTrustStorePassword(String value) {
    this.trustStorePassword = value;
  }

  /** @return Returns the trustStorePath. */
  @Override
  public String getTrustStorePath() {
    return trustStorePath;
  }

  /** @param value the trustStorePath to set. */
  public void setTrustStorePath(String value) {
    this.trustStorePath = value;
  }

  /** @return Returns the protocol. */
  @Override
  public String getProtocol() {
    return protocol;
  }

  /** @param value the protocol to set. */
  public void setProtocol(String value) {
    this.protocol = value;
  }

  /** @return Returns the input dynamicSearch. */
  public boolean isDynamicSearch() {
    return dynamicSearch;
  }

  /** @return Returns the input dynamicSearch. */
  public void setDynamicSearch(boolean dynamicSearch) {
    this.dynamicSearch = dynamicSearch;
  }

  /** @return Returns the input dynamicSeachFieldName. */
  public String getDynamicSearchFieldName() {
    return dynamicSeachFieldName;
  }

  /** @return Returns the input dynamicSeachFieldName. */
  public void setDynamicSearchFieldName(String dynamicSeachFieldName) {
    this.dynamicSeachFieldName = dynamicSeachFieldName;
  }

  /** @return Returns the input dynamicFilter. */
  public boolean isDynamicFilter() {
    return dynamicFilter;
  }

  /** @param dynamicFilter the dynamicFilter to set. */
  public void setDynamicFilter(boolean dynamicFilter) {
    this.dynamicFilter = dynamicFilter;
  }

  /** @return Returns the input dynamicFilterFieldName. */
  public String getDynamicFilterFieldName() {
    return dynamicFilterFieldName;
  }

  /** param dynamicFilterFieldName the dynamicFilterFieldName to set. */
  public void setDynamicFilterFieldName(String dynamicFilterFieldName) {
    this.dynamicFilterFieldName = dynamicFilterFieldName;
  }

  /** @return Returns the input useAuthentication. */
  public boolean isUseAuthentication() {
    return useAuthentication;
  }

  /** @param useAuthentication The useAuthentication to set. */
  public void setUseAuthentication(boolean useAuthentication) {
    this.useAuthentication = useAuthentication;
  }

  /** @return Returns the input usePaging. */
  public boolean isPaging() {
    return usePaging;
  }

  /** @param usePaging The usePaging to set. */
  public void setPaging(boolean usePaging) {
    this.usePaging = usePaging;
  }

  /** @return Returns the input fields. */
  public LdapInputField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields The input fields to set. */
  public void setInputFields(LdapInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  /** @return Returns the includeRowNumber. */
  public boolean isIncludeRowNumber() {
    return includeRowNumber;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /** @return Returns the host name. */
  @Override
  public String getHost() {
    return Host;
  }

  /** @param host The host to set. */
  public void setHost(String host) {
    this.Host = host;
  }

  /** @return Returns the user name. */
  public String getUserName() {
    return userName;
  }

  /** @param userName The username to set. */
  public void setUserName(String userName) {
    this.userName = userName;
  }

  /** @param password The password to set. */
  public void setPassword(String password) {
    this.password = password;
  }

  /** @return Returns the password. */
  public String getPassword() {
    return password;
  }

  /** @return Returns the Port. */
  @Override
  public String getPort() {
    return port;
  }

  /** @param port The port to set. */
  public void setPort(String port) {
    this.port = port;
  }

  /** @return Returns the filter string. */
  public String getFilterString() {
    return filterString;
  }

  /** @param filterString The filter string to set. */
  public void setFilterString(String filterString) {
    this.filterString = filterString;
  }

  /** @return Returns the search string. */
  public String getSearchBase() {
    return searchBase;
  }

  /** @param searchBase The filter Search Base to set. */
  public void setSearchBase(String searchBase) {
    this.searchBase = searchBase;
  }

  /** @return Returns the rowLimit. */
  public int getRowLimit() {
    return rowLimit;
  }

  /** @param timeLimit The timeout time limit to set. */
  public void setTimeLimit(int timeLimit) {
    this.timeLimit = timeLimit;
  }

  /** @return Returns the time limit. */
  public int getTimeLimit() {
    return timeLimit;
  }

  /** @param multiValuedSeparator The multi-valued separator filed. */
  public void setMultiValuedSeparator(String multiValuedSeparator) {
    this.multiValuedSeparator = multiValuedSeparator;
  }

  /** @return Returns the multi valued separator. */
  public String getMultiValuedSeparator() {
    return multiValuedSeparator;
  }

  /** @param pagesize The pagesize. */
  public void setPageSize(String pagesize) {
    this.pagesize = pagesize;
  }

  /** @return Returns the pagesize. */
  public String getPageSize() {
    return pagesize;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  /** @return Returns the rowNumberField. */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    LdapInputMeta retval = (LdapInputMeta) super.clone();

    int nrFields = inputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      if (inputFields[i] != null) {
        retval.inputFields[i] = (LdapInputField) inputFields[i].clone();
      }
    }

    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval.append("    ").append(XmlHandler.addTagValue("usepaging", usePaging));
    retval.append("    ").append(XmlHandler.addTagValue("pagesize", pagesize));
    retval.append("    ").append(XmlHandler.addTagValue("useauthentication", useAuthentication));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("host", Host));
    retval.append("    ").append(XmlHandler.addTagValue("username", userName));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));

    retval.append("    ").append(XmlHandler.addTagValue("port", port));
    retval.append("    ").append(XmlHandler.addTagValue("filterstring", filterString));
    retval.append("    ").append(XmlHandler.addTagValue("searchbase", searchBase));

    /*
     * Describe the fields to read
     */
    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", inputFields[i].getName()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("attribute", inputFields[i].getAttribute()));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "attribute_fetch_as", inputFields[i].getFetchAttributeAsCode()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("sorted_key", inputFields[i].isSortedKey()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("type", inputFields[i].getTypeDesc()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("format", inputFields[i].getFormat()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("length", inputFields[i].getLength()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("precision", inputFields[i].getPrecision()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("currency", inputFields[i].getCurrencySymbol()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("decimal", inputFields[i].getDecimalSymbol()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("group", inputFields[i].getGroupSymbol()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("trim_type", inputFields[i].getTrimTypeCode()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("repeat", inputFields[i].isRepeated()));

      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </fields>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));
    retval.append("    ").append(XmlHandler.addTagValue("timelimit", timeLimit));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("multivaluedseparator", multiValuedSeparator));
    retval.append("    ").append(XmlHandler.addTagValue("dynamicsearch", dynamicSearch));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("dynamicseachfieldname", dynamicSeachFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("dynamicfilter", dynamicFilter));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("dynamicfilterfieldname", dynamicFilterFieldName));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("searchScope", getSearchScopeCode(searchScope)));

    retval.append("    ").append(XmlHandler.addTagValue("protocol", protocol));
    retval.append("    ").append(XmlHandler.addTagValue("trustStorePath", trustStorePath));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "trustStorePassword", Encr.encryptPasswordIfNotUsingVariables(trustStorePassword)));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("trustAllCertificates", trustAllCertificates));
    retval.append("    ").append(XmlHandler.addTagValue("useCertificate", useCertificate));

    return retval.toString();
  }

  private static String getSearchScopeCode(int i) {
    if (i < 0 || i >= searchScopeCode.length) {
      return searchScopeCode[0];
    }
    return searchScopeCode[i];
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      usePaging = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "usepaging"));
      pagesize = XmlHandler.getTagValue(transformNode, "pagesize");
      useAuthentication =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "useauthentication"));
      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      Host = XmlHandler.getTagValue(transformNode, "host");
      userName = XmlHandler.getTagValue(transformNode, "username");
      setPassword(
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "password")));

      port = XmlHandler.getTagValue(transformNode, "port");
      filterString = XmlHandler.getTagValue(transformNode, "filterstring");
      searchBase = XmlHandler.getTagValue(transformNode, "searchbase");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        inputFields[i] = new LdapInputField();

        inputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
        inputFields[i].setAttribute(XmlHandler.getTagValue(fnode, "attribute"));
        inputFields[i].setFetchAttributeAs(
            LdapInputField.getFetchAttributeAsByCode(
                XmlHandler.getTagValue(fnode, "attribute_fetch_as")));
        String sortedkey = XmlHandler.getTagValue(fnode, "sorted_key");
        if (sortedkey != null) {
          inputFields[i].setSortedKey(YES.equalsIgnoreCase(sortedkey));
        } else {
          inputFields[i].setSortedKey(false);
        }
        inputFields[i].setType(
            ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fnode, "type")));
        inputFields[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        inputFields[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
        String srepeat = XmlHandler.getTagValue(fnode, "repeat");
        if (srepeat != null) {
          inputFields[i].setRepeated(YES.equalsIgnoreCase(srepeat));
        } else {
          inputFields[i].setRepeated(false);
        }
        inputFields[i].setTrimType(
            ValueMetaBase.getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));

        inputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
        inputFields[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        inputFields[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        inputFields[i].setGroupSymbol(XmlHandler.getTagValue(fnode, "group"));
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toInt(XmlHandler.getTagValue(transformNode, "limit"), 0);
      timeLimit = Const.toInt(XmlHandler.getTagValue(transformNode, "timelimit"), 0);
      multiValuedSeparator = XmlHandler.getTagValue(transformNode, "multivaluedseparator");
      dynamicSearch = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dynamicsearch"));
      dynamicSeachFieldName = XmlHandler.getTagValue(transformNode, "dynamicseachfieldname");
      dynamicFilter = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dynamicfilter"));
      dynamicFilterFieldName = XmlHandler.getTagValue(transformNode, "dynamicfilterfieldname");
      searchScope =
          getSearchScopeByCode(
              Const.NVL(
                  XmlHandler.getTagValue(transformNode, "searchScope"),
                  getSearchScopeCode(LdapConnection.SEARCH_SCOPE_SUBTREE_SCOPE)));

      protocol = XmlHandler.getTagValue(transformNode, "protocol");
      trustStorePath = XmlHandler.getTagValue(transformNode, "trustStorePath");
      trustStorePassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "trustStorePassword"));
      trustAllCertificates =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "trustAllCertificates"));
      useCertificate =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "useCertificate"));

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "LdapInputMeta.UnableToLoadFromXML"), e);
    }
  }

  private static int getSearchScopeByCode(String tt) {
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

    inputFields = new LdapInputField[nrFields];
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
    this.filterString = LdapConnection.DEFAUL_FILTER_STRING;
    this.searchBase = "";
    this.multiValuedSeparator = ";";
    this.dynamicSearch = false;
    this.dynamicSeachFieldName = null;
    this.dynamicFilter = false;
    this.dynamicFilterFieldName = null;
    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      this.inputFields[i] = new LdapInputField("field" + (i + 1));
    }

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
    for (i = 0; i < inputFields.length; i++) {
      LdapInputField field = inputFields[i];

      int type = field.getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v =
            ValueMetaFactory.createValueMeta(
                variables.resolve(field.getName()), type);
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
      String input[],
      String output[],
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;

    // Check output fields
    if (inputFields.length == 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.NoOutputFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.OutputFieldsOk"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we get input...
    if (input.length > 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // Check hostname
    if (Utils.isEmpty(Host)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.HostnameMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.HostnameOk"),
              transformMeta);
    }
    remarks.add(cr);

    if (isDynamicSearch()) {
      if (Utils.isEmpty(dynamicSeachFieldName)) {
        cr =
            new CheckResult(
                TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "LdapInputMeta.CheckResult.DynamicSearchBaseFieldNameMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
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
                TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.SearchBaseMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.SearchBaseOk"),
                transformMeta);
      }
      remarks.add(cr);
    }
    if (isDynamicFilter()) {
      if (Utils.isEmpty(dynamicFilterFieldName)) {
        cr =
            new CheckResult(
                TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "LdapInputMeta.CheckResult.DynamicFilterFieldNameMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.DynamicFilterFieldNameOk"),
                transformMeta);
      }
      remarks.add(cr);
    } else {
      // Check filter String
      if (Utils.isEmpty(filterString)) {
        cr =
            new CheckResult(
                TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.FilterStringMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LdapInputMeta.CheckResult.FilterStringOk"),
                transformMeta);
      }
      remarks.add(cr);
    }
  }

  @Override
  public LdapInput createTransform(
      TransformMeta transformMeta,
      LdapInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new LdapInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public LdapInputData getTransformData() {
    return new LdapInputData();
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
