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

package org.apache.hop.pipeline.transforms.salesforceinput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnectionUtils;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;

@Transform(
    id = "SalesforceInput",
    name = "i18n::SalesforceInput.TypeLongDesc.SalesforceInput",
    description = "i18n::SalesforceInput.TypeTooltipDesc.SalesforceInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    image = "SFI.svg",
    keywords = "i18n::SalesforceInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/salesforceinput.html")
@Getter
@Setter
public class SalesforceInputMeta
    extends SalesforceTransformMeta<SalesforceInput, SalesforceInputData> {
  public static final String CONST_FIELDS = "fields";
  public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static final Class<?> PKG = SalesforceInputMeta.class;

  /** Flag indicating that we should include the generated SQL in the output */
  @HopMetadataProperty(key = "include_sql", injectionKey = "INCLUDE_SQL_IN_OUTPUT")
  //  @Injection(name = "INCLUDE_SQL_IN_OUTPUT")
  private boolean includeSQL;

  /** The name of the field in the output containing the generated SQL */
  @HopMetadataProperty(key = "sql_field", injectionKey = "SQL_FIELDNAME")
  //  @Injection(name = "SQL_FIELDNAME")
  private String sqlField;

  /** Flag indicating that we should include the server Timestamp in the output */
  @HopMetadataProperty(key = "include_Timestamp", injectionKey = "INCLUDE_TIMESTAMP_IN_OUTPUT")
  //  @Injection(name = "INCLUDE_TIMESTAMP_IN_OUTPUT")
  private boolean includeTimestamp;

  /** The name of the field in the output containing the server Timestamp */
  @HopMetadataProperty(key = "timstamp_field", injectionKey = "TIMESTAMP_FIELDNAME")
  //  @Injection(name = "TIMESTAMP_FIELDNAME")
  private String timestampField;

  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(key = "include_targeturl", injectionKey = "INCLUDE_URL_IN_OUTPUT")
  //  @Injection(name = "INCLUDE_URL_IN_OUTPUT")
  private boolean includeTargetURL;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(key = "targeturl_field", injectionKey = "URL_FIELDNAME")
  //  @Injection(name = "URL_FIELDNAME")
  private String targetURLField;

  /** Flag indicating that we should include the module in the output */
  @HopMetadataProperty(key = "include_module", injectionKey = "INCLUDE_MODULE_IN_OUTPUT")
  //  @Injection(name = "INCLUDE_MODULE_IN_OUTPUT")
  private boolean includeModule;

  /** The name of the field in the output containing the module */
  @HopMetadataProperty(key = "module_field", injectionKey = "MODULE_FIELDNAME")
  //  @Injection(name = "MODULE_FIELDNAME")
  private String moduleField;

  /** Flag indicating that a deletion date field should be included in the output */
  @HopMetadataProperty(
      key = "include_deletion_date",
      injectionKey = "INCLUDE_DELETION_DATE_IN_OUTPUT")
  //  @Injection(name = "INCLUDE_DELETION_DATE_IN_OUTPUT")
  private boolean includeDeletionDate;

  /** The name of the field in the output containing the deletion Date */
  @HopMetadataProperty(key = "deletion_date_field", injectionKey = "DELETION_DATE_FIELDNAME")
  //  @Injection(name = "DELETION_DATE_FIELDNAME")
  private String deletionDateField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key = "include_rownum", injectionKey = "INCLUDE_ROWNUM_IN_OUTPUT")
  //  @Injection(name = "INCLUDE_ROWNUM_IN_OUTPUT")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key = "rownum_field", injectionKey = "ROWNUM_FIELDNAME")
  //  @Injection(name = "ROWNUM_FIELDNAME")
  private String rowNumberField;

  /** The condition */
  @HopMetadataProperty(key = "condition", injectionKey = "QUERY_CONDITION")
  //  @Injection(name = "QUERY_CONDITION")
  private String condition;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "limit", injectionKey = "LIMIT")
  // @Injection(name = "LIMIT")
  private String rowLimit;

  /** The fields to return... */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS")
  //  @InjectionDeep private SalesforceInputField[] inputFields;
  private List<SalesforceInputField> fields;

  /** option: specify query */
  @HopMetadataProperty(key = "specifyQuery", injectionKey = "USE_SPECIFIED_QUERY")
  //  @Injection(name = "USE_SPECIFIED_QUERY")
  private boolean specifyQuery;

  // ** query entered by user **/
  @HopMetadataProperty(key = "query", injectionKey = "SPECIFY_QUERY")
  //  @Injection(name = "SPECIFY_QUERY")
  private String query;

  private int nrFields;

  @HopMetadataProperty(key = "read_to", injectionKey = "END_DATE")
  //  @Injection(name = "END_DATE")
  private String readTo;

  @HopMetadataProperty(key = "read_from", injectionKey = "START_DATE")
  //  @Injection(name = "START_DATE")
  private String readFrom;

  /** records filter */
  @HopMetadataProperty(key = "records_filter")
  private int recordsFilter;

  /** Query all records including deleted ones */
  @HopMetadataProperty(key = "queryAll", injectionKey = "QUERY_ALL")
  //  @Injection(name = "QUERY_ALL")
  private boolean queryAll;

  /** Authentication type: USERNAME_PASSWORD or OAUTH */
  //  @HopMetadataProperty(injectionKey = "AUTHENTICATION_TYPE")
  //  //  @Injection(name = "AUTHENTICATION_TYPE")
  //  private String authenticationType = "USERNAME_PASSWORD"; // Default for backward compatibility

  /** OAuth Client ID */
  //  @HopMetadataProperty(injectionKey = )
  //  @Injection(name = "OAUTH_CLIENT_ID")
  //  private String oauthClientId;

  /** OAuth Client Secret */
  //  @Injection(name = "OAUTH_CLIENT_SECRET")
  //  private String oauthClientSecret;

  /** OAuth Redirect URI */
  //  @Injection(name = "OAUTH_REDIRECT_URI")
  //  private String oauthRedirectUri;

  /** OAuth Access Token */
  //  @Injection(name = "OAUTH_ACCESS_TOKEN")
  //  private String oauthAccessToken;

  /** OAuth Refresh Token */
  //  @Injection(name = "OAUTH_REFRESH_TOKEN")
  //  private String oauthRefreshToken;

  /** OAuth Instance URL */
  //  @Injection(name = "OAUTH_INSTANCE_URL")
  //  private String oauthInstanceUrl;

  public SalesforceInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the input fields.
   */
  //  public SalesforceInputField[] getInputFields() {
  //    return inputFields;
  //  }

  /**
   * @param inputFields The input fields to set.
   */
  //  public void setInputFields(SalesforceInputField[] inputFields) {
  //    this.inputFields = inputFields;
  //  }

  /**
   * @return Returns the query.
   */
  //  public String getQuery() {
  //    return query;
  //  }

  /**
   * @param query The query to set.
   */
  //  public void setQuery(String query) {
  //    this.query = query;
  //  }

  /**
   * @return Returns the specifyQuery.
   */
  //  public boolean isSpecifyQuery() {
  //    return specifyQuery;
  //  }

  /**
   * @param specifyQuery The specifyQuery to set.
   */
  //  public void setSpecifyQuery(boolean specifyQuery) {
  //    this.specifyQuery = specifyQuery;
  //  }

  /**
   * @return Returns the queryAll.
   */
  //  public boolean isQueryAll() {
  //    return queryAll;
  //  }

  /**
   * @param queryAll The queryAll to set.
   */
  //  public void setQueryAll(boolean queryAll) {
  //    this.queryAll = queryAll;
  //  }

  /**
   * @return Returns the authentication type.
   */
  //  public String getAuthenticationType() {
  //    return authenticationType;
  //  }

  /**
   * @param authenticationType The authentication type to set.
   */
  //  public void setAuthenticationType(String authenticationType) {
  //    this.authenticationType = authenticationType;
  //  }

  /**
   * @return Returns true if OAuth authentication is selected.
   */
  //  public boolean isOAuthAuthentication() {
  //    return "OAUTH".equalsIgnoreCase(authenticationType);
  //  }

  /**
   * @return Returns true if username/password authentication is selected.
   */
  //  public boolean isUsernamePasswordAuthentication() {
  //    return "USERNAME_PASSWORD".equalsIgnoreCase(authenticationType);
  //  }

  /**
   * @return Returns the OAuth Client ID.
   */
  //  public String getOauthClientId() {
  //    return oauthClientId;
  //  }

  /**
   * @param oauthClientId The OAuth Client ID to set.
   */
  //  public void setOauthClientId(String oauthClientId) {
  //    this.oauthClientId = oauthClientId;
  //  }

  /**
   * @return Returns the OAuth Client Secret.
   */
  //  public String getOauthClientSecret() {
  //    return oauthClientSecret;
  //  }

  /**
   * @param oauthClientSecret The OAuth Client Secret to set.
   */
  //  public void setOauthClientSecret(String oauthClientSecret) {
  //    this.oauthClientSecret = oauthClientSecret;
  //  }

  /**
   * @return Returns the OAuth Redirect URI.
   */
  //  public String getOauthRedirectUri() {
  //    return oauthRedirectUri;
  //  }

  /**
   * @param oauthRedirectUri The OAuth Redirect URI to set.
   */
  //  public void setOauthRedirectUri(String oauthRedirectUri) {
  //    this.oauthRedirectUri = oauthRedirectUri;
  //  }

  /**
   * @return Returns the OAuth Access Token.
   */
  //  public String getOauthAccessToken() {
  //    return oauthAccessToken;
  //  }

  /**
   * @param oauthAccessToken The OAuth Access Token to set.
   */
  //  public void setOauthAccessToken(String oauthAccessToken) {
  //    this.oauthAccessToken = oauthAccessToken;
  //  }

  /**
   * @return Returns the OAuth Refresh Token.
   */
  //  public String getOauthRefreshToken() {
  //    return oauthRefreshToken;
  //  }

  /**
   * @param oauthRefreshToken The OAuth Refresh Token to set.
   */
  //  public void setOauthRefreshToken(String oauthRefreshToken) {
  //    this.oauthRefreshToken = oauthRefreshToken;
  //  }

  /**
   * @return Returns the OAuth Instance URL.
   */
  //  public String getOauthInstanceUrl() {
  //    return oauthInstanceUrl;
  //  }

  /**
   * @param oauthInstanceUrl The OAuth Instance URL to set.
   */
  //  public void setOauthInstanceUrl(String oauthInstanceUrl) {
  //    this.oauthInstanceUrl = oauthInstanceUrl;
  //  }

  /**
   * @return Returns the condition.
   */
  //  public String getCondition() {
  //    return condition;
  //  }

  /**
   * @param condition The condition to set.
   */
  //  public void setCondition(String condition) {
  //    this.condition = condition;
  //  }

  /**
   * @param targetURLField The targetURLField to set.
   */
  //  public void setTargetURLField(String targetURLField) {
  //    this.targetURLField = targetURLField;
  //  }

  /**
   * @param sqlField The sqlField to set.
   */
  //  public void setSQLField(String sqlField) {
  //    this.sqlField = sqlField;
  //  }

  /**
   * @param timestampField The timestampField to set.
   */
  //  public void setTimestampField(String timestampField) {
  //    this.timestampField = timestampField;
  //  }

  /**
   * @param moduleField The moduleField to set.
   */
  //  public void setModuleField(String moduleField) {
  //    this.moduleField = moduleField;
  //  }

  //  public int getRecordsFilter() {
  //    return recordsFilter;
  //  }

  //  public void setRecordsFilter(int recordsFilter) {
  //    this.recordsFilter = recordsFilter;
  //  }

  @Injection(name = "RETRIEVE")
  public void setRecordsFilterDesc(String recordsFilterDesc) {
    this.recordsFilter = SalesforceConnectionUtils.getRecordsFilterByDesc(recordsFilterDesc);
  }

  /**
   * @return Returns the includeTargetURL.
   */
  public boolean includeTargetURL() {
    return includeTargetURL;
  }

  /**
   * @return Returns the includeSQL.
   */
  //  public boolean includeSQL() {
  //    return includeSQL;
  //  }

  /**
   * @param includeSQL to set.
   */
  //  public void setIncludeSQL(boolean includeSQL) {
  //    this.includeSQL = includeSQL;
  //  }

  /**
   * @return Returns the includeTimestamp.
   */
  public boolean includeTimestamp() {
    return includeTimestamp;
  }

  /**
   * @param includeTimestamp to set.
   */
  //  public void setIncludeTimestamp(boolean includeTimestamp) {
  //    this.includeTimestamp = includeTimestamp;
  //  }

  /**
   * @return Returns the includeModule.
   */
  public boolean includeModule() {
    return includeModule;
  }

  /**
   * @param includeTargetURL The includeTargetURL to set.
   */
  //  public void setIncludeTargetURL(boolean includeTargetURL) {
  //    this.includeTargetURL = includeTargetURL;
  //  }

  /**
   * @param includeModule The includeModule to set.
   */
  //  public void setIncludeModule(boolean includeModule) {
  //    this.includeModule = includeModule;
  //  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /**
   * @param includeRowNumber The includeRowNumber to set.
   */
  //  public void setIncludeRowNumber(boolean includeRowNumber) {
  //    this.includeRowNumber = includeRowNumber;
  //  }

  /**
   * @return Returns the includeDeletionDate.
   */
  public boolean includeDeletionDate() {
    return includeDeletionDate;
  }

  /**
   * @param includeDeletionDate The includeDeletionDate to set.
   */
  //  public void setIncludeDeletionDate(boolean includeDeletionDate) {
  //    this.includeDeletionDate = includeDeletionDate;
  //  }

  /**
   * @return Returns the rowLimit.
   */
  //  public String getRowLimit() {
  //    return rowLimit;
  //  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  //  public void setRowLimit(String rowLimit) {
  //    this.rowLimit = rowLimit;
  //  }

  /**
   * @return Returns the rowNumberField.
   */
  //  public String getRowNumberField() {
  //    return rowNumberField;
  //  }

  /**
   * @return Returns the deletionDateField.
   */
  //  public String getDeletionDateField() {
  //    return deletionDateField;
  //  }

  /**
   * @param value the deletionDateField to set.
   */
  //  public void setDeletionDateField(String value) {
  //    this.deletionDateField = value;
  //  }

  /**
   * @return Returns the targetURLField.
   */
  //  public String getTargetURLField() {
  //    return targetURLField;
  //  }

  /**
   * @return Returns the readFrom.
   */
  //  public String getReadFrom() {
  //    return readFrom;
  //  }

  /**
   * @param readFrom the readFrom to set.
   */
  //  public void setReadFrom(String readFrom) {
  //    this.readFrom = readFrom;
  //  }

  /**
   * @return Returns the readTo.
   */
  //  public String getReadTo() {
  //    return readTo;
  //  }

  /**
   * @param readTo the readTo to set.
   */
  //  public void setReadTo(String readTo) {
  //    this.readTo = readTo;
  //  }

  /**
   * @return Returns the sqlField.
   */
  //  public String getSqlField() {
  //    return sqlField;
  //  }

  /**
   * @return Returns the timestampField.
   */
  //  public String getTimestampField() {
  //    return timestampField;
  //  }

  /**
   * @return Returns the moduleField.
   */
  //  public String getModuleField() {
  //    return moduleField;
  //  }

  /**
   * @param rowNumberField The rowNumberField to set.
   */
  //  public void setRowNumberField(String rowNumberField) {
  //    this.rowNumberField = rowNumberField;
  //  }

  //  @Override
  //  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
  //      throws HopXmlException {
  //    super.loadXml(transformNode, metadataProvider);
  //    readData(transformNode);
  //  }

  @Override
  public Object clone() {
    SalesforceInputMeta retval = (SalesforceInputMeta) super.clone();
    retval.fields = new ArrayList<SalesforceInputField>();

    int nrFields = fields.size();

    //    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      if (fields.get(i) != null) {
        //        retval.inputFields.get(i) = (SalesforceInputField) inputFields.get(i).clone();
        retval.fields.add((SalesforceInputField) fields.get(i).clone());
      }
    }

    return retval;
  }

  //  @Override
  //  public String getXml() {
  //    StringBuilder retval = new StringBuilder(super.getXml());
  //    retval.append("    ").append(XmlHandler.addTagValue("condition", getCondition()));
  //    retval.append("    ").append(XmlHandler.addTagValue("specifyQuery", isSpecifyQuery()));
  //    retval.append("    ").append(XmlHandler.addTagValue("query", getQuery()));
  //    retval.append("    ").append(XmlHandler.addTagValue("include_targeturl",
  // includeTargetURL()));
  //    retval.append("    ").append(XmlHandler.addTagValue("targeturl_field",
  // getTargetURLField()));
  //    retval.append("    ").append(XmlHandler.addTagValue("include_module", includeModule()));
  //    retval.append("    ").append(XmlHandler.addTagValue("module_field", getModuleField()));
  //    retval.append("    ").append(XmlHandler.addTagValue("include_rownum", includeRowNumber()));
  //    retval
  //        .append("    ")
  //        .append(XmlHandler.addTagValue("include_deletion_date", includeDeletionDate()));
  //
  //    retval
  //        .append("    ")
  //        .append(XmlHandler.addTagValue("deletion_date_field", getDeletionDateField()));
  //    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", getRowNumberField()));
  //    retval.append("    ").append(XmlHandler.addTagValue("include_sql", includeSQL()));
  //    retval.append("    ").append(XmlHandler.addTagValue("sql_field", getSqlField()));
  //    retval.append("    ").append(XmlHandler.addTagValue("include_Timestamp",
  // includeTimestamp()));
  //    retval.append("    ").append(XmlHandler.addTagValue("timestamp_field",
  // getTimestampField()));
  //    retval.append("    ").append(XmlHandler.addTagValue("read_from", getReadFrom()));
  //    retval.append("    ").append(XmlHandler.addTagValue("read_to", getReadTo()));
  //    retval
  //        .append("    ")
  //        .append(
  //            XmlHandler.addTagValue(
  //                "records_filter",
  //                SalesforceConnectionUtils.getRecordsFilterCode(getRecordsFilter())));
  //    retval.append("    ").append(XmlHandler.addTagValue("queryAll", isQueryAll()));
  //        retval
  //            .append("    ")
  //            .append(XmlHandler.addTagValue("authentication_type", getAuthenticationType()));
  //        retval.append("    ").append(XmlHandler.addTagValue("oauth_client_id",
  //     getOauthClientId()));
  //        retval
  //            .append("    ")
  //            .append(
  //                XmlHandler.addTagValue(
  //                    "oauth_client_secret",
  //                    Encr.encryptPasswordIfNotUsingVariables(getOauthClientSecret())));
  //        retval
  //            .append("    ")
  //            .append(XmlHandler.addTagValue("oauth_redirect_uri", getOauthRedirectUri()));
  //        retval
  //            .append("    ")
  //            .append(
  //                XmlHandler.addTagValue(
  //                    "oauth_access_token",
  //                    Encr.encryptPasswordIfNotUsingVariables(getOauthAccessToken())));
  //        retval
  //            .append("    ")
  //            .append(
  //                XmlHandler.addTagValue(
  //                    "oauth_refresh_token",
  //                    Encr.encryptPasswordIfNotUsingVariables(getOauthRefreshToken())));
  //        retval
  //            .append("    ")
  //            .append(XmlHandler.addTagValue("oauth_instance_url", getOauthInstanceUrl()));
  //
  //    retval.append("    ").append(XmlHandler.openTag(CONST_FIELDS)).append(Const.CR);
  //    for (SalesforceInputField field : inputFields) {
  //      retval.append(field.getXml());
  //    }
  //    retval.append("    ").append(XmlHandler.closeTag(CONST_FIELDS)).append(Const.CR);
  //    retval.append("    ").append(XmlHandler.addTagValue("limit", getRowLimit()));
  //
  //    return retval.toString();
  //  }
  //
  //  private void readData(Node transformNode) throws HopXmlException {
  //    try {
  //      setCondition(XmlHandler.getTagValue(transformNode, "condition"));
  //      setQuery(XmlHandler.getTagValue(transformNode, "query"));
  //      setSpecifyQuery("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode,
  // "specifyQuery")));
  //      setIncludeTargetURL(
  //          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_targeturl")));
  //      setTargetURLField(XmlHandler.getTagValue(transformNode, "targeturl_field"));
  //      setIncludeModule(
  //          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_module")));
  //      setModuleField(XmlHandler.getTagValue(transformNode, "module_field"));
  //      setIncludeRowNumber(
  //          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_rownum")));
  //      setIncludeDeletionDate(
  //          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_deletion_date")));
  //      setRowNumberField(XmlHandler.getTagValue(transformNode, "rownum_field"));
  //      setDeletionDateField(XmlHandler.getTagValue(transformNode, "deletion_date_field"));
  //
  //      setIncludeSQL("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_sql")));
  //      setSqlField(XmlHandler.getTagValue(transformNode, "sql_field"));
  //      setIncludeTimestamp(
  //          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_Timestamp")));
  //      setTimestampField(XmlHandler.getTagValue(transformNode, "timestamp_field"));
  //      setReadFrom(XmlHandler.getTagValue(transformNode, "read_from"));
  //      setReadTo(XmlHandler.getTagValue(transformNode, "read_to"));
  //      setRecordsFilter(
  //          SalesforceConnectionUtils.getRecordsFilterByCode(
  //              Const.NVL(XmlHandler.getTagValue(transformNode, "records_filter"), "")));
  //      setQueryAll("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "queryAll")));
  //
  //       Load OAuth fields with backward compatibility
  //            String authType = XmlHandler.getTagValue(transformNode, "authentication_type");
  //            setAuthenticationType(Utils.isEmpty(authType) ? "USERNAME_PASSWORD" : authType);
  //            setOauthClientId(XmlHandler.getTagValue(transformNode, "oauth_client_id"));
  //            setOauthClientSecret(
  //                Encr.decryptPasswordOptionallyEncrypted(
  //                    XmlHandler.getTagValue(transformNode, "oauth_client_secret")));
  //            setOauthRedirectUri(XmlHandler.getTagValue(transformNode, "oauth_redirect_uri"));
  //            setOauthAccessToken(
  //                Encr.decryptPasswordOptionallyEncrypted(
  //                    XmlHandler.getTagValue(transformNode, "oauth_access_token")));
  //            setOauthRefreshToken(
  //                Encr.decryptPasswordOptionallyEncrypted(
  //                    XmlHandler.getTagValue(transformNode, "oauth_refresh_token")));
  //            setOauthInstanceUrl(XmlHandler.getTagValue(transformNode, "oauth_instance_url"));
  //
  //      Node fields = XmlHandler.getSubNode(transformNode, CONST_FIELDS);
  //      int nrFields = XmlHandler.countNodes(fields, "field");
  //
  //      allocate(nrFields);
  //
  //      for (int i = 0; i < nrFields; i++) {
  //        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
  //        SalesforceInputField field = new SalesforceInputField(fnode);
  //        inputFields[i] = field;
  //      }
  //      // Is there a limit on the number of rows we process?
  //      setRowLimit(XmlHandler.getTagValue(transformNode, "limit"));
  //    } catch (Exception e) {
  //      throw new HopXmlException("Unable to load transform info from XML", e);
  //    }
  //  }

  //  public void allocate(int nrFields) {
  //    setInputFields(new SalesforceInputField[nrFields]);
  //  }

  //  public int getNrFields() {
  //    return nrFields;
  //  }

  @Override
  public void setDefault() {
    super.setDefault();
    setFields(new ArrayList<>());
    setIncludeDeletionDate(false);
    setQueryAll(false);
    setReadFrom("");
    setReadTo("");
    nrFields = 0;
    setSpecifyQuery(false);
    setQuery("");
    setCondition("");
    setIncludeTargetURL(false);
    setTargetURLField("");
    setIncludeModule(false);
    setModuleField("");
    setIncludeRowNumber(false);
    setRowNumberField("");
    setDeletionDateField("");
    setIncludeSQL(false);
    setSqlField("");
    setIncludeTimestamp(false);
    setTimestampField("");
    //    allocate(0);

    setRowLimit("0");

    // OAuth defaults
    //    setAuthenticationType("USERNAME_PASSWORD"); // Default for backward compatibility
    //    setOauthClientId("");
    //    setOauthClientSecret("");
    //    setOauthRedirectUri("http://localhost:8080/callback");
    //    setOauthAccessToken("");
    //    setOauthRefreshToken("");
    //    setOauthInstanceUrl("");
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
    for (i = 0; i < fields.size(); i++) {
      SalesforceInputField field = fields.get(i);

      int type = field.getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(variables.resolve(field.getName()), type);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setOrigin(name);
        v.setConversionMask(field.getFormat());
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupSymbol());
        v.setCurrencySymbol(field.getCurrencySymbol());
        r.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }

    if (includeTargetURL && !Utils.isEmpty(targetURLField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(targetURLField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeModule && !Utils.isEmpty(moduleField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(moduleField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeSQL && !Utils.isEmpty(sqlField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(sqlField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeTimestamp && !Utils.isEmpty(timestampField)) {
      IValueMeta v = new ValueMetaDate(variables.resolve(timestampField));
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeRowNumber && !Utils.isEmpty(rowNumberField)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeDeletionDate && !Utils.isEmpty(deletionDateField)) {
      IValueMeta v = new ValueMetaDate(variables.resolve(deletionDateField));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
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
    super.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);
    CheckResult cr;

    // See if we get input...
    if (input != null && input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // check return fields
    if (getFields().size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.FieldsOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check additional fields
    if (includeTargetURL() && Utils.isEmpty(getTargetURLField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoTargetURLField"),
              transformMeta);
      remarks.add(cr);
    }
    if (isIncludeSQL() && Utils.isEmpty(getSqlField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoSQLField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeModule() && Utils.isEmpty(moduleField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoModuleField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeTimestamp() && Utils.isEmpty(getTimestampField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoTimestampField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeRowNumber() && Utils.isEmpty(getRowNumberField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoRowNumberField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeDeletionDate() && Utils.isEmpty(getDeletionDateField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoDeletionDateField"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public SalesforceInputDialog getDialog(
      org.eclipse.swt.widgets.Shell shell,
      IVariables variables,
      org.apache.hop.pipeline.transform.ITransformMeta transformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    return new SalesforceInputDialog(
        shell, variables, (SalesforceInputMeta) transformMeta, pipelineMeta);
  }
}
