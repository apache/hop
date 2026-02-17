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
package org.apache.hop.pipeline.transforms.ldapoutput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.ldapinput.ILdapMeta;
import org.apache.hop.pipeline.transforms.ldapinput.LdapProtocolFactory;

@Getter
@Setter
@Transform(
    id = "LDAPOutput",
    name = "i18n::LdapOutput.Name",
    description = "i18n::LdapOutput.Description",
    image = "ldapoutput.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::LdapOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/ldapoutput.html")
public class LdapOutputMeta extends BaseTransformMeta<LdapOutput, LdapOutputData>
    implements ILdapMeta {
  private static final Class<?> PKG = LdapOutputMeta.class;
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

  /** The name of DN field */
  @HopMetadataProperty(key = "dnFieldName")
  private String dnFieldName;

  /**
   * -- GETTER --
   *
   * <p>-- SETTER --
   *
   * @return Returns the failIfNotExist.
   * @param failIfNotExist The failIfNotExist to set.
   */
  @HopMetadataProperty(key = "failIfNotExist")
  private boolean failIfNotExist;

  /** Fields array - replaces String[] updateLookup, updateStream, Boolean[] update */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<LdapOutputField> fields;

  /** For metadata serialization: get the fields list. */
  public List<LdapOutputField> getFields() {
    return fields != null ? fields : new ArrayList<>();
  }

  /** For metadata serialization: set the fields list. */
  public void setFields(List<LdapOutputField> fields) {
    this.fields = fields != null ? new ArrayList<>(fields) : new ArrayList<>();
  }

  /**
   * Operations type -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param searchBase The searchBase filed.
   * @return Returns the searchBase.
   */
  @HopMetadataProperty(key = "searchBase")
  private String searchBase;

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

  @HopMetadataProperty(
      key = "operationType",
      intCodeConverter = LdapOutputOperationTypeConverter.class)
  private int operationType;

  /**
   * -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param value The oldDnFieldName filed.
   * @return Returns the oldDnFieldName.
   */
  @HopMetadataProperty(key = "oldDnFieldName")
  private String oldDnFieldName;

  /**
   * -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param value The newDnFieldName filed.
   * @return Returns the newDnFieldName.
   */
  @HopMetadataProperty(key = "newDnFieldName")
  private String newDnFieldName;

  /**
   * -- SETTER --
   *
   * <p>-- GETTER --
   *
   * @param value The deleteRDN filed.
   * @return Returns the deleteRDN.
   */
  @HopMetadataProperty(key = "deleteRDN")
  private boolean deleteRDN;

  /** The operations description */
  static final String[] operationTypeDesc = {
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.Insert"),
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.Upsert"),
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.Update"),
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.Add"),
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.Delete"),
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.Rename"),
    BaseMessages.getString(PKG, "LdapOutputMeta.operationType.RemoveAttribute")
  };

  public static final String CONST_UPDATE = "update";

  /** The operations type codes */
  static final String[] operationTypeCode = {
    "insert", "upsert", CONST_UPDATE, "add", "delete", "rename", "remove_attribute"
  };

  public static final int OPERATION_TYPE_INSERT = 0;

  public static final int OPERATION_TYPE_UPSERT = 1;

  public static final int OPERATION_TYPE_UPDATE = 2;

  public static final int OPERATION_TYPE_ADD = 3;

  public static final int OPERATION_TYPE_DELETE = 4;

  public static final int OPERATION_TYPE_RENAME = 5;

  public static final int OPERATION_TYPE_REMOVE_ATTRIBUTE = 6;

  @HopMetadataProperty(
      key = "referralType",
      intCodeConverter = LdapOutputReferralTypeConverter.class)
  private int referralType;

  /** The referrals description */
  public static final String[] referralTypeDesc = {
    BaseMessages.getString(PKG, "LdapOutputMeta.referralType.Follow"),
    BaseMessages.getString(PKG, "LdapOutputMeta.referralType.Ignore")
  };

  /** The referrals type codes */
  static final String[] referralTypeCode = {"follow", "ignore"};

  public static final int REFERRAL_TYPE_FOLLOW = 0;

  public static final int REFERRAL_TYPE_IGNORE = 1;

  @HopMetadataProperty(
      key = "derefAliasesType",
      intCodeConverter = LdapOutputDerefAliasesTypeConverter.class)
  private int derefAliasesType;

  /** The derefAliasess description */
  static final String[] derefAliasesTypeDesc = {
    BaseMessages.getString(PKG, "LdapOutputMeta.derefAliasesType.Always"),
    BaseMessages.getString(PKG, "LdapOutputMeta.derefAliasesType.Never"),
    BaseMessages.getString(PKG, "LdapOutputMeta.derefAliasesType.Searching"),
    BaseMessages.getString(PKG, "LdapOutputMeta.derefAliasesType.Finding")
  };

  /** The derefAliasess type codes */
  static final String[] derefAliasesTypeCode = {"always", "never", "searching", "finding"};

  public static final int DEREFALIASES_TYPE_ALWAYS = 0;

  public static final int DEREFALIASES_TYPE_NEVER = 1;

  public static final int DEREFALIASES_TYPE_SEARCHING = 2;

  public static final int DEREFALIASES_TYPE_FINDING = 3;

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

  public LdapOutputMeta() {
    super(); // allocate BaseTransformMeta
    this.fields = new ArrayList<>();
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

  public Boolean[] getUpdate() {
    if (fields == null) return new Boolean[0];
    return fields.stream().map(LdapOutputField::isUpdate).toArray(Boolean[]::new);
  }

  public void setUpdate(Boolean[] update) {
    if (fields == null) {
      allocate(update != null ? update.length : 0);
    }
    for (int i = 0; i < update.length; i++) {
      if (i < fields.size()) {
        fields.get(i).setUpdate(update[i]);
      } else {
        fields.add(new LdapOutputField(null, null, update[i]));
      }
    }
  }

  public static int getOperationTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < operationTypeDesc.length; i++) {
      if (operationTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getOperationTypeByCode(tt);
  }

  public static int getReferralTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < referralTypeDesc.length; i++) {
      if (referralTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getReferralTypeByCode(tt);
  }

  public static int getDerefAliasesTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < derefAliasesTypeDesc.length; i++) {
      if (derefAliasesTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getReferralTypeByCode(tt);
  }

  public static int getOperationTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < operationTypeCode.length; i++) {
      if (operationTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getReferralTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < referralTypeCode.length; i++) {
      if (referralTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getDerefAliasesTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < derefAliasesTypeCode.length; i++) {
      if (derefAliasesTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static String getOperationTypeDesc(int i) {
    if (i < 0 || i >= operationTypeDesc.length) {
      return operationTypeDesc[0];
    }
    return operationTypeDesc[i];
  }

  public static String getReferralTypeDesc(int i) {
    if (i < 0 || i >= referralTypeDesc.length) {
      return referralTypeDesc[0];
    }
    return referralTypeDesc[i];
  }

  public static String getDerefAliasesTypeDesc(int i) {
    if (i < 0 || i >= derefAliasesTypeDesc.length) {
      return derefAliasesTypeDesc[0];
    }
    return derefAliasesTypeDesc[i];
  }

  /**
   * @return Returns the updateStream.
   */
  public String[] getUpdateStream() {
    if (fields == null) return new String[0];
    return fields.stream().map(LdapOutputField::getUpdateStream).toArray(String[]::new);
  }

  /**
   * @param updateStream The updateStream to set.
   */
  public void setUpdateStream(String[] updateStream) {
    if (fields == null) {
      allocate(updateStream != null ? updateStream.length : 0);
    }
    for (int i = 0; i < updateStream.length; i++) {
      if (i < fields.size()) {
        fields.get(i).setUpdateStream(updateStream[i]);
      } else {
        fields.add(new LdapOutputField(null, updateStream[i], null));
      }
    }
  }

  /**
   * @return Returns the updateLookup.
   */
  public String[] getUpdateLookup() {
    if (fields == null) return new String[0];
    return fields.stream().map(LdapOutputField::getUpdateLookup).toArray(String[]::new);
  }

  /**
   * @param updateLookup The updateLookup to set.
   */
  public void setUpdateLookup(String[] updateLookup) {
    if (fields == null) {
      allocate(updateLookup != null ? updateLookup.length : 0);
    }
    for (int i = 0; i < updateLookup.length; i++) {
      if (i < fields.size()) {
        fields.get(i).setUpdateLookup(updateLookup[i]);
      } else {
        LdapOutputField field = new LdapOutputField();
        field.setUpdateLookup(updateLookup[i]);
        // Default: the same name!
        if (field.getUpdateStream() == null) {
          field.setUpdateStream(updateLookup[i]);
        }
        fields.add(field);
      }
    }
  }

  /**
   * @return Returns the host name.
   */
  @Override
  public String getHost() {
    return host;
  }

  public void setDnField(String value) {
    this.dnFieldName = value;
  }

  public String getDnField() {
    return this.dnFieldName;
  }

  /**
   * Setter for metadata serialization - matches field name dnFieldName
   *
   * @param value The DN field name to set
   */
  public void setDnFieldName(String value) {
    this.dnFieldName = value;
  }

  /**
   * Getter for metadata serialization - matches field name dnFieldName
   *
   * @return The DN field name
   */
  public String getDnFieldName() {
    return this.dnFieldName;
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
    LdapOutputMeta retval = (LdapOutputMeta) super.clone();
    if (fields != null) {
      retval.fields = new ArrayList<>();
      for (LdapOutputField field : fields) {
        retval.fields.add(
            new LdapOutputField(
                field.getUpdateLookup(), field.getUpdateStream(), field.isUpdate()));
      }
    } else {
      retval.fields = new ArrayList<>();
    }
    return retval;
  }

  public void allocate(int nrvalues) {
    fields = new ArrayList<>(nrvalues);
    for (int i = 0; i < nrvalues; i++) {
      fields.add(new LdapOutputField());
    }
  }

  public static String getOperationTypeCode(int i) {
    if (i < 0 || i >= operationTypeCode.length) {
      return operationTypeCode[0];
    }
    return operationTypeCode[i];
  }

  public static String getReferralTypeCode(int i) {
    if (i < 0 || i >= referralTypeCode.length) {
      return referralTypeCode[0];
    }
    return referralTypeCode[i];
  }

  public static String getDerefAliasesCode(int i) {
    if (i < 0 || i >= derefAliasesTypeCode.length) {
      return derefAliasesTypeCode[0];
    }
    return derefAliasesTypeCode[i];
  }

  @Override
  public void setDefault() {
    useAuthentication = false;
    host = "";
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

    fields = new ArrayList<>();
    operationType = OPERATION_TYPE_INSERT;
    referralType = REFERRAL_TYPE_FOLLOW;
    derefAliasesType = DEREFALIASES_TYPE_ALWAYS;
    this.trustStorePath = null;
    this.trustStorePassword = null;
    this.trustAllCertificates = false;
    this.protocol = LdapProtocolFactory.getConnectionTypes(log).get(0);
    this.useCertificate = false;
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

    // See if we get input...
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapOutputMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapOutputMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // Check hostname
    if (Utils.isEmpty(host)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapOutputMeta.CheckResult.HostnameMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapOutputMeta.CheckResult.HostnameOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check return fields
    if (fields == null || fields.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "LdapOutputUpdateMeta.CheckResult.NoFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "LdapOutputUpdateMeta.CheckResult.FieldsOk"),
              transformMeta);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public String toString() {
    return "LdapConnection " + getName();
  }

  @Override
  public String getDerefAliases() {
    return LdapOutputMeta.getDerefAliasesCode(getDerefAliasesType());
  }

  @Override
  public String getReferrals() {
    return getReferralTypeCode(getReferralType());
  }
}
