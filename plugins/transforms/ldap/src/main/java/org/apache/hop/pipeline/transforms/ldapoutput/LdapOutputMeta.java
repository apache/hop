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

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
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
import org.apache.hop.pipeline.transforms.ldapinput.ILdapMeta;
import org.apache.hop.pipeline.transforms.ldapinput.LdapProtocolFactory;
import org.w3c.dom.Node;

@Transform(
    id = "LDAPOutput",
    name = "i18n::LdapOutput.Name",
    description = "i18n::LdapOutput.Description",
    image = "ldapoutput.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = {"ldap", "output"},
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/ldapoutput.html")
public class LdapOutputMeta extends BaseTransformMeta
    implements ILdapMeta, ITransformMeta<LdapOutput, LdapOutputData> {
  private static Class<?> PKG = LdapOutputMeta.class; // For Translator

  /** Flag indicating that we use authentication for connection */
  private boolean useAuthentication;

  /** The Host name */
  private String host;

  /** The User name */
  private String userName;

  /** The Password to use in LDAP authentication */
  private String password;

  /** The Port */
  private String port;

  /** The name of DN field */
  private String dnFieldName;

  private boolean failIfNotExist;

  /** Field value to update */
  private String[] updateLookup;

  /** Stream name to update value with */
  private String[] updateStream;

  /** boolean indicating if field needs to be updated */
  private Boolean[] update;

  /** Operations type */
  private String searchBase;

  /** Multi valued separator */
  private String multiValuedSeparator;

  private int operationType;

  private String oldDnFieldName;
  private String newDnFieldName;
  private boolean deleteRDN;

  /** The operations description */
  static final String[] operationTypeDesc = {
    BaseMessages.getString( PKG, "LdapOutputMeta.operationType.Insert"),
    BaseMessages.getString( PKG, "LdapOutputMeta.operationType.Upsert"),
    BaseMessages.getString( PKG, "LdapOutputMeta.operationType.Update"),
    BaseMessages.getString( PKG, "LdapOutputMeta.operationType.Add"),
    BaseMessages.getString( PKG, "LdapOutputMeta.operationType.Delete"),
    BaseMessages.getString( PKG, "LdapOutputMeta.operationType.Rename")
  };

  /** The operations type codes */
  static final String[] operationTypeCode = {
    "insert", "upsert", "update", "add", "delete", "rename"
  };

  public static final int OPERATION_TYPE_INSERT = 0;

  public static final int OPERATION_TYPE_UPSERT = 1;

  public static final int OPERATION_TYPE_UPDATE = 2;

  public static final int OPERATION_TYPE_ADD = 3;

  public static final int OPERATION_TYPE_DELETE = 4;

  public static final int OPERATION_TYPE_RENAME = 5;

  private int referralType;

  /** The referrals description */
  public static final String[] referralTypeDesc = {
    BaseMessages.getString( PKG, "LdapOutputMeta.referralType.Follow"),
    BaseMessages.getString( PKG, "LdapOutputMeta.referralType.Ignore")
  };

  /** The referrals type codes */
  static final String[] referralTypeCode = {"follow", "ignore"};

  public static final int REFERRAL_TYPE_FOLLOW = 0;

  public static final int REFERRAL_TYPE_IGNORE = 1;

  private int derefAliasesType;

  /** The derefAliasess description */
  static final String[] derefAliasesTypeDesc = {
    BaseMessages.getString( PKG, "LdapOutputMeta.derefAliasesType.Always"),
    BaseMessages.getString( PKG, "LdapOutputMeta.derefAliasesType.Never"),
    BaseMessages.getString( PKG, "LdapOutputMeta.derefAliasesType.Searching"),
    BaseMessages.getString( PKG, "LdapOutputMeta.derefAliasesType.Finding")
  };

  /** The derefAliasess type codes */
  static final String[] derefAliasesTypeCode = {"always", "never", "searching", "finding"};

  public static final int DEREFALIASES_TYPE_ALWAYS = 0;

  public static final int DEREFALIASES_TYPE_NEVER = 1;

  public static final int DEREFALIASES_TYPE_SEARCHING = 2;

  public static final int DEREFALIASES_TYPE_FINDING = 3;

  /** Protocol */
  private String protocol;

  /** Trust store */
  private boolean useCertificate;

  private String trustStorePath;
  private String trustStorePassword;
  private boolean trustAllCertificates;

  public LdapOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the input useCertificate. */
  public boolean isUseCertificate() {
    return useCertificate;
  }

  /** @return Returns the useCertificate. */
  public void setUseCertificate(boolean value) {
    this.useCertificate = value;
  }

  /** @return Returns the input trustAllCertificates. */
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  /** @return Returns the input trustAllCertificates. */
  public void setTrustAllCertificates(boolean value) {
    this.trustAllCertificates = value;
  }

  /** @return Returns the trustStorePath. */
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /** @param value the trustStorePassword to set. */
  public void setTrustStorePassword(String value) {
    this.trustStorePassword = value;
  }

  /** @return Returns the trustStorePath. */
  public String getTrustStorePath() {
    return trustStorePath;
  }

  /** @param value the trustStorePath to set. */
  public void setTrustStorePath(String value) {
    this.trustStorePath = value;
  }

  /** @return Returns the protocol. */
  public String getProtocol() {
    return protocol;
  }

  /** @param value the protocol to set. */
  public void setProtocol(String value) {
    this.protocol = value;
  }

  public Boolean[] getUpdate() {
    return update;
  }

  public void setUpdate(Boolean[] update) {
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

  private static int getOperationTypeByCode(String tt) {
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

  private static int getReferralTypeByCode(String tt) {
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

  private static int getDerefAliasesTypeByCode(String tt) {
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

  public void setOperationType(int operationType) {
    this.operationType = operationType;
  }

  public void setReferralType(int value) {
    this.referralType = value;
  }

  public void setDerefAliasesType(int value) {
    this.derefAliasesType = value;
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

  /** @return Returns the updateStream. */
  public String[] getUpdateStream() {
    return updateStream;
  }

  /** @param updateStream The updateStream to set. */
  public void setUpdateStream(String[] updateStream) {
    this.updateStream = updateStream;
  }

  /** @return Returns the updateLookup. */
  public String[] getUpdateLookup() {
    return updateLookup;
  }

  /** @param updateLookup The updateLookup to set. */
  public void setUpdateLookup(String[] updateLookup) {
    this.updateLookup = updateLookup;
  }

  /** @return Returns the input useAuthentication. */
  public boolean isUseAuthentication() {
    return useAuthentication;
  }

  /** @param useAuthentication The useAuthentication to set. */
  public void setUseAuthentication(boolean useAuthentication) {
    this.useAuthentication = useAuthentication;
  }

  /** @return Returns the host name. */
  public String getHost() {
    return host;
  }

  /** @param host The host to set. */
  public void setHost(String host) {
    this.host = host;
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

  public void setDnField(String value) {
    this.dnFieldName = value;
  }

  public String getDnField() {
    return this.dnFieldName;
  }

  /** @return Returns the Port. */
  public String getPort() {
    return port;
  }

  /** @param port The port to set. */
  public void setPort(String port) {
    this.port = port;
  }

  /** @return Returns the failIfNotExist. */
  public boolean isFailIfNotExist() {
    return failIfNotExist;
  }

  /** @param failIfNotExist The failIfNotExist to set. */
  public void setFailIfNotExist(boolean failIfNotExist) {
    this.failIfNotExist = failIfNotExist;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    LdapOutputMeta retval = (LdapOutputMeta) super.clone();
    int nrvalues = updateLookup.length;

    retval.allocate(nrvalues);
    System.arraycopy(updateLookup, 0, retval.updateLookup, 0, nrvalues);
    System.arraycopy(updateStream, 0, retval.updateStream, 0, nrvalues);
    System.arraycopy(update, 0, retval.update, 0, nrvalues);

    return retval;
  }

  /** @param value The deleteRDN filed. */
  public void setDeleteRDN(boolean value) {
    this.deleteRDN = value;
  }

  /** @return Returns the deleteRDN. */
  public boolean isDeleteRDN() {
    return deleteRDN;
  }

  /** @param value The newDnFieldName filed. */
  public void setNewDnFieldName(String value) {
    this.newDnFieldName = value;
  }

  /** @return Returns the newDnFieldName. */
  public String getNewDnFieldName() {
    return newDnFieldName;
  }

  /** @param value The oldDnFieldName filed. */
  public void setOldDnFieldName(String value) {
    this.oldDnFieldName = value;
  }

  /** @return Returns the oldDnFieldName. */
  public String getOldDnFieldName() {
    return oldDnFieldName;
  }

  /** @param searchBase The searchBase filed. */
  public void setSearchBaseDN(String searchBase) {
    this.searchBase = searchBase;
  }

  /** @return Returns the searchBase. */
  public String getSearchBaseDN() {
    return searchBase;
  }

  /** @param multiValuedSeparator The multi-valued separator filed. */
  public void setMultiValuedSeparator(String multiValuedSeparator) {
    this.multiValuedSeparator = multiValuedSeparator;
  }

  /** @return Returns the multi valued separator. */
  public String getMultiValuedSeparator() {
    return multiValuedSeparator;
  }

  public void allocate(int nrvalues) {
    updateLookup = new String[nrvalues];
    updateStream = new String[nrvalues];
    update = new Boolean[nrvalues];
  }

  private static String getOperationTypeCode(int i) {
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

  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval.append("    ").append(XmlHandler.addTagValue("useauthentication", useAuthentication));
    retval.append("    ").append(XmlHandler.addTagValue("host", host));
    retval.append("    ").append(XmlHandler.addTagValue("username", userName));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    retval.append("    ").append(XmlHandler.addTagValue("port", port));
    retval.append("    ").append(XmlHandler.addTagValue("dnFieldName", dnFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("failIfNotExist", failIfNotExist));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("operationType", getOperationTypeCode(operationType)));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("multivaluedseparator", multiValuedSeparator));
    retval.append("    ").append(XmlHandler.addTagValue("searchBase", searchBase));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("referralType", getReferralTypeCode(referralType)));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("derefAliasesType", getDerefAliasesCode(derefAliasesType)));

    retval.append("    ").append(XmlHandler.addTagValue("oldDnFieldName", oldDnFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("newDnFieldName", newDnFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("deleteRDN", deleteRDN));

    retval.append("    <fields>" + Const.CR);

    for (int i = 0; i < updateLookup.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", updateLookup[i]));
      retval.append("        ").append(XmlHandler.addTagValue("field", updateStream[i]));
      retval.append("        ").append(XmlHandler.addTagValue("update", update[i].booleanValue()));
      retval.append("      </field>").append(Const.CR);
    }

    retval.append("      </fields>" + Const.CR);
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

  private void readData(Node transformNode) throws HopXmlException {
    try {

      useAuthentication =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "useauthentication"));
      host = XmlHandler.getTagValue(transformNode, "host");
      userName = XmlHandler.getTagValue(transformNode, "username");
      setPassword(
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "password")));

      port = XmlHandler.getTagValue(transformNode, "port");
      dnFieldName = XmlHandler.getTagValue(transformNode, "dnFieldName");
      failIfNotExist =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "failIfNotExist"));
      operationType =
          getOperationTypeByCode(
              Const.NVL(XmlHandler.getTagValue(transformNode, "operationType"), ""));
      multiValuedSeparator = XmlHandler.getTagValue(transformNode, "multivaluedseparator");
      searchBase = XmlHandler.getTagValue(transformNode, "searchBase");
      referralType =
          getReferralTypeByCode(
              Const.NVL(XmlHandler.getTagValue(transformNode, "referralType"), ""));
      derefAliasesType =
          getDerefAliasesTypeByCode(
              Const.NVL(XmlHandler.getTagValue(transformNode, "derefAliasesType"), ""));

      oldDnFieldName = XmlHandler.getTagValue(transformNode, "oldDnFieldName");
      newDnFieldName = XmlHandler.getTagValue(transformNode, "newDnFieldName");
      deleteRDN = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "deleteRDN"));

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        updateLookup[i] = XmlHandler.getTagValue(fnode, "name");
        updateStream[i] = XmlHandler.getTagValue(fnode, "field");
        if (updateStream[i] == null) {
          updateStream[i] = updateLookup[i]; // default: the same name!
        }
        String updateValue = XmlHandler.getTagValue(fnode, "update");
        if (updateValue == null) {
          // default TRUE
          update[i] = Boolean.TRUE;
        } else {
          if (updateValue.equalsIgnoreCase("Y")) {
            update[i] = Boolean.TRUE;
          } else {
            update[i] = Boolean.FALSE;
          }
        }
      }

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
          BaseMessages.getString( PKG, "LdapOutputMeta.UnableToLoadFromXML"),
          e);
    }
  }

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

    int nrFields = 0;
    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      updateLookup[i] = "name" + (i + 1);
      updateStream[i] = "field" + (i + 1);
      update[i] = Boolean.TRUE;
    }
    operationType = OPERATION_TYPE_INSERT;
    referralType = REFERRAL_TYPE_FOLLOW;
    derefAliasesType = DEREFALIASES_TYPE_ALWAYS;
    this.trustStorePath = null;
    this.trustStorePassword = null;
    this.trustAllCertificates = false;
    this.protocol = LdapProtocolFactory.getConnectionTypes(log).get(0);
    this.useCertificate = false;
  }

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
              BaseMessages.getString(
                PKG, "LdapOutputMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                PKG, "LdapOutputMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // Check hostname
    if (Utils.isEmpty(host)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                PKG, "LdapOutputMeta.CheckResult.HostnameMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                PKG, "LdapOutputMeta.CheckResult.HostnameOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check return fields
    if (updateLookup.length == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                PKG, "LdapOutputUpdateMeta.CheckResult.NoFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                PKG, "LdapOutputUpdateMeta.CheckResult.FieldsOk"),
              transformMeta);
    }
  }

  public LdapOutput createTransform(
      TransformMeta transformMeta,
      LdapOutputData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new LdapOutput(transformMeta, this, data, cnr, tr, pipeline);
  }

  public LdapOutputData getTransformData() {
    return new LdapOutputData();
  }

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
