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

package org.apache.hop.pipeline.transforms.checksum;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
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

import java.util.List;

/*
 * Created on 30-06-2008
 *
 * @author Samatar Hassan
 */
@Transform(
    id = "CheckSum",
    image = "checksum.svg",
    name = "i18n::CheckSum.Name",
    description = "i18n::CheckSum.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/checksum.html")
@InjectionSupported(
    localizationPrefix = "CheckSum.Injection.",
    groups = {"FIELDS"})
public class CheckSumMeta extends BaseTransformMeta
    implements ITransformMeta<CheckSum, CheckSumData> {

  private static final Class<?> PKG = CheckSumMeta.class; // For Translator

  public static final String TYPE_CRC32 = "CRC32";
  public static final String TYPE_ADLER32 = "ADLER32";
  public static final String TYPE_MD5 = "MD5";
  public static final String TYPE_SHA1 = "SHA-1";
  public static final String TYPE_SHA256 = "SHA-256";
  public static final String TYPE_SHA384 = "SHA-384";
  public static final String TYPE_SHA512 = "SHA-512";

  public static String[] checksumtypeCodes = {
    TYPE_CRC32, TYPE_ADLER32, TYPE_MD5, TYPE_SHA1, TYPE_SHA256, TYPE_SHA384, TYPE_SHA512
  };
  public static String[] checksumtypeDescs = {
    BaseMessages.getString(PKG, "CheckSumMeta.Type.CRC32"),
    BaseMessages.getString(PKG, "CheckSumMeta.Type.ADLER32"),
    BaseMessages.getString(PKG, "CheckSumMeta.Type.MD5"),
    BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA1"),
    BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA256"),
    BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA384"),
    BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA512")
  };

  /** The result type description */
  private static final String[] resultTypeDesc = {
    BaseMessages.getString(PKG, "CheckSumMeta.ResultType.String"),
    BaseMessages.getString(PKG, "CheckSumMeta.ResultType.Hexadecimal"),
    BaseMessages.getString(PKG, "CheckSumMeta.ResultType.Binary")
  };

  /** The result type codes */
  public static final String[] resultTypeCode = {"string", "hexadecimal", "binary"};

  public static final int RESULT_TYPE_STRING = 0;
  public static final int RESULT_TYPE_HEXADECIMAL = 1;
  public static final int RESULT_TYPE_BINARY = 2;

  /** by which fields to display? */
  @Injection(name = "FIELD_NAME", group = "FIELDS")
  private String[] fieldName;

  @Injection(name = "RESULT_FIELD")
  private String resultfieldName;

  @Injection(name = "TYPE")
  private String checksumtype;

  /** result type */
  @Injection(name = "RESULT_TYPE")
  private int resultType;

  public CheckSumMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void setCheckSumType(int i) {
    checksumtype = checksumtypeCodes[i];
  }

  public int getTypeByDesc() {
    if (checksumtype == null) {
      return 0;
    }
    for (int i = 0; i < checksumtypeCodes.length; i++) {
      if (checksumtype.equals(checksumtypeCodes[i])) {
        return i;
      }
    }
    return 0;
  }

  public String getCheckSumType() {
    return checksumtype;
  }

  public String[] getChecksumtypeDescs() {
    return checksumtypeDescs;
  }

  public String[] getResultTypeDescs() {
    return resultTypeDesc;
  }

  public int getResultType() {
    return resultType;
  }

  public String getResultTypeDesc(int i) {
    if (i < 0 || i >= resultTypeDesc.length) {
      return resultTypeDesc[0];
    }
    return resultTypeDesc[i];
  }

  public int getResultTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < resultTypeDesc.length; i++) {
      if (resultTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getResultTypeByCode(tt);
  }

  private int getResultTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < resultTypeCode.length; i++) {
      if (resultTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public void setResultType(int resultType) {
    this.resultType = resultType;
  }

  /** @return Returns the resultfieldName. */
  public String getResultFieldName() {
    return resultfieldName;
  }

  /** @param resultfieldName The resultfieldName to set. */
  public void setResultFieldName(String resultfieldName) {
    this.resultfieldName = resultfieldName;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    CheckSumMeta retval = (CheckSumMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate(nrFields);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    return retval;
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields];
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      checksumtype = XmlHandler.getTagValue(transformNode, "checksumtype");
      resultfieldName = XmlHandler.getTagValue(transformNode, "resultfieldName");
      resultType =
          getResultTypeByCode(Const.NVL(XmlHandler.getTagValue(transformNode, "resultType"), ""));

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  private static String getResultTypeCode(int i) {
    if (i < 0 || i >= resultTypeCode.length) {
      return resultTypeCode[0];
    }
    return resultTypeCode[i];
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(200);
    retval.append("      ").append(XmlHandler.addTagValue("checksumtype", checksumtype));
    retval.append("      ").append(XmlHandler.addTagValue("resultfieldName", resultfieldName));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("resultType", getResultTypeCode(resultType)));

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", fieldName[i]));
      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

  @Override
  public void setDefault() {
    resultfieldName = null;
    checksumtype = checksumtypeCodes[0];
    resultType = RESULT_TYPE_HEXADECIMAL;
    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      fieldName[i] = "field" + i;
    }
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Output field (String)
    if (!Utils.isEmpty(resultfieldName)) {
      IValueMeta v = null;
      if (checksumtype.equals(TYPE_CRC32) || checksumtype.equals(TYPE_ADLER32)) {
        v = new ValueMetaInteger(variables.resolve(resultfieldName));
      } else {
        switch (resultType) {
          case RESULT_TYPE_BINARY:
            v = new ValueMetaBinary(variables.resolve(resultfieldName));
            break;
          default:
            v = new ValueMetaString(variables.resolve(resultfieldName));
            break;
        }
      }
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
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
    CheckResult cr;
    String errorMessage = "";

    if (Utils.isEmpty(resultfieldName)) {
      errorMessage = BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CheckSumMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      boolean errorFound = false;
      errorMessage = "";

      // Starting from selected fields in ...
      for (int i = 0; i < fieldName.length; i++) {
        int idx = prev.indexOfValue(fieldName[i]);
        if (idx < 0) {
          errorMessage += "\t\t" + fieldName[i] + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.FieldsFound", errorMessage);

        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (fieldName.length > 0) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.AllFieldsFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "CheckSumMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public CheckSum createTransform(
      TransformMeta transformMeta, CheckSumData data, int cnr, PipelineMeta tr, Pipeline pipeline) {
    return new CheckSum(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public CheckSumData getTransformData() {
    return new CheckSumData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
