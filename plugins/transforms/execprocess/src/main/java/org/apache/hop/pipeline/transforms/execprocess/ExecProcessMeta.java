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

package org.apache.hop.pipeline.transforms.execprocess;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
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
 * Created on 03-11-2008
 *
 */

@Transform(
    id = "ExecProcess",
    image = "execprocess.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ExecProcess",
    description = "i18n::BaseTransform.TypeTooltipDesc.ExecProcess",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/execprocess.html")
public class ExecProcessMeta extends BaseTransformMeta
    implements ITransformMeta<ExecProcess, ExecProcessData> {
  private static final Class<?> PKG = ExecProcessMeta.class; // For Translator

  /** dynamic process field name */
  private String processfield;

  /** function result: new value name */
  private String resultfieldname;

  /** function result: error fieldname */
  private String errorfieldname;

  /** function result: exit value fieldname */
  private String exitvaluefieldname;

  /** fail if the exit status is different from 0 */
  private boolean failwhennotsuccess;

  /** Output Line Delimiter - defaults to empty string for backward compatibility */
  public String outputLineDelimiter = "";

  /** Whether arguments for the command are provided in input fields */
  private boolean argumentsInFields;

  /** The field names where arguments should be found */
  private String[] argumentFieldNames;

  public ExecProcessMeta() {
    super(); // allocate BaseTransformMeta
    allocate(0);
  }

  public void allocate(int argumentCount) {
    this.argumentFieldNames = new String[argumentCount];
  }

  /** @return Returns the processfield. */
  public String getProcessField() {
    return processfield;
  }

  /** @param processfield The processfield to set. */
  public void setProcessField(String processfield) {
    this.processfield = processfield;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  /** @return Returns the errorfieldname. */
  public String getErrorFieldName() {
    return errorfieldname;
  }

  /** @param errorfieldname The errorfieldname to set. */
  public void setErrorFieldName(String errorfieldname) {
    this.errorfieldname = errorfieldname;
  }

  /** @return Returns the exitvaluefieldname. */
  public String getExitValueFieldName() {
    return exitvaluefieldname;
  }

  /** @param exitvaluefieldname The exitvaluefieldname to set. */
  public void setExitValueFieldName(String exitvaluefieldname) {
    this.exitvaluefieldname = exitvaluefieldname;
  }

  /** @return Returns the failwhennotsuccess. */
  public boolean isFailWhenNotSuccess() {
    return failwhennotsuccess;
  }

  /**
   * @param failwhennotsuccess The failwhennotsuccess to set.
   * @deprecated due to method name typo
   */
  @Deprecated
  public void setFailWhentNoSuccess(boolean failwhennotsuccess) {
    setFailWhenNotSuccess(failwhennotsuccess);
  }

  /** @param failwhennotsuccess The failwhennotsuccess to set. */
  public void setFailWhenNotSuccess(boolean failwhennotsuccess) {
    this.failwhennotsuccess = failwhennotsuccess;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  @Override
  public Object clone() {
    ExecProcessMeta retval = (ExecProcessMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    resultfieldname = "Result output";
    errorfieldname = "Error output";
    exitvaluefieldname = "Exit value";
    failwhennotsuccess = false;
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
    // Output fields (String)
    String realOutputFieldname = variables.resolve(resultfieldname);
    if (!Utils.isEmpty(realOutputFieldname)) {
      IValueMeta v = new ValueMetaString(realOutputFieldname);
      v.setLength(100, -1);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realerrofieldname = variables.resolve(errorfieldname);
    if (!Utils.isEmpty(realerrofieldname)) {
      IValueMeta v = new ValueMetaString(realerrofieldname);
      v.setLength(100, -1);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realexitvaluefieldname = variables.resolve(exitvaluefieldname);
    if (!Utils.isEmpty(realexitvaluefieldname)) {
      IValueMeta v = new ValueMetaInteger(realexitvaluefieldname);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("processfield", processfield));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    " + XmlHandler.addTagValue("errorfieldname", errorfieldname));
    retval.append("    " + XmlHandler.addTagValue("exitvaluefieldname", exitvaluefieldname));
    retval.append("    " + XmlHandler.addTagValue("failwhennotsuccess", failwhennotsuccess));
    retval.append("    " + XmlHandler.addTagValue("outputlinedelimiter", outputLineDelimiter));
    retval.append("    ").append(XmlHandler.addTagValue("argumentsInFields", argumentsInFields));

    retval.append("    ").append(XmlHandler.openTag("argumentFields")).append(Const.CR);
    for (int i = 0; i < argumentFieldNames.length; i++) {
      retval.append("      ").append(XmlHandler.openTag("argumentField")).append(Const.CR);
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("argumentFieldName", argumentFieldNames[i]));
      retval.append("      ").append(XmlHandler.closeTag("argumentField")).append(Const.CR);
    }
    retval.append("    ").append(XmlHandler.closeTag("argumentFields")).append(Const.CR);
    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      processfield = XmlHandler.getTagValue(transformNode, "processfield");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      errorfieldname = XmlHandler.getTagValue(transformNode, "errorfieldname");
      exitvaluefieldname = XmlHandler.getTagValue(transformNode, "exitvaluefieldname");
      failwhennotsuccess =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "failwhennotsuccess"));
      outputLineDelimiter = XmlHandler.getTagValue(transformNode, "outputlinedelimiter");
      if (outputLineDelimiter == null) {
        outputLineDelimiter = ""; // default to empty string for backward compatibility
      }

      argumentsInFields =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "argumentsInFields"));
      Node argumentFieldsNode = XmlHandler.getSubNode(transformNode, "argumentFields");
      if (argumentFieldsNode == null) {
        argumentFieldNames = new String[0];
      } else {
        int argumentFieldCount = XmlHandler.countNodes(argumentFieldsNode, "argumentField");
        argumentFieldNames = new String[argumentFieldCount];
        for (int i = 0; i < argumentFieldCount; i++) {
          Node fnode = XmlHandler.getSubNodeByNr(argumentFieldsNode, "argumentField", i);
          argumentFieldNames[i] = XmlHandler.getTagValue(fnode, "argumentFieldName");
        }
      }

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ExecProcessMeta.Exception.UnableToReadTransformMeta"), e);
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

    if (Utils.isEmpty(resultfieldname)) {
      errorMessage = BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    if (Utils.isEmpty(processfield)) {
      errorMessage = BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ProcessFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ProcessFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ExecProcessMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public ExecProcess createTransform(
      TransformMeta transformMeta,
      ExecProcessData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExecProcess(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ExecProcessData getTransformData() {
    return new ExecProcessData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return failwhennotsuccess;
  }

  public void setOutputLineDelimiter(String value) {
    this.outputLineDelimiter = value;
  }

  public String getOutputLineDelimiter() {
    return outputLineDelimiter;
  }

  public boolean isArgumentsInFields() {
    return argumentsInFields;
  }

  public void setArgumentsInFields(boolean argumentsInFields) {
    this.argumentsInFields = argumentsInFields;
  }

  public String[] getArgumentFieldNames() {
    return argumentFieldNames;
  }

  public void setArgumentFieldNames(String[] argumentFieldNames) {
    this.argumentFieldNames = argumentFieldNames;
  }
}
