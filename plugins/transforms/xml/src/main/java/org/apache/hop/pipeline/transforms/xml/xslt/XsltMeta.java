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

package org.apache.hop.pipeline.transforms.xml.xslt;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.io.File;
import java.util.List;

/*
 * Created on 15-Oct-2007
 *
 */
@Transform(
    id = "XSLT",
    image = "XSLT.svg",
    name = "i18n::XSLT.name",
    description = "i18n::XSLT.description",
    categoryDescription = "i18n::XSLT.category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/xslt.html")
public class XsltMeta extends BaseTransformMeta implements ITransformMeta<Xslt, XsltData> {
  private static final Class<?> PKG = XsltMeta.class; // For Translator

  public static final String[] outputProperties =
      new String[] {
        "method",
        "version",
        "encoding",
        "standalone",
        "indent",
        "omit-xml-declaration",
        "doctype-public",
        "doctype-system",
        "media-type"
      };

  private String xslFilename;
  private String fieldName;
  private String resultFieldname;
  private String xslFileField;
  private boolean xslFileFieldUse;
  private boolean xslFieldIsAFile;
  private String xslFactory;

  /** output property name */
  private String[] outputPropertyName;

  /** output property value */
  private String[] outputPropertyValue;

  /** parameter name */
  private String[] parameterName;

  /** parameter field */
  private String[] parameterField;

  public XsltMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the parameterName. */
  public String[] getParameterName() {
    return parameterName;
  }

  /** @param argumentDirection The parameterName to set. */
  public void setParameterName(String[] argumentDirection) {
    this.parameterName = argumentDirection;
  }

  /** @return Returns the parameterField. */
  public String[] getParameterField() {
    return parameterField;
  }

  /** @param argumentDirection The parameterField to set. */
  public void setParameterField(String[] argumentDirection) {
    this.parameterField = argumentDirection;
  }

  /** @return Returns the XSL filename. */
  public String getXslFilename() {
    return xslFilename;
  }

  /** @return Returns the OutputPropertyName. */
  public String[] getOutputPropertyName() {
    return outputPropertyName;
  }

  /** @param argumentDirection The OutputPropertyName to set. */
  public void setOutputPropertyName(String[] argumentDirection) {
    this.outputPropertyName = argumentDirection;
  }

  /** @return Returns the OutputPropertyField. */
  public String[] getOutputPropertyValue() {
    return outputPropertyValue;
  }

  /** @param argumentDirection The outputPropertyValue to set. */
  public void setOutputPropertyValue(String[] argumentDirection) {
    this.outputPropertyValue = argumentDirection;
  }

  public void setXSLFileField(String xslfilefieldin) {
    xslFileField = xslfilefieldin;
  }

  public void setXSLFactory(String xslfactoryin) {
    xslFactory = xslfactoryin;
  }

  /** @return Returns the XSL factory type. */
  public String getXSLFactory() {
    return xslFactory;
  }

  public String getXSLFileField() {
    return xslFileField;
  }

  public String getResultfieldname() {
    return resultFieldname;
  }

  public String getFieldname() {
    return fieldName;
  }

  /** @param xslFilename The Xsl filename to set. */
  public void setXslFilename(String xslFilename) {
    this.xslFilename = xslFilename;
  }

  public void setResultfieldname(String resultfield) {
    this.resultFieldname = resultfield;
  }

  public void setFieldname(String fieldnamein) {
    this.fieldName = fieldnamein;
  }

  public void allocate(int nrParameters, int outputProps) {
    parameterName = new String[nrParameters];
    parameterField = new String[nrParameters];

    outputPropertyName = new String[outputProps];
    outputPropertyValue = new String[outputProps];
  }

  public Object clone() {
    XsltMeta retval = (XsltMeta) super.clone();
    int nrparams = parameterName.length;
    int nroutputprops = outputPropertyName.length;
    retval.allocate(nrparams, nroutputprops);

    for (int i = 0; i < nrparams; i++) {
      retval.parameterName[i] = parameterName[i];
      retval.parameterField[i] = parameterField[i];
    }
    for (int i = 0; i < nroutputprops; i++) {
      retval.outputPropertyName[i] = outputPropertyName[i];
      retval.outputPropertyValue[i] = outputPropertyValue[i];
    }

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      XsltData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Xslt(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public XsltData getTransformData() {
    return new XsltData();
  }

  public boolean useXSLField() {
    return xslFileFieldUse;
  }

  public void setXSLField(boolean value) {
    this.xslFileFieldUse = value;
  }

  public void setXSLFieldIsAFile(boolean xslFieldisAFile) {
    this.xslFieldIsAFile = xslFieldisAFile;
  }

  public boolean isXSLFieldIsAFile() {
    return xslFieldIsAFile;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      xslFilename = XmlHandler.getTagValue(transformNode, "xslfilename");
      fieldName = XmlHandler.getTagValue(transformNode, "fieldname");
      resultFieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      xslFileField = XmlHandler.getTagValue(transformNode, "xslfilefield");
      xslFileFieldUse =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "xslfilefielduse"));
      String isAFile = XmlHandler.getTagValue(transformNode, "xslfieldisafile");
      if (xslFileFieldUse && Utils.isEmpty(isAFile)) {
        xslFieldIsAFile = true;
      } else {
        xslFieldIsAFile = "Y".equalsIgnoreCase(isAFile);
      }

      xslFactory = XmlHandler.getTagValue(transformNode, "xslfactory");

      Node parametersNode = XmlHandler.getSubNode(transformNode, "parameters");
      int nrparams = XmlHandler.countNodes(parametersNode, "parameter");

      Node parametersOutputProps = XmlHandler.getSubNode(transformNode, "outputproperties");
      int nroutputprops = XmlHandler.countNodes(parametersOutputProps, "outputproperty");
      allocate(nrparams, nroutputprops);

      for (int i = 0; i < nrparams; i++) {
        Node anode = XmlHandler.getSubNodeByNr(parametersNode, "parameter", i);
        parameterField[i] = XmlHandler.getTagValue(anode, "field");
        parameterName[i] = XmlHandler.getTagValue(anode, "name");
      }
      for (int i = 0; i < nroutputprops; i++) {
        Node anode = XmlHandler.getSubNodeByNr(parametersOutputProps, "outputproperty", i);
        outputPropertyName[i] = XmlHandler.getTagValue(anode, "name");
        outputPropertyValue[i] = XmlHandler.getTagValue(anode, "value");
      }

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "XsltMeta.Exception.UnableToLoadTransformInfoFromXML"), e);
    }
  }

  public void setDefault() {
    xslFilename = null;
    fieldName = null;
    resultFieldname = "result";
    xslFactory = "JAXP";
    xslFileField = null;
    xslFileFieldUse = false;
    xslFieldIsAFile = true;

    int nrparams = 0;
    int nroutputproperties = 0;
    allocate(nrparams, nroutputproperties);

    for (int i = 0; i < nrparams; i++) {
      parameterField[i] = "param" + i;
      parameterName[i] = "param";
    }
    for (int i = 0; i < nroutputproperties; i++) {
      outputPropertyName[i] = "outputprop" + i;
      outputPropertyValue[i] = "outputprop";
    }
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Output field (String)
    IValueMeta v = new ValueMetaString(variables.resolve(getResultfieldname()));
    v.setOrigin(name);
    inputRowMeta.addValueMeta(v);
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer();

    retval.append("    " + XmlHandler.addTagValue("xslfilename", xslFilename));
    retval.append("    " + XmlHandler.addTagValue("fieldname", fieldName));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultFieldname));
    retval.append("    " + XmlHandler.addTagValue("xslfilefield", xslFileField));
    retval.append("    " + XmlHandler.addTagValue("xslfilefielduse", xslFileFieldUse));
    retval.append("    " + XmlHandler.addTagValue("xslfieldisafile", xslFieldIsAFile));

    retval.append("    " + XmlHandler.addTagValue("xslfactory", xslFactory));
    retval.append("    <parameters>").append(Const.CR);

    for (int i = 0; i < parameterName.length; i++) {
      retval.append("      <parameter>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("field", parameterField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("name", parameterName[i]));
      retval.append("      </parameter>").append(Const.CR);
    }

    retval.append("    </parameters>").append(Const.CR);
    retval.append("    <outputproperties>").append(Const.CR);

    for (int i = 0; i < outputPropertyName.length; i++) {
      retval.append("      <outputproperty>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", outputPropertyName[i]));
      retval.append("        ").append(XmlHandler.addTagValue("value", outputPropertyValue[i]));
      retval.append("      </outputproperty>").append(Const.CR);
    }

    retval.append("    </outputproperties>").append(Const.CR);
    return retval.toString();
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

    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "XsltMeta.CheckResult.ConnectedTransformOK", String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsltMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsltMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsltMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    // Check if The result field is given
    if (getResultfieldname() == null) {
      // Result Field is missing !
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorResultFieldNameMissing"),
              transformMeta);
      remarks.add(cr);
    }

    // Check if XSL Filename field is provided
    if (xslFileFieldUse) {
      if (getXSLFileField() == null) {
        // Result Field is missing !
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorResultXSLFieldNameMissing"),
                transformMeta);
        remarks.add(cr);
      } else {
        // Result Field is provided !
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorResultXSLFieldNameOK"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      if (xslFilename == null) {
        // Result Field is missing !
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorXSLFileNameMissing"),
                transformMeta);
        remarks.add(cr);

      } else {
        // Check if it's exist and it's a file
        String RealFilename = variables.resolve(xslFilename);
        File f = new File(RealFilename);

        if (f.exists()) {
          if (f.isFile()) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "XsltMeta.CheckResult.FileExists", RealFilename),
                    transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "XsltMeta.CheckResult.ExistsButNoFile", RealFilename),
                    transformMeta);
            remarks.add(cr);
          }
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "XsltMeta.CheckResult.FileNotExists", RealFilename),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
