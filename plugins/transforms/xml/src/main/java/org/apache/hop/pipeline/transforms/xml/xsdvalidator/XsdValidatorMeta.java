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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/*
 * Created on 14-08-2007
 *
 */
@Transform(
    id = "XSDValidator",
    image = "XOU.svg",
    name = "i18n::XSDValidator.name",
    description = "i18n::XSDValidator.description",
    categoryDescription = "i18n::XSDValidator.category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/xsdvalidator.html")
public class XsdValidatorMeta extends BaseTransformMeta
    implements ITransformMeta<XsdValidator, XsdValidatorData> {
  private static final Class<?> PKG = XsdValidatorMeta.class; // For Translator

  private String xsdFilename;
  private String xmlStream;
  private String resultFieldname;
  private boolean addValidationMessage;
  private String validationMessageField;
  private boolean outputStringField;
  private String ifXmlValid;
  private String ifXmlInvalid;
  private boolean xmlSourceFile;
  private String xsdDefinedField;

  private String xsdSource;

  private boolean allowExternalEntities;

  public String SPECIFY_FILENAME = "filename";
  public String SPECIFY_FIELDNAME = "fieldname";
  public String NO_NEED = "noneed";

  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION =
      "ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION";
  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT = "true";

  public boolean isAllowExternalEntities() {
    return allowExternalEntities;
  }

  public void setAllowExternalEntities(boolean allowExternalEntities) {
    this.allowExternalEntities = allowExternalEntities;
  }

  public void setXSDSource(String xsdsourcein) {
    this.xsdSource = xsdsourcein;
  }

  public String getXSDSource() {
    return xsdSource;
  }

  public void setXSDDefinedField(String xsddefinedfieldin) {
    this.xsdDefinedField = xsddefinedfieldin;
  }

  public String getXSDDefinedField() {
    return xsdDefinedField;
  }

  public boolean getXMLSourceFile() {
    return xmlSourceFile;
  }

  public void setXMLSourceFile(boolean xmlsourcefilein) {
    this.xmlSourceFile = xmlsourcefilein;
  }

  public String getIfXmlValid() {
    return ifXmlValid;
  }

  public String getIfXmlInvalid() {
    return ifXmlInvalid;
  }

  public void setIfXMLValid(String ifXmlValid) {
    this.ifXmlValid = ifXmlValid;
  }

  public void setIfXmlInvalid(String ifXmlInvalid) {
    this.ifXmlInvalid = ifXmlInvalid;
  }

  public boolean getOutputStringField() {
    return outputStringField;
  }

  public void setOutputStringField(boolean outputStringField) {
    this.outputStringField = outputStringField;
  }

  public String getValidationMessageField() {
    return validationMessageField;
  }

  public void setValidationMessageField(String validationMessageField) {
    this.validationMessageField = validationMessageField;
  }

  public boolean useAddValidationMessage() {
    return addValidationMessage;
  }

  public void setAddValidationMessage(boolean addValidationMessage) {
    this.addValidationMessage = addValidationMessage;
  }

  public XsdValidatorMeta() {
    super(); // allocate BaseTransformMeta
    allowExternalEntities =
        Boolean.valueOf(
            System.getProperties()
                .getProperty(
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION,
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT));
  }

  /** @return Returns the XSD filename. */
  public String getXSDFilename() {
    return xsdFilename;
  }

  public String getResultfieldname() {
    return resultFieldname;
  }

  public String getXMLStream() {
    return xmlStream;
  }

  /** @param xdsFilename The XSD filename to set. */
  public void setXSDfilename(String xdsFilename) {
    this.xsdFilename = xdsFilename;
  }

  public void setResultfieldname(String resultFieldname) {
    this.resultFieldname = resultFieldname;
  }

  public void setXMLStream(String xmlStream) {
    this.xmlStream = xmlStream;
  }

  public Object clone() {
    XsdValidatorMeta retval = (XsdValidatorMeta) super.clone();

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      XsdValidatorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new XsdValidator(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public XsdValidatorData getTransformData() {
    return new XsdValidatorData();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {

      xsdFilename = XmlHandler.getTagValue(transformNode, "xdsfilename");
      xmlStream = XmlHandler.getTagValue(transformNode, "xmlstream");
      resultFieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      xsdDefinedField = XmlHandler.getTagValue(transformNode, "xsddefinedfield");
      xsdSource = XmlHandler.getTagValue(transformNode, "xsdsource");

      addValidationMessage =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addvalidationmsg"));

      validationMessageField = XmlHandler.getTagValue(transformNode, "validationmsgfield");
      ifXmlValid = XmlHandler.getTagValue(transformNode, "ifxmlvalid");
      ifXmlInvalid = XmlHandler.getTagValue(transformNode, "ifxmlunvalid");
      outputStringField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "outputstringfield"));
      xmlSourceFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "xmlsourcefile"));
      allowExternalEntities =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "allowExternalEntities"));

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "XsdValidatorMeta.Exception.UnableToLoadTransformInfoFromXML"),
          e);
    }
  }

  public void setDefault() {
    xsdFilename = "";
    xmlStream = "";
    resultFieldname = "result";
    addValidationMessage = false;
    validationMessageField = "ValidationMsgField";
    ifXmlValid = "";
    ifXmlInvalid = "";
    outputStringField = false;
    xmlSourceFile = false;
    xsdDefinedField = "";
    xsdSource = SPECIFY_FILENAME;
    allowExternalEntities =
        Boolean.valueOf(
            System.getProperties()
                .getProperty(
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION,
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT));
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!Utils.isEmpty(resultFieldname)) {
      if (outputStringField) {
        // Output field (String)
        IValueMeta v = new ValueMetaString(variables.resolve(getResultfieldname()));
        inputRowMeta.addValueMeta(v);
      } else {

        // Output field (boolean)
        IValueMeta v = new ValueMetaBoolean(variables.resolve(getResultfieldname()));
        inputRowMeta.addValueMeta(v);
      }
    }
    // Add String Field that contain validation message (most the time, errors)
    if (addValidationMessage && !Utils.isEmpty(validationMessageField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(validationMessageField));
      inputRowMeta.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuffer xml = new StringBuffer();

    xml.append("    " + XmlHandler.addTagValue("xdsfilename", xsdFilename));
    xml.append("    " + XmlHandler.addTagValue("xmlstream", xmlStream));
    xml.append("    " + XmlHandler.addTagValue("resultfieldname", resultFieldname));
    xml.append("    " + XmlHandler.addTagValue("addvalidationmsg", addValidationMessage));
    xml.append("    " + XmlHandler.addTagValue("validationmsgfield", validationMessageField));
    xml.append("    " + XmlHandler.addTagValue("ifxmlunvalid", ifXmlInvalid));
    xml.append("    " + XmlHandler.addTagValue("ifxmlvalid", ifXmlValid));

    xml.append("    " + XmlHandler.addTagValue("outputstringfield", outputStringField));
    xml.append("    " + XmlHandler.addTagValue("xmlsourcefile", xmlSourceFile));
    xml.append("    " + XmlHandler.addTagValue("xsddefinedfield", xsdDefinedField));
    xml.append("    " + XmlHandler.addTagValue("xsdsource", xsdSource));
    xml.append("    " + XmlHandler.addTagValue("allowExternalEntities", allowExternalEntities));

    return xml.toString();
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // Check XML stream field
    if (Utils.isEmpty(xmlStream)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.XMLStreamFieldEmpty"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.XMLStreamFieldOK"),
              transforminfo);
      remarks.add(cr);
    }

    // Check result fieldname
    if (Utils.isEmpty(resultFieldname)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ResultFieldEmpty"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ResultFieldOK"),
              transforminfo);
      remarks.add(cr);
    }

    if (xsdSource.equals(SPECIFY_FILENAME)) {
      if (Utils.isEmpty(xsdFilename)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.XSDFieldEmpty"),
                transforminfo);
        remarks.add(cr);
      }
    }

    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "XsdValidatorMeta.CheckResult.ConnectedTransformOK",
                  String.valueOf(prev.size())),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.NoInputReceived"),
              transforminfo);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ExpectedInputOk"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ExpectedInputError"),
              transforminfo);
      remarks.add(cr);
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported transformation that runs this will reside in a ZIP file, we can't reference
   * files relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
   * @param variables the variable variables to use
   * @param definitions
   * @param resourceNamingInterface The repository to optionally load other resources from (to be
   *     converted to XML)
   * @param metadataProvider the metadataProvider in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //

      // From : ${Internal.Transformation.Filename.Directory}/../foo/bar.xsd
      // To : /home/matt/test/files/foo/bar.xsd
      //
      if (!Utils.isEmpty(xsdFilename)) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(xsdFilename));
        xsdFilename = resourceNamingInterface.nameResource(fileObject, variables, true);
        return xsdFilename;
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
