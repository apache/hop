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

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "XSDValidator",
    image = "XOU.svg",
    name = "i18n::XSDValidator.name",
    description = "i18n::XSDValidator.description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Validation",
    keywords = "i18n::XsdValidatorMeta.keyword",
    documentationUrl = "/pipeline/transforms/xsdvalidator.html")
@Getter
@Setter
public class XsdValidatorMeta extends BaseTransformMeta<XsdValidator, XsdValidatorData> {
  private static final Class<?> PKG = XsdValidatorMeta.class;

  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION =
      "ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION";
  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT = "true";

  public static final String SPECIFY_FILENAME = "filename";
  public static final String SPECIFY_FIELDNAME = "fieldname";
  public static final String NO_NEED = "noneed";

  @HopMetadataProperty(key = "xdsfilename")
  private String xsdFilename;

  @HopMetadataProperty(key = "xmlstream")
  private String xmlStream;

  @HopMetadataProperty(key = "resultfieldname")
  private String resultFieldName;

  @HopMetadataProperty(key = "addvalidationmsg")
  private boolean addValidationMessage;

  @HopMetadataProperty(key = "validationmsgfield")
  private String validationMessageField;

  @HopMetadataProperty(key = "outputstringfield")
  private boolean outputStringField;

  @HopMetadataProperty(key = "ifxmlvalid")
  private String ifXmlValid;

  @HopMetadataProperty(key = "ifxmlunvalid")
  private String ifXmlInvalid;

  @HopMetadataProperty(key = "xmlsourcefile")
  private boolean xmlSourceFile;

  @HopMetadataProperty(key = "xsddefinedfield")
  private String xsdDefinedField;

  @HopMetadataProperty(key = "xsdsource")
  private String xsdSource;

  @HopMetadataProperty(key = "allowExternalEntities")
  private boolean allowExternalEntities;

  public XsdValidatorMeta() {
    super();
    xsdFilename = "";
    xmlStream = "";
    resultFieldName = "result";
    validationMessageField = "ValidationMsgField";
    ifXmlValid = "";
    ifXmlInvalid = "";
    xsdDefinedField = "";
    xsdSource = SPECIFY_FILENAME;
    allowExternalEntities =
        Boolean.parseBoolean(
            System.getProperties()
                .getProperty(
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION,
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT));
  }

  public XsdValidatorMeta(XsdValidatorMeta m) {
    this();
    this.addValidationMessage = m.addValidationMessage;
    this.allowExternalEntities = m.allowExternalEntities;
    this.ifXmlInvalid = m.ifXmlInvalid;
    this.ifXmlValid = m.ifXmlValid;
    this.outputStringField = m.outputStringField;
    this.resultFieldName = m.resultFieldName;
    this.validationMessageField = m.validationMessageField;
    this.xmlSourceFile = m.xmlSourceFile;
    this.xmlStream = m.xmlStream;
    this.xsdDefinedField = m.xsdDefinedField;
    this.xsdFilename = m.xsdFilename;
    this.xsdSource = m.xsdSource;
  }

  @Override
  public Object clone() {
    return new XsdValidatorMeta(this);
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
    if (!Utils.isEmpty(resultFieldName)) {
      if (outputStringField) {
        // Output field (String)
        IValueMeta v = new ValueMetaString(variables.resolve(getResultFieldName()));
        inputRowMeta.addValueMeta(v);
      } else {

        // Output field (boolean)
        IValueMeta v = new ValueMetaBoolean(variables.resolve(getResultFieldName()));
        inputRowMeta.addValueMeta(v);
      }
    }
    // Add String Field that contain validation message (most the time, errors)
    if (addValidationMessage && !Utils.isEmpty(validationMessageField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(validationMessageField));
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

    // Check XML stream field
    if (Utils.isEmpty(xmlStream)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.XMLStreamFieldEmpty"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.XMLStreamFieldOK"),
              transformMeta);
      remarks.add(cr);
    }

    // Check result fieldname
    if (Utils.isEmpty(resultFieldName)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ResultFieldEmpty"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ResultFieldOK"),
              transformMeta);
      remarks.add(cr);
    }

    if (xsdSource.equals(SPECIFY_FILENAME) && Utils.isEmpty(xsdFilename)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.XSDFieldEmpty"),
              transformMeta);
      remarks.add(cr);
    }

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "XsdValidatorMeta.CheckResult.ConnectedTransformOK",
                  String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsdValidatorMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
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
   * @param definitions The definitions to use
   * @param resourceNamingInterface The repository to optionally load other resources from (to be
   *     converted to XML)
   * @param metadataProvider the metadataProvider in which non-Hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
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
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(xsdFilename), variables);
        xsdFilename = resourceNamingInterface.nameResource(fileObject, variables, true);
        return xsdFilename;
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
