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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "XSLT",
    image = "XSLT.svg",
    name = "i18n::XSLT.name",
    description = "i18n::XSLT.description",
    categoryDescription = "i18n::XSLT.category",
    keywords = "i18n::XsltMeta.keyword",
    documentationUrl = "/pipeline/transforms/xslt.html")
@Getter
@Setter
public class XsltMeta extends BaseTransformMeta<Xslt, XsltData> {
  private static final Class<?> PKG = XsltMeta.class;

  @HopMetadataProperty(key = "xslfilename")
  private String xslFilename;

  @HopMetadataProperty(key = "fieldname")
  private String fieldName;

  @HopMetadataProperty(key = "resultfieldname")
  private String resultFieldName;

  @HopMetadataProperty(key = "xslfilefield")
  private String xslFileField;

  @HopMetadataProperty(key = "xslfilefielduse")
  private boolean xslFileFieldUse;

  @HopMetadataProperty(key = "xslfieldisafile")
  private boolean xslFieldIsAFile;

  @HopMetadataProperty(key = "xslfactory")
  private String xslFactory;

  @HopMetadataProperty(key = "outputproperty", groupKey = "outputproperties")
  private List<OutputProperty> outputProperties;

  @HopMetadataProperty(key = "parameter", groupKey = "parameters")
  private List<Parameter> parameters;

  public XsltMeta() {
    super();
    resultFieldName = "result";
    xslFactory = "JAXP";
    xslFieldIsAFile = true;
    outputProperties = new ArrayList<>();
    parameters = new ArrayList<>();
  }

  public XsltMeta(XsltMeta m) {
    this();
    this.fieldName = m.fieldName;
    this.resultFieldName = m.resultFieldName;
    this.xslFactory = m.xslFactory;
    this.xslFieldIsAFile = m.xslFieldIsAFile;
    this.xslFileField = m.xslFileField;
    this.xslFileFieldUse = m.xslFileFieldUse;
    this.xslFilename = m.xslFilename;
    m.outputProperties.forEach(p -> this.outputProperties.add(new OutputProperty(p)));
    m.parameters.forEach(p -> this.parameters.add(new Parameter(p)));
  }

  @Override
  public Object clone() {
    return new XsltMeta(this);
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
    IValueMeta v = new ValueMetaString(variables.resolve(getResultFieldName()));
    v.setOrigin(name);
    inputRowMeta.addValueMeta(v);
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

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "XsltMeta.CheckResult.ConnectedTransformOK", String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
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
    if (getResultFieldName() == null) {
      // Result Field is missing !
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorResultFieldNameMissing"),
              transformMeta);
      remarks.add(cr);
    }

    // Check if XSL Filename field is provided
    if (xslFileFieldUse) {
      if (getXslFileField() == null) {
        // Result Field is missing !
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorResultXSLFieldNameMissing"),
                transformMeta);
        remarks.add(cr);
      } else {
        // Result Field is provided !
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorResultXSLFieldNameOK"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      if (xslFilename == null) {
        // Result Field is missing !
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "XsltMeta.CheckResult.ErrorXSLFileNameMissing"),
                transformMeta);
        remarks.add(cr);

      } else {
        // Check if it's exist and it's a file
        String realFilename = variables.resolve(xslFilename);
        File f = new File(realFilename);

        if (f.exists()) {
          if (f.isFile()) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "XsltMeta.CheckResult.FileExists", realFilename),
                    transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "XsltMeta.CheckResult.ExistsButNoFile", realFilename),
                    transformMeta);
            remarks.add(cr);
          }
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "XsltMeta.CheckResult.FileNotExists", realFilename),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Getter
  @Setter
  public static class OutputProperty {
    @HopMetadataProperty(key = "name", injectionKey = "PROPERTY_NAME")
    private String outputPropertyName;

    @HopMetadataProperty(key = "value", injectionKey = "PROPERTY_VALUE")
    private String outputPropertyValue;

    public OutputProperty() {}

    public OutputProperty(OutputProperty p) {
      this();
      this.outputPropertyName = p.outputPropertyName;
      this.outputPropertyValue = p.outputPropertyValue;
    }
  }

  @Getter
  @Setter
  public static class Parameter {
    /** parameter name */
    @HopMetadataProperty(key = "name", injectionKey = "PARAMETER_NAME")
    private String parameterName;

    /** parameter field */
    @HopMetadataProperty(key = "field", injectionKey = "PARAMETER_FIELD")
    private String parameterField;

    public Parameter() {}

    public Parameter(Parameter p) {
      this();
      this.parameterField = p.parameterField;
      this.parameterName = p.parameterName;
    }
  }
}
