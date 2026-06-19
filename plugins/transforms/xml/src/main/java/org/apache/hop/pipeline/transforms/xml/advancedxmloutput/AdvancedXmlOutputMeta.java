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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "AdvancedXMLOutput",
    image = "AXO.svg",
    name = "i18n::AdvancedXMLOutput.name",
    description = "i18n::AdvancedXMLOutput.description",
    categoryDescription = "i18n::AdvancedXMLOutput.category",
    keywords = "i18n::AdvancedXmlOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/xmloutputadvanced.html",
    isIncludeJdbcDrivers = false)
@Getter
@Setter
public class AdvancedXmlOutputMeta
    extends BaseTransformMeta<AdvancedXmlOutput, AdvancedXmlOutputData> {

  private static final Class<?> PKG = AdvancedXmlOutputMeta.class;

  public static final String OPERATION_TYPE_WRITE_TO_FILE = "writetofile";

  public static final String OPERATION_TYPE_OUTPUT_VALUE = "outputvalue";

  public static final String OPERATION_TYPE_BOTH = "both";

  @Getter
  public enum XmlOutputOperation implements IEnumHasCodeAndDescription {
    WRITE_TO_FILE(
        OPERATION_TYPE_WRITE_TO_FILE,
        BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.OperationType.WriteToFile")),
    OUTPUT_VALUE(
        OPERATION_TYPE_OUTPUT_VALUE,
        BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.OperationType.OutputValue")),
    BOTH(
        OPERATION_TYPE_BOTH,
        BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.OperationType.Both"));

    private final String code;
    private final String description;

    XmlOutputOperation(String code, String description) {
      this.code = code;
      this.description = description;
    }

    @Override
    public String getCode() {
      return code;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  @HopMetadataProperty(key = "file")
  private XmlFileOutputSupport fileSupport;

  @Getter(AccessLevel.NONE)
  @Injection(name = "", group = "GENERAL")
  @HopMetadataProperty(
      key = "operation_type",
      storeWithCode = true,
      injectionKey = "OPERATION",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.OPERATION")
  private XmlOutputOperation operationType = XmlOutputOperation.WRITE_TO_FILE;

  @HopMetadataProperty(key = "output_xml_field")
  private String outputXmlField;

  /**
   * When true (default), each row emitted in output-to-field modes carries all input fields plus
   * the generated XML. When false, only the XML field is emitted (narrow stream).
   */
  @HopMetadataProperty(
      key = "include_input_fields_in_output",
      defaultBoolean = true,
      injectionKey = "INCLUDE_INPUT_FIELDS_IN_OUTPUT",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.INCLUDE_INPUT_FIELDS_IN_OUTPUT")
  private boolean includeInputFieldsInOutput = true;

  public XmlOutputOperation getOperationType() {
    return operationType == null ? XmlOutputOperation.WRITE_TO_FILE : operationType;
  }

  public void setOperationType(XmlOutputOperation operationType) {
    this.operationType = operationType;
  }

  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.ENCODING")
  private String encoding;

  /** When true, suppress whitespace and EOL between elements. */
  @HopMetadataProperty(
      key = "compact_file",
      injectionKey = "COMPACT_FILE",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.COMPACT_FILE")
  private boolean compactFile;

  @HopMetadataProperty(
      key = "blank_line_after_xml_decl",
      injectionKey = "BLANK_LINE_AFTER_XML_DECL",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.BLANK_LINE_AFTER_XML_DECL")
  private boolean blankLineAfterXmlDeclaration;

  @HopMetadataProperty(
      key = "create_attr_if_null",
      injectionKey = "CREATE_ATTR_IF_NULL",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.CREATE_ATTR_IF_NULL")
  private boolean createAttributeIfNull;

  @HopMetadataProperty(
      key = "create_attr_if_unmapped",
      injectionKey = "CREATE_ATTR_IF_UNMAPPED",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.CREATE_ATTR_IF_UNMAPPED")
  private boolean createAttributeIfUnmapped;

  /** When true, an element with no mapped field is still emitted as an empty tag. */
  @HopMetadataProperty(
      key = "create_empty_element",
      injectionKey = "CREATE_EMPTY_ELEMENT",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.CREATE_EMPTY_ELEMENT")
  private boolean createEmptyElement;

  /** Trim leading/trailing whitespace on emitted text values. */
  @HopMetadataProperty(
      key = "trim_values",
      injectionKey = "TRIM_VALUES",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.TRIM_VALUES")
  private boolean trimValues;

  @HopMetadataProperty(
      key = "default_decimal_separator",
      injectionKey = "DEFAULT_DECIMAL_SEPARATOR",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DEFAULT_DECIMAL_SEPARATOR")
  private String defaultDecimalSeparator;

  @HopMetadataProperty(
      key = "default_grouping_separator",
      injectionKey = "DEFAULT_GROUPING_SEPARATOR",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DEFAULT_GROUPING_SEPARATOR")
  private String defaultGroupingSeparator;

  @HopMetadataProperty(
      key = "generate_xsd",
      injectionKey = "GENERATE_XSD",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.GENERATE_XSD")
  private boolean generateXsd;

  @HopMetadataProperty(
      key = "doctype_root",
      injectionKey = "DOCTYPE_ROOT",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DOCTYPE_ROOT")
  private String doctypeRootElement;

  @HopMetadataProperty(
      key = "doctype_system",
      injectionKey = "DOCTYPE_SYSTEM",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DOCTYPE_SYSTEM")
  private String doctypeSystemId;

  @HopMetadataProperty(
      key = "doctype_public",
      injectionKey = "DOCTYPE_PUBLIC",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DOCTYPE_PUBLIC")
  private String doctypePublicId;

  /**
   * Optional XSL stylesheet href to emit as an {@code <?xml-stylesheet ?>} processing instruction.
   */
  @HopMetadataProperty(
      key = "xsl_stylesheet_href",
      injectionKey = "XSL_HREF",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.XSL_HREF")
  private String xslStylesheetHref;

  @HopMetadataProperty(
      key = "xsl_stylesheet_type",
      injectionKey = "XSL_TYPE",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.XSL_TYPE")
  private String xslStylesheetType;

  @HopMetadataProperty(key = "tree")
  private XmlNode rootNode;

  public AdvancedXmlOutputMeta() {
    super();
    this.fileSupport = new XmlFileOutputSupport();
    this.encoding = Const.UTF_8;
    this.blankLineAfterXmlDeclaration = true;
    this.createEmptyElement = true;
    this.xslStylesheetType = "text/xsl";
    this.rootNode = defaultRootNode();
    this.operationType = XmlOutputOperation.WRITE_TO_FILE;
    this.outputXmlField = "outputXml";
  }

  public AdvancedXmlOutputMeta(AdvancedXmlOutputMeta m) {
    this();
    this.fileSupport = new XmlFileOutputSupport(m.fileSupport);
    this.encoding = m.encoding;
    this.compactFile = m.compactFile;
    this.blankLineAfterXmlDeclaration = m.blankLineAfterXmlDeclaration;
    this.createAttributeIfNull = m.createAttributeIfNull;
    this.createAttributeIfUnmapped = m.createAttributeIfUnmapped;
    this.createEmptyElement = m.createEmptyElement;
    this.trimValues = m.trimValues;
    this.defaultDecimalSeparator = m.defaultDecimalSeparator;
    this.defaultGroupingSeparator = m.defaultGroupingSeparator;
    this.generateXsd = m.generateXsd;
    this.doctypeRootElement = m.doctypeRootElement;
    this.doctypeSystemId = m.doctypeSystemId;
    this.doctypePublicId = m.doctypePublicId;
    this.xslStylesheetHref = m.xslStylesheetHref;
    this.xslStylesheetType = m.xslStylesheetType;
    this.rootNode = m.rootNode == null ? defaultRootNode() : new XmlNode(m.rootNode);
    this.operationType = m.getOperationType();
    this.outputXmlField = m.outputXmlField;
    this.includeInputFieldsInOutput = m.isIncludeInputFieldsInOutput();
  }

  @Override
  public Object clone() {
    return new AdvancedXmlOutputMeta(this);
  }

  private static XmlNode defaultRootNode() {
    XmlNode root = new XmlNode("Rows", XmlNode.NodeKind.Element);
    XmlNode loop = new XmlNode("Row", XmlNode.NodeKind.Element);
    loop.setLoop(true);
    root.addChild(loop);
    return root;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!writesXmlField()) {
      return;
    }
    try {
      String fieldName = variables.resolve(outputXmlField);
      if (!Utils.isEmpty(fieldName)) {
        if (!includeInputFieldsInOutput) {
          row.clear();
        }
        row.addValueMeta(ValueMetaFactory.createValueMeta(fieldName, IValueMeta.TYPE_STRING));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String resolvedOperationType() {
    return getOperationType().getCode();
  }

  public boolean writesXmlField() {
    XmlOutputOperation op = getOperationType();
    return op == XmlOutputOperation.OUTPUT_VALUE || op == XmlOutputOperation.BOTH;
  }

  public boolean writesXmlFile() {
    XmlOutputOperation op = getOperationType();
    return op == XmlOutputOperation.WRITE_TO_FILE || op == XmlOutputOperation.BOTH;
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

    List<String> validationErrors = AdvancedXmlOutputValidator.validate(rootNode, prev);
    if (validationErrors.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AdvancedXMLOutputMeta.CheckResult.TreeOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      for (String err : validationErrors) {
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, err, transformMeta));
      }
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AdvancedXMLOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AdvancedXMLOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    if (writesXmlField() && Utils.isEmpty(variables.resolve(outputXmlField))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "AdvancedXMLOutputMeta.CheckResult.OutputXmlFieldMissing"),
              transformMeta));
    }

    if (writesXmlFile()
        && fileSupport != null
        && Utils.isEmpty(variables.resolve(fileSupport.getFileName()))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AdvancedXMLOutputMeta.CheckResult.FilenameMissing"),
              transformMeta));
    }

    if (!writesXmlFile() && fileSupport != null && fileSupport.isZipped()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AdvancedXMLOutputMeta.CheckResult.ZipNeedsFile"),
              transformMeta));
    }
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      if (writesXmlFile() && fileSupport != null && !Utils.isEmpty(fileSupport.getFileName())) {
        FileObject fileObject =
            HopVfs.getFileObject(variables.resolve(fileSupport.getFileName()), variables);
        fileSupport.setFileName(resourceNamingInterface.nameResource(fileObject, variables, true));
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
