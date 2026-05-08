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
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
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

/**
 * Metadata for the Advanced XML Output transform: writes input rows to one or more XML files
 * following a hierarchical, user-defined XML tree (with one row-loop element and optional group-by
 * ancestors).
 */
@Transform(
    id = "AdvancedXMLOutput",
    image = "AXO.svg",
    name = "i18n::AdvancedXMLOutput.name",
    description = "i18n::AdvancedXMLOutput.description",
    categoryDescription = "i18n::AdvancedXMLOutput.category",
    keywords = "i18n::AdvancedXmlOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/advancedxmloutput.html",
    isIncludeJdbcDrivers = false)
@Getter
@Setter
public class AdvancedXmlOutputMeta
    extends BaseTransformMeta<AdvancedXmlOutput, AdvancedXmlOutputData> {

  private static final Class<?> PKG = AdvancedXmlOutputMeta.class;

  /** Filename / split / zip / result options. */
  @HopMetadataProperty(key = "file")
  private XmlFileOutputSupport fileSupport;

  /** Output character encoding. */
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

  /** Add a blank line after the {@code <?xml ?>} declaration. */
  @HopMetadataProperty(
      key = "blank_line_after_xml_decl",
      injectionKey = "BLANK_LINE_AFTER_XML_DECL",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.BLANK_LINE_AFTER_XML_DECL")
  private boolean blankLineAfterXmlDeclaration;

  /** When true, an emitted attribute keeps its tag even if the source value is null. */
  @HopMetadataProperty(
      key = "create_attr_if_null",
      injectionKey = "CREATE_ATTR_IF_NULL",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.CREATE_ATTR_IF_NULL")
  private boolean createAttributeIfNull;

  /** When true, an attribute with no mapped field is still emitted (with empty / default value). */
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

  /** Optional global decimal separator override (per-node still wins). */
  @HopMetadataProperty(
      key = "default_decimal_separator",
      injectionKey = "DEFAULT_DECIMAL_SEPARATOR",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DEFAULT_DECIMAL_SEPARATOR")
  private String defaultDecimalSeparator;

  /** Optional global grouping separator override (per-node still wins). */
  @HopMetadataProperty(
      key = "default_grouping_separator",
      injectionKey = "DEFAULT_GROUPING_SEPARATOR",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DEFAULT_GROUPING_SEPARATOR")
  private String defaultGroupingSeparator;

  /** When true, write a sibling {@code .xsd} schema describing the produced XML structure. */
  @HopMetadataProperty(
      key = "generate_xsd",
      injectionKey = "GENERATE_XSD",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.GENERATE_XSD")
  private boolean generateXsd;

  /** Optional DOCTYPE root element name. When set, emit DOCTYPE between the XML decl and root. */
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

  /** The hierarchical XML tree definition. */
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
  }

  @Override
  public Object clone() {
    return new AdvancedXmlOutputMeta(this);
  }

  /** Default tree: {@code <Rows><Row loop="true"/></Rows>} so a fresh transform validates. */
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
    // No fields are added to the output stream; rows pass through unchanged.
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

    // Tree: at least one node, exactly one loop node, mapped fields exist
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
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      if (fileSupport != null && !Utils.isEmpty(fileSupport.getFileName())) {
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
