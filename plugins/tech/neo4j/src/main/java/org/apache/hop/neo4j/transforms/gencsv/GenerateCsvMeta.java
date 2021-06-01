/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.transforms.gencsv;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "Neo4jLoad",
    name = "Neo4j Generate CSVs",
    description =
        "Generate CSV files for nodes and relationships in the import/ folder for use with neo4j-import",
    image = "neo4j_load.svg",
    categoryDescription = "Neo4j",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/neo4j-gencsv.adoc")
public class GenerateCsvMeta extends BaseTransformMeta
    implements ITransformMeta<GenerateCsv, GenerateCsvData> {

  public static final String GRAPH_FIELD_NAME = "graph_field_name";
  public static final String BASE_FOLDER = "base_folder";
  public static final String UNIQUENESS_STRATEGY = "uniqueness_strategy";
  public static final String FILES_PREFIX = "files_prefix";
  public static final String FILENAME_FIELD = "filename_field";
  public static final String FILE_TYPE_FIELD = "file_type_field";

  protected String graphFieldName;
  protected String baseFolder;
  protected UniquenessStrategy uniquenessStrategy;

  protected String filesPrefix;
  protected String filenameField;
  protected String fileTypeField;

  @Override
  public void setDefault() {
    baseFolder = "/var/lib/neo4j/";
    uniquenessStrategy = UniquenessStrategy.None;
    filesPrefix = "prefix";
    filenameField = "filename";
    fileTypeField = "fileType";
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider) {

    inputRowMeta.clear();

    IValueMeta filenameValueMeta = new ValueMetaString(space.resolve(filenameField));
    filenameValueMeta.setOrigin(name);
    inputRowMeta.addValueMeta(filenameValueMeta);

    IValueMeta fileTypeValueMeta = new ValueMetaString(space.resolve(fileTypeField));
    fileTypeValueMeta.setOrigin(name);
    inputRowMeta.addValueMeta(fileTypeValueMeta);
  }

  @Override
  public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append(XmlHandler.addTagValue(GRAPH_FIELD_NAME, graphFieldName));
    xml.append(XmlHandler.addTagValue(BASE_FOLDER, baseFolder));
    xml.append(
        XmlHandler.addTagValue(
            UNIQUENESS_STRATEGY, uniquenessStrategy != null ? uniquenessStrategy.name() : null));
    xml.append(XmlHandler.addTagValue(FILES_PREFIX, filesPrefix));
    xml.append(XmlHandler.addTagValue(FILENAME_FIELD, filenameField));
    xml.append(XmlHandler.addTagValue(FILE_TYPE_FIELD, fileTypeField));
    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    graphFieldName = XmlHandler.getTagValue(transformNode, GRAPH_FIELD_NAME);
    baseFolder = XmlHandler.getTagValue(transformNode, BASE_FOLDER);
    uniquenessStrategy =
        UniquenessStrategy.getStrategyFromName(
            XmlHandler.getTagValue(transformNode, UNIQUENESS_STRATEGY));
    filesPrefix = XmlHandler.getTagValue(transformNode, FILES_PREFIX);
    filenameField = XmlHandler.getTagValue(transformNode, FILENAME_FIELD);
    fileTypeField = XmlHandler.getTagValue(transformNode, FILE_TYPE_FIELD);
  }

  @Override
  public GenerateCsv createTransform(
      TransformMeta transformMeta,
      GenerateCsvData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GenerateCsv(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public GenerateCsvData getTransformData() {
    return new GenerateCsvData();
  }

  /**
   * Gets graphFieldName
   *
   * @return value of graphFieldName
   */
  public String getGraphFieldName() {
    return graphFieldName;
  }

  /** @param graphFieldName The graphFieldName to set */
  public void setGraphFieldName(String graphFieldName) {
    this.graphFieldName = graphFieldName;
  }

  /**
   * Gets baseFolder
   *
   * @return value of baseFolder
   */
  public String getBaseFolder() {
    return baseFolder;
  }

  /** @param baseFolder The baseFolder to set */
  public void setBaseFolder(String baseFolder) {
    this.baseFolder = baseFolder;
  }

  /**
   * Gets nodeUniquenessStrategy
   *
   * @return value of nodeUniquenessStrategy
   */
  public UniquenessStrategy getUniquenessStrategy() {
    return uniquenessStrategy;
  }

  /** @param uniquenessStrategy The nodeUniquenessStrategy to set */
  public void setUniquenessStrategy(UniquenessStrategy uniquenessStrategy) {
    this.uniquenessStrategy = uniquenessStrategy;
  }

  /**
   * Gets filesPrefix
   *
   * @return value of filesPrefix
   */
  public String getFilesPrefix() {
    return filesPrefix;
  }

  /** @param filesPrefix The filesPrefix to set */
  public void setFilesPrefix(String filesPrefix) {
    this.filesPrefix = filesPrefix;
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets fileTypeField
   *
   * @return value of fileTypeField
   */
  public String getFileTypeField() {
    return fileTypeField;
  }

  /** @param fileTypeField The fileTypeField to set */
  public void setFileTypeField(String fileTypeField) {
    this.fileTypeField = fileTypeField;
  }
}
