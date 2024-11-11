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
 */

package org.apache.hop.neo4j.transforms.gencsv;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "Neo4jLoad",
    name = "i18n::GenerateCsvMeta.name",
    description = "i18n::GenerateCsvMeta.description",
    image = "neo4j_load.svg",
    categoryDescription = "i18n::GenerateCsvMeta.categoryDescription",
    keywords = "i18n::GenerateCsvMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-gencsv.html")
public class GenerateCsvMeta extends BaseTransformMeta<GenerateCsv, GenerateCsvData> {

  @HopMetadataProperty(key = "graph_field_name")
  protected String graphFieldName;

  @HopMetadataProperty(key = "base_folder")
  protected String baseFolder;

  @HopMetadataProperty(key = "uniqueness_strategy")
  protected UniquenessStrategy uniquenessStrategy;

  @HopMetadataProperty(key = "files_prefix")
  protected String filesPrefix;

  @HopMetadataProperty(key = "filename_field")
  protected String filenameField;

  @HopMetadataProperty(key = "file_type_field")
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
}
