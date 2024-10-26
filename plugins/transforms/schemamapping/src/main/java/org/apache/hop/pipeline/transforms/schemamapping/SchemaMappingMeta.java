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

package org.apache.hop.pipeline.transforms.schemamapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;

@Transform(
    id = "SchemaMapping",
    image = "schemamapping.svg",
    name = "i18n::SchemaMapping.Name",
    description = "i18n::SchemaMapping.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Mapping",
    keywords = "i18n::SchemaMappingMeta.keyword",
    documentationUrl = "/pipeline/transforms/schemamapping.html")
public class SchemaMappingMeta extends BaseTransformMeta<SchemaMapping, SchemaMappingData> {

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.STATIC_SCHEMA_DEFINITION)
  private String schemaName;

  @HopMetadataProperty(
      groupKey = "mappings",
      key = "mapping",
      injectionKey = "MAPPING_FIELD",
      injectionGroupKey = "MAPPING_FIELDS",
      injectionGroupDescription = "SchemaMappingMeta.Injection.Fields",
      injectionKeyDescription = "SchemaMappingMeta.Injection.Field")
  private List<SchemaMappingField> mappingFieldset;

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public List<SchemaMappingField> getMappingFieldset() {
    return mappingFieldset;
  }

  public void setMappingFieldset(List<SchemaMappingField> mappingFieldset) {
    this.mappingFieldset = mappingFieldset;
  }

  public SchemaMappingMeta() {
    super(); // allocate BaseTransformMeta
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

    SchemaDefinition sd =
        (new SchemaDefinitionUtil().loadSchemaDefinition(metadataProvider, schemaName));

    HashMap<String, String> mappingsMap = new HashMap<>();
    for (SchemaMappingField smf : mappingFieldset) {
      mappingsMap.put(smf.getFieldSchemaDefinition(), smf.getFieldStream());
    }

    try {
      IRowMeta selectedSchemaRowMeta = new RowMeta();

      for (int i = 0; i < sd.getRowMeta().size(); i++) {
        IValueMeta v = sd.getRowMeta().getValueMeta(i);
        if (mappingsMap.get(v.getName()) != null) {
          IValueMeta vInputItem = inputRowMeta.searchValueMeta(mappingsMap.get(v.getName()));
          if (vInputItem != null) {
            v.setOrigin(vInputItem.getOrigin());
          }
        } else {
          v.setOrigin(name);
        }
        selectedSchemaRowMeta.addValueMeta(v);
      }
      inputRowMeta.clear();
      inputRowMeta.addRowMeta(selectedSchemaRowMeta);
    } catch (HopPluginException e) {
      throw new HopTransformException(e);
    }
  }

  @Override
  public void setDefault() {
    this.mappingFieldset = new ArrayList<SchemaMappingField>();
  }
}
