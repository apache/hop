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
package org.apache.hop.pipeline.transforms.plugincatalog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Plugin Catalog transform metadata.
 *
 * <p>Emits one row per plugin (or per plugin property) by reflecting over the live Hop {@link
 * org.apache.hop.core.plugins.PluginRegistry}. Labels are resolved to human-readable text; output
 * is never stale against the running Hop version or any installed third-party plugins.
 */
@Getter
@Setter
@Transform(
    id = "PluginCatalog",
    image = "plugincatalog.svg",
    name = "i18n::PluginCatalog.Name",
    description = "i18n::PluginCatalog.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/plugincatalog.html",
    keywords = "i18n::PluginCatalog.Keywords")
public class PluginCatalogMeta extends BaseTransformMeta<PluginCatalog, PluginCatalogData> {

  // Output field names shared by both detail levels.
  public static final String FIELD_PLUGIN_ID = "plugin_id";
  public static final String FIELD_PLUGIN_TYPE = "plugin_type";
  public static final String FIELD_NAME = "name";
  public static final String FIELD_DESCRIPTION = "description";
  public static final String FIELD_CATEGORY = "category";
  public static final String FIELD_KEYWORDS = "keywords";
  public static final String FIELD_CLASS_NAME = "class_name";
  public static final String FIELD_ENGLISH_ALIASES = "english_aliases";
  public static final String FIELD_LOCALE = "locale";

  /** Number of plugin-level columns emitted before the detail-level columns. */
  public static final int BASE_FIELD_COUNT = 9;

  // PER_PLUGIN extra.
  public static final String FIELD_METADATA_FIELDS = "metadata_fields";
  // PER_PROPERTY extras.
  public static final String FIELD_PROPERTY_FIELD = "property_field";
  public static final String FIELD_PROPERTY_XML_KEY = "property_xml_key";
  public static final String FIELD_PROPERTY_JAVA_TYPE = "property_java_type";
  public static final String FIELD_PROPERTY_PASSWORD = "property_password";
  public static final String FIELD_PROPERTY_GROUP = "property_group";

  @HopMetadataProperty(key = "includeTransforms", injectionKey = "INCLUDE_TRANSFORMS")
  private boolean includeTransforms = true;

  @HopMetadataProperty(key = "includeActions", injectionKey = "INCLUDE_ACTIONS")
  private boolean includeActions = true;

  @HopMetadataProperty(key = "includeMetadataTypes", injectionKey = "INCLUDE_METADATA_TYPES")
  private boolean includeMetadataTypes = true;

  @HopMetadataProperty(key = "detailLevel", injectionKey = "DETAIL_LEVEL")
  private DetailLevel detailLevel = DetailLevel.PER_PLUGIN;

  public PluginCatalogMeta() {
    super();
  }

  @Override
  public void setDefault() {
    includeTransforms = true;
    includeActions = true;
    includeMetadataTypes = true;
    detailLevel = DetailLevel.PER_PLUGIN;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      row.clear();
      addString(row, FIELD_PLUGIN_ID, origin);
      addString(row, FIELD_PLUGIN_TYPE, origin);
      addString(row, FIELD_NAME, origin);
      addString(row, FIELD_DESCRIPTION, origin);
      addString(row, FIELD_CATEGORY, origin);
      addString(row, FIELD_KEYWORDS, origin);
      addString(row, FIELD_CLASS_NAME, origin);
      addString(row, FIELD_ENGLISH_ALIASES, origin);
      addString(row, FIELD_LOCALE, origin);

      if (detailLevel == DetailLevel.PER_PROPERTY) {
        addString(row, FIELD_PROPERTY_FIELD, origin);
        addString(row, FIELD_PROPERTY_XML_KEY, origin);
        addString(row, FIELD_PROPERTY_JAVA_TYPE, origin);
        addBoolean(row, FIELD_PROPERTY_PASSWORD, origin);
        addString(row, FIELD_PROPERTY_GROUP, origin);
      } else {
        addString(row, FIELD_METADATA_FIELDS, origin);
      }
    } catch (Exception e) {
      throw new HopTransformException("Error creating Plugin Catalog output fields", e);
    }
  }

  private static void addString(IRowMeta row, String name, String origin) {
    ValueMetaString field = new ValueMetaString(name);
    field.setOrigin(origin);
    row.addValueMeta(field);
  }

  private static void addBoolean(IRowMeta row, String name, String origin) {
    ValueMetaBoolean field = new ValueMetaBoolean(name);
    field.setOrigin(origin);
    row.addValueMeta(field);
  }
}
