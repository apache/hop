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
package org.apache.hop.pgvector.transforms.upsert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pgvector.util.PgVectorColumnMapping;
import org.apache.hop.pgvector.util.PgVectorSchemaBuilder;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "PgVectorUpsert",
    image = "vector-store.svg",
    name = "i18n::PgVectorUpsert.Name",
    description = "i18n::PgVectorUpsert.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "pgvector,vector,embedding,ai,rag,postgres,postgresql,upsert",
    documentationUrl = "/pipeline/transforms/pgvector-upsert.html")
@GuiPlugin
public class PgVectorUpsertMeta extends BaseTransformMeta<PgVectorUpsert, PgVectorUpsertData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PGVECTOR_UPSERT_DIALOG_OPTIONS";
  public static final String WIDGET_ID_FIELD = "PGVECTOR_UPSERT_ID_FIELD";
  public static final String WIDGET_DOCUMENT_ID_FIELD = "PGVECTOR_UPSERT_DOCUMENT_ID_FIELD";
  public static final String WIDGET_CHUNK_INDEX_FIELD = "PGVECTOR_UPSERT_CHUNK_INDEX_FIELD";
  public static final String WIDGET_CONTENT_FIELD = "PGVECTOR_UPSERT_CONTENT_FIELD";
  public static final String WIDGET_EMBEDDING_FIELD = "PGVECTOR_UPSERT_EMBEDDING_FIELD";

  private static final String TAB_MAIN = "i18n::PgVectorUpsert.Tab.Main";
  private static final String TAB_MAIN_ORDER = "0100";

  private static final Class<?> PKG = PgVectorUpsertMeta.class;

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::PgVectorUpsert.connection.Label",
      toolTip = "i18n::PgVectorUpsert.connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @GuiWidgetElement(
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorUpsert.schemaName.Label",
      toolTip = "i18n::PgVectorUpsert.schemaName.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "schemaName")
  private String schemaName = "public";

  @GuiWidgetElement(
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorUpsert.tableName.Label",
      toolTip = "i18n::PgVectorUpsert.tableName.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "tableName")
  private String tableName = "hop_rag_chunks";

  @GuiWidgetElement(
      id = WIDGET_ID_FIELD,
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorUpsert.idField.Label",
      toolTip = "i18n::PgVectorUpsert.idField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "idField")
  private String idField;

  @GuiWidgetElement(
      id = WIDGET_DOCUMENT_ID_FIELD,
      order = "0500",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorUpsert.documentIdField.Label",
      toolTip = "i18n::PgVectorUpsert.documentIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "documentIdField")
  private String documentIdField = "document_id";

  @GuiWidgetElement(
      id = WIDGET_CHUNK_INDEX_FIELD,
      order = "0600",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorUpsert.chunkIndexField.Label",
      toolTip = "i18n::PgVectorUpsert.chunkIndexField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "chunkIndexField")
  private String chunkIndexField = "chunk_index";

  @GuiWidgetElement(
      id = WIDGET_CONTENT_FIELD,
      order = "0700",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorUpsert.contentField.Label",
      toolTip = "i18n::PgVectorUpsert.contentField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "contentField")
  private String contentField = "chunk_text";

  @GuiWidgetElement(
      id = WIDGET_EMBEDDING_FIELD,
      order = "0800",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorUpsert.embeddingField.Label",
      toolTip = "i18n::PgVectorUpsert.embeddingField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "embeddingField")
  private String embeddingField = "embedding";

  @GuiWidgetElement(
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorUpsert.embeddingDimensions.Label",
      toolTip = "i18n::PgVectorUpsert.embeddingDimensions.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "embeddingDimensions")
  private String embeddingDimensions = "768";

  @GuiWidgetElement(
      order = "1000",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PgVectorUpsert.createTableIfMissing.Label",
      toolTip = "i18n::PgVectorUpsert.createTableIfMissing.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "createTableIfMissing")
  private boolean createTableIfMissing = true;

  @GuiWidgetElement(
      order = "1100",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PgVectorUpsert.createHnswIndex.Label",
      toolTip = "i18n::PgVectorUpsert.createHnswIndex.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "createHnswIndex")
  private boolean createHnswIndex = true;

  @GuiWidgetElement(
      order = "1300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PgVectorUpsert.deleteDocumentBeforeUpsert.Label",
      toolTip = "i18n::PgVectorUpsert.deleteDocumentBeforeUpsert.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "deleteDocumentBeforeUpsert")
  private boolean deleteDocumentBeforeUpsert = false;

  @GuiWidgetElement(
      order = "1400",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorUpsert.commitSize.Label",
      toolTip = "i18n::PgVectorUpsert.commitSize.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "commitSize")
  private String commitSize = "100";

  @GuiWidgetElement(
      order = "1200",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorUpsert.indexMetric.Label",
      toolTip = "i18n::PgVectorUpsert.indexMetric.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "indexMetric")
  private VectorDistanceMetric indexMetric = VectorDistanceMetric.COSINE;

  @HopMetadataProperty(
      key = "mapping",
      injectionGroupKey = "MAPPINGS",
      injectionKeyDescription = "PgVectorUpsertMeta.Injection.MAPPING")
  private List<PgVectorColumnMapping> columnMappings = new ArrayList<>();

  public PgVectorUpsertMeta() {
    columnMappings = new ArrayList<>();
  }

  @Override
  public Object clone() {
    PgVectorUpsertMeta copy = (PgVectorUpsertMeta) super.clone();
    copy.columnMappings = new ArrayList<>();
    if (columnMappings != null) {
      for (PgVectorColumnMapping mapping : columnMappings) {
        copy.columnMappings.add(
            new PgVectorColumnMapping(mapping.getColumnName(), mapping.getStreamField()));
      }
    }
    return copy;
  }

  @Override
  public void setDefault() {
    connection = "";
    schemaName = "public";
    tableName = "hop_rag_chunks";
    documentIdField = "document_id";
    chunkIndexField = "chunk_index";
    contentField = "chunk_text";
    embeddingField = "embedding";
    embeddingDimensions = "768";
    createTableIfMissing = true;
    createHnswIndex = true;
    deleteDocumentBeforeUpsert = false;
    commitSize = "100";
    indexMetric = VectorDistanceMetric.COSINE;
    columnMappings = new ArrayList<>();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // Pass-through transform: output row layout matches input.
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

    if (Utils.isEmpty(connection)) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.ConnectionRequired");
    }
    if (Utils.isEmpty(tableName)) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.TableRequired");
    }

    if (Utils.isEmpty(contentField)) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.ContentFieldRequired");
    } else if (prev != null && prev.indexOfValue(contentField) < 0) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.ContentFieldNotFound", contentField);
    }

    if (Utils.isEmpty(embeddingField)) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.EmbeddingFieldRequired");
    } else if (prev != null && prev.indexOfValue(embeddingField) < 0) {
      error(
          remarks,
          transformMeta,
          "PgVectorUpsert.Validation.EmbeddingFieldNotFound",
          embeddingField);
    }

    if (Utils.isEmpty(idField)) {
      if (Utils.isEmpty(documentIdField) || Utils.isEmpty(chunkIndexField)) {
        error(remarks, transformMeta, "PgVectorUpsert.Validation.NoIdSource");
      }
    } else if (prev != null && prev.indexOfValue(idField) < 0) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.IdFieldNotFound", idField);
    }

    // Both accept variables, which a design-time check cannot resolve. Only a value that is
    // genuinely fixed can be judged here; anything still holding ${...} is left to run time.
    if (isResolvedNumberBad(variables, embeddingDimensions, dimensions -> dimensions <= 0)) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.DimensionsPositive");
    }
    if (isResolvedNumberBad(variables, commitSize, size -> size < 0)) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.CommitSizeNonNegative");
    }

    if (columnMappings != null) {
      for (PgVectorColumnMapping mapping : columnMappings) {
        if (mapping == null || Utils.isEmpty(mapping.getColumnName())) {
          continue;
        }
        if (PgVectorSchemaBuilder.isReservedColumn(mapping.getColumnName())) {
          error(
              remarks,
              transformMeta,
              "PgVectorUpsert.Validation.MappingColumnReserved",
              mapping.getColumnName());
        }
        if (!Utils.isEmpty(mapping.getStreamField())
            && prev != null
            && prev.indexOfValue(mapping.getStreamField()) < 0) {
          error(
              remarks,
              transformMeta,
              "PgVectorUpsert.Validation.MappingStreamFieldNotFound",
              mapping.getStreamField());
        }
      }
    }

    boolean multipleCopies = transformMeta != null && transformMeta.getCopies(variables) > 1;

    if (createTableIfMissing) {
      warning(remarks, transformMeta, "PgVectorUpsert.Validation.CreateExtensionPrivileges");
    }
    // The HNSW index and the mapped-column ALTERs run whether or not this transform creates the
    // table, so any of the three puts concurrent DDL in the path of a second copy.
    boolean runsDdl =
        createTableIfMissing
            || createHnswIndex
            || (columnMappings != null && !columnMappings.isEmpty());
    if (runsDdl && multipleCopies) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.DdlWithMultipleCopies");
    }

    // deletedDocuments is per copy, so a second copy can delete rows the first just inserted for
    // the same document.
    if (deleteDocumentBeforeUpsert && multipleCopies) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.DeleteWithMultipleCopies");
    }
  }

  /**
   * True when {@code value} resolves to a fixed number that {@code invalid} rejects. A value that
   * still holds a variable after substitution cannot be judged at design time and passes.
   */
  private static boolean isResolvedNumberBad(
      IVariables variables, String value, IntPredicate invalid) {
    String resolved = variables.resolve(value);
    if (StringUtil.containsVariableToken(resolved)) {
      return false;
    }
    return invalid.test(Const.toInt(resolved, -1));
  }

  private static void error(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... parameters) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(PKG, key, parameters),
            transformMeta));
  }

  private static void warning(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... parameters) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, key, parameters),
            transformMeta));
  }
}
