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
package org.apache.hop.pgvector.transforms.search;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "PgVectorSearch",
    image = "vector-store.svg",
    name = "i18n::PgVectorSearch.Name",
    description = "i18n::PgVectorSearch.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "pgvector,vector,embedding,ai,rag,postgres,postgresql,similarity,search",
    documentationUrl = "/pipeline/transforms/pgvector-search.html")
@GuiPlugin
public class PgVectorSearchMeta extends BaseTransformMeta<PgVectorSearch, PgVectorSearchData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PGVECTOR_SEARCH_DIALOG_OPTIONS";
  public static final String WIDGET_EMBEDDING_FIELD = "PGVECTOR_SEARCH_EMBEDDING_FIELD";

  private static final String TAB_MAIN = "i18n::PgVectorSearch.Tab.Main";
  private static final String TAB_MAIN_ORDER = "0100";

  private static final Class<?> PKG = PgVectorSearchMeta.class;

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::PgVectorSearch.connection.Label",
      toolTip = "i18n::PgVectorSearch.connection.Tooltip",
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
      label = "i18n::PgVectorSearch.schemaName.Label",
      toolTip = "i18n::PgVectorSearch.schemaName.Tooltip",
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
      label = "i18n::PgVectorSearch.tableName.Label",
      toolTip = "i18n::PgVectorSearch.tableName.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "tableName")
  private String tableName = "hop_rag_chunks";

  @GuiWidgetElement(
      id = WIDGET_EMBEDDING_FIELD,
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorSearch.embeddingField.Label",
      toolTip = "i18n::PgVectorSearch.embeddingField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "embeddingField")
  private String embeddingField = "embedding";

  @GuiWidgetElement(
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.topK.Label",
      toolTip = "i18n::PgVectorSearch.topK.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "topK")
  private String topK = "5";

  @GuiWidgetElement(
      order = "0600",
      type = GuiElementType.COMBO,
      label = "i18n::PgVectorSearch.distanceMetric.Label",
      toolTip = "i18n::PgVectorSearch.distanceMetric.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "distanceMetric")
  private VectorDistanceMetric distanceMetric = VectorDistanceMetric.COSINE;

  @GuiWidgetElement(
      order = "0700",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.minScore.Label",
      toolTip = "i18n::PgVectorSearch.minScore.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "minScore")
  private String minScore = "0.0";

  /**
   * Have the search eat the incoming row when the query returns nothing. Mirrors the option on
   * Hop's Database Lookup: off by default, so a row that finds no match still reaches the output
   * with empty match fields rather than disappearing.
   */
  @GuiWidgetElement(
      order = "0800",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PgVectorSearch.eatingRowOnNoMatch.Label",
      toolTip = "i18n::PgVectorSearch.eatingRowOnNoMatch.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "eat_row_on_no_match",
      injectionKey = "EAT_ROW_ON_NO_MATCH",
      injectionKeyDescription = "PgVectorSearchMeta.Injection.EAT_ROW_ON_NO_MATCH")
  private boolean eatingRowOnNoMatch;

  @GuiWidgetElement(
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.resultIdField.Label",
      toolTip = "i18n::PgVectorSearch.resultIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "resultIdField")
  private String resultIdField = "match_id";

  @GuiWidgetElement(
      order = "1000",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.resultDocumentIdField.Label",
      toolTip = "i18n::PgVectorSearch.resultDocumentIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "resultDocumentIdField")
  private String resultDocumentIdField = "match_document_id";

  @GuiWidgetElement(
      order = "1100",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.resultChunkIndexField.Label",
      toolTip = "i18n::PgVectorSearch.resultChunkIndexField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "resultChunkIndexField")
  private String resultChunkIndexField = "match_chunk_index";

  @GuiWidgetElement(
      order = "1200",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.resultContentField.Label",
      toolTip = "i18n::PgVectorSearch.resultContentField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "resultContentField")
  private String resultContentField = "match_content";

  @GuiWidgetElement(
      order = "1300",
      type = GuiElementType.TEXT,
      label = "i18n::PgVectorSearch.resultScoreField.Label",
      toolTip = "i18n::PgVectorSearch.resultScoreField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(key = "resultScoreField")
  private String resultScoreField = "match_score";

  @HopMetadataProperty(
      key = "filter",
      injectionGroupKey = "FILTERS",
      injectionKeyDescription = "PgVectorSearchMeta.Injection.FILTER")
  private List<PgVectorSearchFilter> filters = new ArrayList<>();

  public PgVectorSearchMeta() {
    filters = new ArrayList<>();
  }

  @Override
  public Object clone() {
    PgVectorSearchMeta copy = (PgVectorSearchMeta) super.clone();
    copy.filters = new ArrayList<>();
    if (filters != null) {
      for (PgVectorSearchFilter filter : filters) {
        copy.filters.add(
            new PgVectorSearchFilter(
                filter.getColumnName(), filter.getStreamField(), filter.isSkipIfEmpty()));
      }
    }
    return copy;
  }

  @Override
  public void setDefault() {
    connection = "";
    schemaName = "public";
    tableName = "hop_rag_chunks";
    embeddingField = "embedding";
    topK = "5";
    distanceMetric = VectorDistanceMetric.COSINE;
    minScore = "0.0";
    eatingRowOnNoMatch = false;
    resultIdField = "match_id";
    resultDocumentIdField = "match_document_id";
    resultChunkIndexField = "match_chunk_index";
    resultContentField = "match_content";
    resultScoreField = "match_score";
    filters = new ArrayList<>();
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      addField(row, new ValueMetaString(resultIdField), origin);
      addField(row, new ValueMetaString(resultDocumentIdField), origin);
      addField(row, new ValueMetaInteger(resultChunkIndexField), origin);
      addField(row, new ValueMetaString(resultContentField), origin);
      addField(row, new ValueMetaNumber(resultScoreField), origin);
    } catch (Exception e) {
      throw new HopTransformException("Error creating search output fields", e);
    }
  }

  private static void addField(IRowMeta row, IValueMeta field, String origin)
      throws HopPluginException {
    String fieldName = field.getName();
    if (fieldName == null || fieldName.isEmpty() || row.indexOfValue(fieldName) >= 0) {
      return;
    }
    field.setOrigin(origin);
    row.addValueMeta(field);
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
      error(remarks, transformMeta, "PgVectorSearch.Validation.ConnectionRequired");
    }
    if (Utils.isEmpty(tableName)) {
      error(remarks, transformMeta, "PgVectorSearch.Validation.TableRequired");
    }
    if (Utils.isEmpty(embeddingField)) {
      error(remarks, transformMeta, "PgVectorSearch.Validation.EmbeddingFieldRequired");
    } else if (prev != null && prev.indexOfValue(embeddingField) < 0) {
      error(
          remarks,
          transformMeta,
          "PgVectorSearch.Validation.EmbeddingFieldNotFound",
          embeddingField);
    }
    // Top-k accepts variables, which a design-time check cannot resolve. Only a value that is
    // genuinely fixed can be judged here; anything still holding ${...} is left to run time.
    String resolvedTopK = variables.resolve(topK);
    if (!StringUtil.containsVariableToken(resolvedTopK) && Const.toInt(resolvedTopK, -1) < 1) {
      error(remarks, transformMeta, "PgVectorSearch.Validation.TopKPositive");
    }
    if (filters != null) {
      for (PgVectorSearchFilter filter : filters) {
        if (filter == null || Utils.isEmpty(filter.getStreamField())) {
          continue;
        }
        if (prev != null && prev.indexOfValue(filter.getStreamField()) < 0) {
          error(
              remarks,
              transformMeta,
              "PgVectorSearch.Validation.FilterFieldNotFound",
              filter.getStreamField());
        }
      }
    }
    String resolvedMinScore = variables.resolve(minScore);
    if (!StringUtil.containsVariableToken(resolvedMinScore)
        && Const.toDouble(resolvedMinScore, 0.0) > 0) {
      warning(remarks, transformMeta, "PgVectorSearch.Validation.MinScoreAfterTopK");
    }
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
