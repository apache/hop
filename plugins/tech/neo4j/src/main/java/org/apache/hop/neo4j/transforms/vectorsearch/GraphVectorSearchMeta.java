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

package org.apache.hop.neo4j.transforms.vectorsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.shared.GraphConnectionSelectionLine;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Searches a vector index of a graph database for the nodes nearest to the vector of every input
 * row: one output row per node found, with the similarity score and the properties asked for.
 */
@Transform(
    id = "GraphVectorSearch",
    name = "i18n::GraphVectorSearch.Name",
    description = "i18n::GraphVectorSearch.Description",
    image = "graph_vector_search.svg",
    categoryDescription = "Graph",
    keywords = "i18n::GraphVectorSearch.Keywords",
    documentationUrl = "/pipeline/transforms/graph-vector-search.html")
@GuiPlugin
@Getter
@Setter
public class GraphVectorSearchMeta
    extends BaseTransformMeta<GraphVectorSearch, GraphVectorSearchData> {

  private static final Class<?> PKG = GraphVectorSearchMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "GRAPH_VECTOR_SEARCH_DIALOG_OPTIONS";
  public static final String WIDGET_EMBEDDING_FIELD = "GRAPH_VECTOR_SEARCH_EMBEDDING_FIELD";

  private static final String TAB_SEARCH = "i18n::GraphVectorSearch.Tab.Search";
  private static final String TAB_SEARCH_ORDER = "0100";
  private static final String TAB_OUTPUT = "i18n::GraphVectorSearch.Tab.Output";
  private static final String TAB_OUTPUT_ORDER = "0200";

  @GuiWidgetElement(
      id = "connection",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = GraphDatabaseMeta.class,
      metadataSelectionLine = GraphConnectionSelectionLine.class,
      label = "i18n::GraphVectorSearch.Connection.Label",
      toolTip = "i18n::GraphVectorSearch.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION",
      injectionKeyDescription = "GraphVectorSearch.Injection.CONNECTION")
  private String connection;

  /** Search the vector index of nodes or of relationships. Nodes by default. */
  @GuiWidgetElement(
      id = "elementType",
      order = "0150",
      type = GuiElementType.COMBO,
      label = "i18n::GraphVectorSearch.ElementType.Label",
      toolTip = "i18n::GraphVectorSearch.ElementType.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "element_type",
      injectionKey = "ELEMENT_TYPE",
      injectionKeyDescription = "GraphVectorSearch.Injection.ELEMENT_TYPE")
  private GraphObjectType elementType;

  @GuiWidgetElement(
      id = "indexName",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::GraphVectorSearch.IndexName.Label",
      toolTip = "i18n::GraphVectorSearch.IndexName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "index_name",
      injectionKey = "INDEX_NAME",
      injectionKeyDescription = "GraphVectorSearch.Injection.INDEX_NAME")
  private String indexName;

  @GuiWidgetElement(
      id = "label",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::GraphVectorSearch.Label.Label",
      toolTip = "i18n::GraphVectorSearch.Label.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "label",
      injectionKey = "LABEL",
      injectionKeyDescription = "GraphVectorSearch.Injection.LABEL")
  private String label;

  @GuiWidgetElement(
      id = "vectorProperty",
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::GraphVectorSearch.VectorProperty.Label",
      toolTip = "i18n::GraphVectorSearch.VectorProperty.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "vector_property",
      injectionKey = "VECTOR_PROPERTY",
      injectionKeyDescription = "GraphVectorSearch.Injection.VECTOR_PROPERTY")
  private String vectorProperty;

  @GuiWidgetElement(
      id = "similarity",
      order = "0500",
      type = GuiElementType.COMBO,
      label = "i18n::GraphVectorSearch.Similarity.Label",
      toolTip = "i18n::GraphVectorSearch.Similarity.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "similarity",
      injectionKey = "SIMILARITY",
      injectionKeyDescription = "GraphVectorSearch.Injection.SIMILARITY")
  private GraphVectorSimilarity similarity;

  @GuiWidgetElement(
      id = WIDGET_EMBEDDING_FIELD,
      order = "0600",
      type = GuiElementType.COMBO,
      label = "i18n::GraphVectorSearch.EmbeddingField.Label",
      toolTip = "i18n::GraphVectorSearch.EmbeddingField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "embedding_field",
      injectionKey = "EMBEDDING_FIELD",
      injectionKeyDescription = "GraphVectorSearch.Injection.EMBEDDING_FIELD")
  private String embeddingField;

  @GuiWidgetElement(
      id = "topK",
      order = "0700",
      type = GuiElementType.TEXT,
      label = "i18n::GraphVectorSearch.TopK.Label",
      toolTip = "i18n::GraphVectorSearch.TopK.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "top_k",
      injectionKey = "TOP_K",
      injectionKeyDescription = "GraphVectorSearch.Injection.TOP_K")
  private String topK;

  @GuiWidgetElement(
      id = "minScore",
      order = "0800",
      type = GuiElementType.TEXT,
      label = "i18n::GraphVectorSearch.MinScore.Label",
      toolTip = "i18n::GraphVectorSearch.MinScore.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "min_score",
      injectionKey = "MIN_SCORE",
      injectionKeyDescription = "GraphVectorSearch.Injection.MIN_SCORE")
  private String minScore;

  /**
   * Have the search eat the incoming row when the query returns nothing, like the option of
   * pgvector search: off by default, so a row which finds nothing still reaches the output.
   */
  @GuiWidgetElement(
      id = "eatingRowOnNoMatch",
      order = "0900",
      type = GuiElementType.CHECKBOX,
      label = "i18n::GraphVectorSearch.EatingRowOnNoMatch.Label",
      toolTip = "i18n::GraphVectorSearch.EatingRowOnNoMatch.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SEARCH,
      groupOrder = TAB_SEARCH_ORDER)
  @HopMetadataProperty(
      key = "eat_row_on_no_match",
      injectionKey = "EAT_ROW_ON_NO_MATCH",
      injectionKeyDescription = "GraphVectorSearch.Injection.EAT_ROW_ON_NO_MATCH")
  private boolean eatingRowOnNoMatch;

  @GuiWidgetElement(
      id = "scoreField",
      order = "0100",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GraphVectorSearch.ScoreField.Label",
      toolTip = "i18n::GraphVectorSearch.ScoreField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "score_field",
      injectionKey = "SCORE_FIELD",
      injectionKeyDescription = "GraphVectorSearch.Injection.SCORE_FIELD")
  private String scoreField;

  @GuiWidgetElement(
      id = "returnProperties",
      order = "0200",
      type = GuiElementType.TABLE,
      label = "i18n::GraphVectorSearch.ReturnProperties.Label",
      toolTip = "i18n::GraphVectorSearch.ReturnProperties.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER,
      tableRows = 6)
  @HopMetadataProperty(
      key = "return_property",
      groupKey = "return_properties",
      injectionGroupKey = "RETURN_PROPERTIES",
      injectionGroupDescription = "GraphVectorSearch.Injection.RETURN_PROPERTIES")
  private List<GraphVectorSearchProperty> returnProperties;

  public GraphVectorSearchMeta() {
    setDefault();
  }

  @Override
  public void setDefault() {
    connection = "";
    elementType = GraphObjectType.NODE;
    indexName = "";
    label = "";
    vectorProperty = "";
    similarity = GraphVectorSimilarity.COSINE;
    embeddingField = "embedding";
    topK = "5";
    minScore = "";
    eatingRowOnNoMatch = false;
    scoreField = "score";
    returnProperties = new ArrayList<>();
  }

  @Override
  public Object clone() {
    GraphVectorSearchMeta copy = (GraphVectorSearchMeta) super.clone();
    copy.returnProperties = new ArrayList<>();
    if (returnProperties != null) {
      returnProperties.forEach(p -> copy.returnProperties.add(new GraphVectorSearchProperty(p)));
    }
    return copy;
  }

  /** True to search the vector index of relationships, false for nodes. */
  public boolean isSearchingRelationships() {
    return elementType == GraphObjectType.RELATIONSHIP;
  }

  /** The value type names for the type column of the returned properties. */
  public List<String> getValueTypeNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return Arrays.asList(ValueMetaFactory.getValueMetaNames());
  }

  /** The returned properties which have a property name. */
  public List<GraphVectorSearchProperty> getValidReturnProperties() {
    List<GraphVectorSearchProperty> valid = new ArrayList<>();
    if (returnProperties != null) {
      for (GraphVectorSearchProperty property : returnProperties) {
        if (property != null && !Utils.isEmpty(property.getProperty())) {
          valid.add(property);
        }
      }
    }
    return valid;
  }

  /** The output field of a returned property: its field name, or the property name. */
  public static String getOutputFieldName(GraphVectorSearchProperty property) {
    return Utils.isEmpty(property.getFieldName())
        ? property.getProperty()
        : property.getFieldName();
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
      if (!Utils.isEmpty(scoreField)) {
        IValueMeta score = new ValueMetaNumber(scoreField);
        score.setOrigin(origin);
        row.addValueMeta(score);
      }
      for (GraphVectorSearchProperty property : getValidReturnProperties()) {
        IValueMeta valueMeta = createValueMeta(property);
        valueMeta.setOrigin(origin);
        row.addValueMeta(valueMeta);
      }
    } catch (HopPluginException e) {
      throw new HopTransformException("Error creating the output fields of the vector search", e);
    }
  }

  /** The value meta of the output field of a returned property, a String without type. */
  public static IValueMeta createValueMeta(GraphVectorSearchProperty property)
      throws HopPluginException {
    int type =
        Utils.isEmpty(property.getType())
            ? IValueMeta.TYPE_STRING
            : ValueMetaFactory.getIdForValueMeta(property.getType());
    if (type == IValueMeta.TYPE_NONE) {
      type = IValueMeta.TYPE_STRING;
    }
    return ValueMetaFactory.createValueMeta(getOutputFieldName(property), type);
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
      error(remarks, transformMeta, "GraphVectorSearch.Error.NoConnection");
    }
    if (Utils.isEmpty(indexName) && (Utils.isEmpty(label) || Utils.isEmpty(vectorProperty))) {
      error(remarks, transformMeta, "GraphVectorSearch.Error.NoIndex");
    }
    if (Utils.isEmpty(embeddingField)) {
      error(remarks, transformMeta, "GraphVectorSearch.Error.NoEmbeddingField");
    } else if (prev != null && prev.indexOfValue(embeddingField) < 0) {
      error(
          remarks, transformMeta, "GraphVectorSearch.Error.EmbeddingFieldNotFound", embeddingField);
    }
    String resolvedTopK = variables.resolve(topK);
    if (!StringUtil.containsVariableToken(resolvedTopK) && Const.toInt(resolvedTopK, -1) < 1) {
      error(remarks, transformMeta, "GraphVectorSearch.Error.TopK", resolvedTopK);
    }
    String resolvedMinScore = variables.resolve(minScore);
    if (!Utils.isEmpty(resolvedMinScore) && !StringUtil.containsVariableToken(resolvedMinScore)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "GraphVectorSearch.Warning.MinScoreAfterTopK"),
              transformMeta));
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
}
