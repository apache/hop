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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.core.data.GraphVectors;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.shared.NeoHopData;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Searches a vector index of a graph database for the vector of every input row. */
public class GraphVectorSearch extends BaseTransform<GraphVectorSearchMeta, GraphVectorSearchData> {

  private static final Class<?> PKG = GraphVectorSearchMeta.class;

  public GraphVectorSearch(
      TransformMeta transformMeta,
      GraphVectorSearchMeta meta,
      GraphVectorSearchData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    try {
      if (Utils.isEmpty(meta.getConnection())) {
        throw new HopException(BaseMessages.getString(PKG, "GraphVectorSearch.Error.NoConnection"));
      }
      String topK = resolve(meta.getTopK());
      int k = Const.toInt(topK, -1);
      if (k < 1) {
        throw new HopException(BaseMessages.getString(PKG, "GraphVectorSearch.Error.TopK", topK));
      }
      String minScore = resolve(meta.getMinScore());
      if (!Utils.isEmpty(minScore)) {
        try {
          data.minScore = Double.parseDouble(minScore.trim());
        } catch (NumberFormatException e) {
          throw new HopException(
              BaseMessages.getString(PKG, "GraphVectorSearch.Error.MinScore", minScore));
        }
      }

      NamedGraphConnection graphConnection =
          NeoConnectionUtils.getGraphConnection(
              getMetadataProvider(), resolve(meta.getConnection()));
      IGraphDialect dialect = graphConnection.getDialect(this);
      if (!dialect.isSupportingVectorSearch()) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "GraphVectorSearch.Error.NotSupported",
                graphConnection.name(),
                dialect.getId()));
      }
      if (meta.isSearchingRelationships() && !dialect.isSupportingRelationshipVectorSearch()) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "GraphVectorSearch.Error.RelationshipsNotSupported",
                graphConnection.name(),
                dialect.getId()));
      }
      List<String> properties = new ArrayList<>();
      data.propertyValueMetas = new ArrayList<>();
      for (GraphVectorSearchProperty property : meta.getValidReturnProperties()) {
        properties.add(resolve(property.getProperty()));
        data.propertyValueMetas.add(GraphVectorSearchMeta.createValueMeta(property));
      }
      data.statement =
          dialect.getVectorSearchStatement(
              new GraphVectorSearchDefinition(
                  resolve(meta.getIndexName()),
                  resolve(meta.getLabel()),
                  resolve(meta.getVectorProperty()),
                  k,
                  properties,
                  meta.getSimilarity(),
                  meta.getElementType()));
      data.connection = graphConnection.connect(getLogChannel(), this);
      return true;
    } catch (HopException e) {
      logError(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, null, this, getMetadataProvider());
      data.embeddingFieldIndex = data.inputRowMeta.indexOfValue(resolve(meta.getEmbeddingField()));
      if (data.embeddingFieldIndex < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "GraphVectorSearch.Error.EmbeddingFieldNotFound", meta.getEmbeddingField()));
      }
    }

    try {
      search(row);
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(
            data.inputRowMeta,
            row,
            1,
            e.getMessage(),
            meta.getEmbeddingField(),
            "GRAPHVECTORSEARCH001");
      } else {
        throw e;
      }
    }
    return true;
  }

  private void search(Object[] row) throws HopException {
    IValueMeta embeddingMeta = data.inputRowMeta.getValueMeta(data.embeddingFieldIndex);
    List<Double> vector = GraphVectors.toList(embeddingMeta, row[data.embeddingFieldIndex]);
    if (vector == null || vector.isEmpty()) {
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG, "GraphVectorSearch.Log.EmptyEmbedding", meta.getEmbeddingField()));
      }
      handleNoMatch(row);
      return;
    }
    int matches = 0;
    for (Object[] hit : search(vector)) {
      Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      System.arraycopy(hit, 0, outputRow, data.inputRowMeta.size(), hit.length);
      putRow(data.outputRowMeta, outputRow);
      matches++;
    }
    if (matches == 0) {
      handleNoMatch(row);
    }
  }

  /**
   * The output values of the nodes or relationships found: the score, if there is a score field,
   * and the returned properties. The minimum score is applied after the top k, so fewer than k rows
   * can come back.
   */
  List<Object[]> search(List<Double> vector) throws HopException {
    Map<String, Object> parameters = new HashMap<>(data.statement.parameters());
    parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, vector);
    List<Map<String, Object>> results =
        data.connection.execute(data.statement.statement(), parameters);
    return toOutputValues(results, data.minScore, !Utils.isEmpty(meta.getScoreField()), data);
  }

  static List<Object[]> toOutputValues(
      List<Map<String, Object>> results,
      Double minScore,
      boolean withScore,
      GraphVectorSearchData data)
      throws HopException {
    List<Object[]> hits = new ArrayList<>();
    for (Map<String, Object> result : results) {
      Object scoreValue = result.get(GraphVectorSearchDefinition.COLUMN_SCORE);
      Double score = scoreValue instanceof Number number ? number.doubleValue() : null;
      if (minScore != null && (score == null || score < minScore)) {
        continue;
      }
      List<Object> values = new ArrayList<>();
      if (withScore) {
        values.add(score);
      }
      for (int i = 0; i < data.propertyValueMetas.size(); i++) {
        IValueMeta valueMeta = data.propertyValueMetas.get(i);
        values.add(
            NeoHopData.convertToHopValue(
                valueMeta.getName(),
                result.get(GraphVectorSearchDefinition.getPropertyColumn(i)),
                valueMeta));
      }
      hits.add(values.toArray());
    }
    return hits;
  }

  /** The input row with empty search fields, unless rows without match are eaten. */
  private void handleNoMatch(Object[] row) throws HopException {
    if (meta.isEatingRowOnNoMatch()) {
      return;
    }
    putRow(data.outputRowMeta, RowDataUtil.createResizedCopy(row, data.outputRowMeta.size()));
  }

  @Override
  public void dispose() {
    if (data.connection != null) {
      try {
        data.connection.close();
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "GraphVectorSearch.Error.Closing"), e);
      }
      data.connection = null;
    }
    super.dispose();
  }
}
