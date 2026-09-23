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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pgvector.util.EmbeddingJsonParser;
import org.apache.hop.pgvector.util.PgVectorDatabase;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
import org.apache.hop.pgvector.util.PgVectorSqlBuilder;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Runs top-k similarity search against a PostgreSQL pgvector table. */
public class PgVectorSearch extends BaseTransform<PgVectorSearchMeta, PgVectorSearchData> {

  private static final Class<?> PKG = PgVectorSearchMeta.class;

  public PgVectorSearch(
      TransformMeta transformMeta,
      PgVectorSearchMeta meta,
      PgVectorSearchData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (Utils.isEmpty(meta.getConnection())) {
      logError(BaseMessages.getString(PKG, "PgVectorSearch.Validation.ConnectionRequired"));
      return false;
    }
    data.topK = Const.toInt(resolve(meta.getTopK()), -1);
    if (data.topK <= 0) {
      logError(BaseMessages.getString(PKG, "PgVectorSearch.Validation.TopKPositive"));
      return false;
    }
    data.minScore = Const.toDouble(resolve(meta.getMinScore()), 0.0);
    return super.init();
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
      resolveFieldIndices();
      resolveFilterBindings();
      openDatabase();
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
            "PGVECTORSEARCH001");
      } else {
        throw e;
      }
    }

    return true;
  }

  private void search(Object[] row) throws HopException {
    // Taken as the raw value: a field of the Vector value type is already a float[].
    Object embedding = row[data.embeddingFieldIndex];
    if (EmbeddingJsonParser.isEmpty(embedding)) {
      // No query vector to search with. Handled like an empty result rather than as a failure,
      // so a stream that happens to carry a few unembedded rows does not stop the pipeline.
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG, "PgVectorSearch.Log.EmptyEmbedding", meta.getEmbeddingField()));
      }
      handleNoMatch(row);
      return;
    }

    int matches = 0;
    try {
      bindSearchParameters(row, embedding);
      try (ResultSet resultSet = data.searchStatement.executeQuery()) {
        while (resultSet.next()) {
          double score = resultSet.getDouble("similarity");
          // The minimum score is applied after the top-k limit, so a run can legitimately
          // return fewer than k rows. Filtering inside the query would defeat the ANN index.
          if (score < data.minScore) {
            continue;
          }
          Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
          setField(outputRow, data.resultIdFieldIndex, resultSet.getString("id"));
          setField(outputRow, data.resultDocumentIdFieldIndex, resultSet.getString("document_id"));
          setField(outputRow, data.resultChunkIndexFieldIndex, readChunkIndex(resultSet));
          setField(outputRow, data.resultContentFieldIndex, resultSet.getString("content"));
          setField(outputRow, data.resultScoreFieldIndex, score);
          putRow(data.outputRowMeta, outputRow);
          matches++;
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "PgVectorSearch.Error.Searching"), e);
    }

    if (matches == 0) {
      handleNoMatch(row);
    }
  }

  /**
   * Emits the input row with empty match fields, unless the transform is configured to drop rows
   * that find nothing.
   */
  private void handleNoMatch(Object[] row) throws HopException {
    if (meta.isEatingRowOnNoMatch()) {
      return;
    }
    putRow(data.outputRowMeta, RowDataUtil.createResizedCopy(row, data.outputRowMeta.size()));
  }

  private static Long readChunkIndex(ResultSet resultSet) throws java.sql.SQLException {
    long value = resultSet.getLong("chunk_index");
    return resultSet.wasNull() ? null : value;
  }

  private void resolveFieldIndices() throws HopException {
    data.embeddingFieldIndex = data.inputRowMeta.indexOfValue(meta.getEmbeddingField());
    if (data.embeddingFieldIndex < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "PgVectorSearch.Validation.EmbeddingFieldNotFound", meta.getEmbeddingField()));
    }
    data.resultIdFieldIndex = data.outputRowMeta.indexOfValue(meta.getResultIdField());
    data.resultDocumentIdFieldIndex =
        data.outputRowMeta.indexOfValue(meta.getResultDocumentIdField());
    data.resultChunkIndexFieldIndex =
        data.outputRowMeta.indexOfValue(meta.getResultChunkIndexField());
    data.resultContentFieldIndex = data.outputRowMeta.indexOfValue(meta.getResultContentField());
    data.resultScoreFieldIndex = data.outputRowMeta.indexOfValue(meta.getResultScoreField());
  }

  private void bindSearchParameters(Object[] row, Object embedding) throws Exception {
    String vectorLiteral = EmbeddingJsonParser.toPgVectorLiteral(embedding);
    int parameterIndex = 1;
    // Placeholder order must match searchSql(): score expression, filters, ORDER BY, LIMIT.
    data.searchStatement.setString(parameterIndex++, vectorLiteral);
    for (PgVectorSearchData.FilterBinding binding : data.filterBindings) {
      String filterValue = data.inputRowMeta.getString(row, binding.streamFieldIndex);
      data.searchStatement.setString(parameterIndex++, filterValue);
    }
    data.searchStatement.setString(parameterIndex++, vectorLiteral);
    data.searchStatement.setInt(parameterIndex, data.topK);
  }

  private void resolveFilterBindings() throws HopException {
    data.filterBindings = new ArrayList<>();
    if (meta.getFilters() == null) {
      return;
    }
    for (PgVectorSearchFilter filter : meta.getFilters()) {
      if (filter == null
          || Utils.isEmpty(filter.getColumnName())
          || Utils.isEmpty(filter.getStreamField())) {
        continue;
      }
      int index = data.inputRowMeta.indexOfValue(filter.getStreamField());
      if (index < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "PgVectorSearch.Validation.FilterFieldNotFound", filter.getStreamField()));
      }
      data.filterBindings.add(new PgVectorSearchData.FilterBinding(index, filter));
    }
  }

  private void openDatabase() throws HopException {
    try {
      data.database =
          PgVectorDatabase.connect(this, this, getMetadataProvider(), meta.getConnection());
      VectorDistanceMetric metric =
          meta.getDistanceMetric() != null ? meta.getDistanceMetric() : VectorDistanceMetric.COSINE;
      String qualifiedTable =
          PgVectorSqlBuilder.qualifiedTable(
              resolve(meta.getSchemaName()), resolve(meta.getTableName()));
      List<PgVectorSearchFilter> activeFilters =
          data.filterBindings.stream().map(b -> b.filter).toList();
      data.searchStatement =
          data.database
              .getConnection()
              .prepareStatement(
                  PgVectorSqlBuilder.searchSql(qualifiedTable, metric, activeFilters));
    } catch (Exception e) {
      closeDatabase();
      throw new HopException(BaseMessages.getString(PKG, "PgVectorSearch.Error.Initializing"), e);
    }
  }

  private static void setField(Object[] row, int index, Object value) {
    if (index >= 0 && index < row.length) {
      row[index] = value;
    }
  }

  private void closeDatabase() {
    if (data.searchStatement != null) {
      try {
        data.searchStatement.close();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "PgVectorSearch.Error.ClosingStatement"), e);
      }
      data.searchStatement = null;
    }
    if (data.database != null) {
      data.database.disconnect();
      data.database = null;
    }
  }

  @Override
  public void dispose() {
    closeDatabase();
    super.dispose();
  }
}
