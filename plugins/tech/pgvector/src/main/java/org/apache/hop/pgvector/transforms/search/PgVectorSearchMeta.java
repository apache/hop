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
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
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
public class PgVectorSearchMeta extends BaseTransformMeta<PgVectorSearch, PgVectorSearchData> {

  private static final Class<?> PKG = PgVectorSearchMeta.class;

  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(key = "schemaName")
  private String schemaName = "public";

  @HopMetadataProperty(key = "tableName")
  private String tableName = "hop_rag_chunks";

  @HopMetadataProperty(key = "embeddingField")
  private String embeddingField = "embedding";

  @HopMetadataProperty(key = "topK")
  private int topK = 5;

  @HopMetadataProperty(key = "distanceMetric")
  private VectorDistanceMetric distanceMetric = VectorDistanceMetric.COSINE;

  @HopMetadataProperty(key = "minScore")
  private double minScore = 0.0;

  /**
   * Have the search eat the incoming row when the query returns nothing. Mirrors the option on
   * Hop's Database Lookup: off by default, so a row that finds no match still reaches the output
   * with empty match fields rather than disappearing.
   */
  @HopMetadataProperty(
      key = "eat_row_on_no_match",
      injectionKey = "EAT_ROW_ON_NO_MATCH",
      injectionKeyDescription = "PgVectorSearchMeta.Injection.EAT_ROW_ON_NO_MATCH")
  private boolean eatingRowOnNoMatch;

  @HopMetadataProperty(key = "resultIdField")
  private String resultIdField = "match_id";

  @HopMetadataProperty(key = "resultDocumentIdField")
  private String resultDocumentIdField = "match_document_id";

  @HopMetadataProperty(key = "resultChunkIndexField")
  private String resultChunkIndexField = "match_chunk_index";

  @HopMetadataProperty(key = "resultContentField")
  private String resultContentField = "match_content";

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
        copy.filters.add(new PgVectorSearchFilter(filter.getColumnName(), filter.getStreamField()));
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
    topK = 5;
    distanceMetric = VectorDistanceMetric.COSINE;
    minScore = 0.0;
    eatingRowOnNoMatch = false;
    resultIdField = "match_id";
    resultDocumentIdField = "match_document_id";
    resultChunkIndexField = "match_chunk_index";
    resultContentField = "match_content";
    resultScoreField = "match_score";
    filters = new ArrayList<>();
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
    if (topK < 1) {
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
    if (minScore > 0) {
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
