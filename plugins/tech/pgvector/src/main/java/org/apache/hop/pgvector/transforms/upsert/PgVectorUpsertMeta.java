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
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
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
public class PgVectorUpsertMeta extends BaseTransformMeta<PgVectorUpsert, PgVectorUpsertData> {

  private static final Class<?> PKG = PgVectorUpsertMeta.class;

  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(key = "schemaName")
  private String schemaName = "public";

  @HopMetadataProperty(key = "tableName")
  private String tableName = "hop_rag_chunks";

  @HopMetadataProperty(key = "idField")
  private String idField;

  @HopMetadataProperty(key = "documentIdField")
  private String documentIdField = "document_id";

  @HopMetadataProperty(key = "chunkIndexField")
  private String chunkIndexField = "chunk_index";

  @HopMetadataProperty(key = "contentField")
  private String contentField = "chunk_text";

  @HopMetadataProperty(key = "embeddingField")
  private String embeddingField = "embedding";

  @HopMetadataProperty(key = "embeddingDimensions")
  private int embeddingDimensions = 768;

  @HopMetadataProperty(key = "createTableIfMissing")
  private boolean createTableIfMissing = true;

  @HopMetadataProperty(key = "createHnswIndex")
  private boolean createHnswIndex = true;

  @HopMetadataProperty(key = "deleteDocumentBeforeUpsert")
  private boolean deleteDocumentBeforeUpsert = false;

  @HopMetadataProperty(key = "commitSize")
  private int commitSize = 100;

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
    embeddingDimensions = 768;
    createTableIfMissing = true;
    createHnswIndex = true;
    deleteDocumentBeforeUpsert = false;
    commitSize = 100;
    indexMetric = VectorDistanceMetric.COSINE;
    columnMappings = new ArrayList<>();
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

    if (embeddingDimensions <= 0) {
      error(remarks, transformMeta, "PgVectorUpsert.Validation.DimensionsPositive");
    }
    if (commitSize < 0) {
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

    if (createTableIfMissing) {
      warning(remarks, transformMeta, "PgVectorUpsert.Validation.CreateExtensionPrivileges");
      if (transformMeta != null && transformMeta.getCopies(variables) > 1) {
        error(remarks, transformMeta, "PgVectorUpsert.Validation.DdlWithMultipleCopies");
      }
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
