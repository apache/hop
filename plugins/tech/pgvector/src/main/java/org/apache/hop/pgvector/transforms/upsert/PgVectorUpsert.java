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

import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pgvector.util.EmbeddingJsonParser;
import org.apache.hop.pgvector.util.PgVectorColumnMapping;
import org.apache.hop.pgvector.util.PgVectorColumnType;
import org.apache.hop.pgvector.util.PgVectorDatabase;
import org.apache.hop.pgvector.util.PgVectorSchemaBuilder;
import org.apache.hop.pgvector.util.PgVectorSqlBuilder;
import org.apache.hop.pgvector.util.PgVectorTableColumn;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Upserts chunk rows with embeddings into a PostgreSQL pgvector table. */
public class PgVectorUpsert extends BaseTransform<PgVectorUpsertMeta, PgVectorUpsertData> {

  private static final Class<?> PKG = PgVectorUpsertMeta.class;

  /**
   * Upper bound on the per-document delete cache. Without a cap this set grows for the lifetime of
   * the run, which matters when a corpus has millions of distinct documents.
   */
  private static final int MAX_DELETED_DOCUMENT_CACHE = 100_000;

  public PgVectorUpsert(
      TransformMeta transformMeta,
      PgVectorUpsertMeta meta,
      PgVectorUpsertData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (Utils.isEmpty(meta.getConnection())) {
      logError(BaseMessages.getString(PKG, "PgVectorUpsert.Validation.ConnectionRequired"));
      return false;
    }
    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      commitBatch();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      openDatabase();
      resolveFieldIndices();
    }

    try {
      addRowToBatch(row);
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(data.inputRowMeta, row, 1, e.getMessage(), null, "PGVECTORUPSERT001");
      } else {
        throw e;
      }
    }

    return true;
  }

  private void addRowToBatch(Object[] row) throws HopException {
    String documentId = getFieldAsString(row, data.documentIdFieldIndex);
    String content = getFieldAsString(row, data.contentFieldIndex);
    // Taken as the raw value: a field of the Vector value type is already a float[], and
    // rendering it to text here only to parse it back would be wasted work on every row.
    Object embedding = row[data.embeddingFieldIndex];

    if (Utils.isEmpty(content) || EmbeddingJsonParser.isEmpty(embedding)) {
      // Nothing to store for this row, but it still belongs in the output stream.
      putRow(data.inputRowMeta, row);
      return;
    }

    // The delete has to be flushed together with the inserts it makes room for. Committing the
    // pending batch first keeps "delete document, then re-insert its chunks" from being split
    // across a failure boundary, which would otherwise leave the document deleted and not
    // replaced.
    if (shouldDeleteDocument(documentId)) {
      commitBatch();
      deleteDocument(documentId);
    }

    String id = resolveId(row);
    String chunkIndex = getFieldAsString(row, data.chunkIndexFieldIndex);

    try {
      bindInsertRow(row, id, documentId, chunkIndex, content, embedding);
      data.insertStatement.addBatch();
      data.batchRows.add(row);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorUpsert.Error.BindingRow", String.valueOf(id)), e);
    }

    if (meta.getCommitSize() > 0 && data.batchRows.size() >= meta.getCommitSize()) {
      commitBatch();
    }
  }

  private boolean shouldDeleteDocument(String documentId) {
    return meta.isDeleteDocumentBeforeUpsert()
        && data.deleteStatement != null
        && !Utils.isEmpty(documentId)
        && !data.deletedDocuments.contains(documentId);
  }

  private void deleteDocument(String documentId) throws HopException {
    try {
      data.deleteStatement.setString(1, documentId);
      data.deleteStatement.executeUpdate();
      if (data.deletedDocuments.size() >= MAX_DELETED_DOCUMENT_CACHE) {
        Iterator<String> oldest = data.deletedDocuments.iterator();
        oldest.next();
        oldest.remove();
      }
      data.deletedDocuments.add(documentId);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorUpsert.Error.DeletingDocument", documentId), e);
    }
  }

  private void bindInsertRow(
      Object[] row,
      String id,
      String documentId,
      String chunkIndex,
      String content,
      Object embedding)
      throws Exception {
    String vectorLiteral = EmbeddingJsonParser.toPgVectorLiteral(embedding);
    for (PgVectorUpsertData.MappingBinding binding : data.mappingBindings) {
      Object value =
          switch (binding.column.name()) {
            case "id" -> id;
            case "document_id" -> documentId;
            case "chunk_index" -> parseChunkIndex(chunkIndex);
            case "content" -> content;
            case "embedding" -> vectorLiteral;
            default -> getFieldAsString(row, binding.streamFieldIndex);
          };
      if (binding.column.type() == PgVectorColumnType.INTEGER) {
        data.insertStatement.setInt(binding.statementIndex, (Integer) value);
      } else {
        data.insertStatement.setString(
            binding.statementIndex, value == null ? null : value.toString());
      }
    }
  }

  private void openDatabase() throws HopException {
    try {
      data.database =
          PgVectorDatabase.connect(this, this, getMetadataProvider(), meta.getConnection());
    } catch (HopException e) {
      throw e;
    }

    try {
      data.database.setCommit(meta.getCommitSize());
      VectorDistanceMetric indexMetric =
          meta.getIndexMetric() != null ? meta.getIndexMetric() : VectorDistanceMetric.COSINE;
      String schemaName = resolve(meta.getSchemaName());
      String tableName = resolve(meta.getTableName());
      if (meta.isCreateTableIfMissing()) {
        PgVectorDatabase.ensureSchema(data.database, meta, schemaName, tableName, indexMetric);
      }
      String qualifiedTable = PgVectorSqlBuilder.qualifiedTable(schemaName, tableName);
      data.tableColumns = PgVectorSchemaBuilder.tableColumns(meta);
      data.insertStatement =
          data.database
              .getConnection()
              .prepareStatement(PgVectorSqlBuilder.upsertSql(qualifiedTable, data.tableColumns));
      if (meta.isDeleteDocumentBeforeUpsert()) {
        data.deleteStatement =
            data.database
                .getConnection()
                .prepareStatement(PgVectorSqlBuilder.deleteByDocumentIdSql(qualifiedTable));
      }
    } catch (Exception e) {
      // Do not leak the connection when the statements cannot be prepared.
      closeDatabase();
      throw new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Initializing"), e);
    }
  }

  private void resolveFieldIndices() throws HopException {
    data.contentFieldIndex = requireFieldIndex(meta.getContentField());
    data.embeddingFieldIndex = requireFieldIndex(meta.getEmbeddingField());
    data.documentIdFieldIndex = optionalFieldIndex(meta.getDocumentIdField());
    data.chunkIndexFieldIndex = optionalFieldIndex(meta.getChunkIndexField());
    if (!Utils.isEmpty(meta.getIdField())) {
      data.idFieldIndex = requireFieldIndex(meta.getIdField());
    }

    data.mappingBindings = new ArrayList<>();
    int parameterIndex = 1;
    for (PgVectorTableColumn column : data.tableColumns) {
      int streamIndex = resolveStreamIndex(column.name());
      data.mappingBindings.add(
          new PgVectorUpsertData.MappingBinding(streamIndex, parameterIndex++, column));
    }
  }

  private int resolveStreamIndex(String columnName) throws HopException {
    return switch (columnName) {
      case "id" -> data.idFieldIndex;
      case "document_id" -> data.documentIdFieldIndex;
      case "chunk_index" -> data.chunkIndexFieldIndex;
      case "content" -> data.contentFieldIndex;
      case "embedding" -> data.embeddingFieldIndex;
      default -> requireMappedFieldIndex(columnName);
    };
  }

  private int requireMappedFieldIndex(String columnName) throws HopException {
    if (meta.getColumnMappings() != null) {
      for (PgVectorColumnMapping mapping : meta.getColumnMappings()) {
        if (mapping != null
            && columnName.equals(
                PgVectorSchemaBuilder.normalizeColumnName(mapping.getColumnName()))) {
          return requireFieldIndex(mapping.getStreamField());
        }
      }
    }
    throw new HopException(
        BaseMessages.getString(PKG, "PgVectorUpsert.Error.NoMappingForColumn", columnName));
  }

  private int requireFieldIndex(String fieldName) throws HopException {
    int index = data.inputRowMeta.indexOfValue(fieldName);
    if (index < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "PgVectorUpsert.Error.FieldNotFound", String.valueOf(fieldName)));
    }
    return index;
  }

  private int optionalFieldIndex(String fieldName) {
    if (Utils.isEmpty(fieldName)) {
      return -1;
    }
    return data.inputRowMeta.indexOfValue(fieldName);
  }

  private String resolveId(Object[] row) throws HopException {
    if (data.idFieldIndex >= 0) {
      return getFieldAsString(row, data.idFieldIndex);
    }
    String documentId = getFieldAsString(row, data.documentIdFieldIndex);
    String chunkIndex = getFieldAsString(row, data.chunkIndexFieldIndex);
    if (Utils.isEmpty(documentId) && Utils.isEmpty(chunkIndex)) {
      throw new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.NoIdAvailable"));
    }
    return documentId + "_" + chunkIndex;
  }

  private String getFieldAsString(Object[] row, int index) throws HopException {
    if (index < 0) {
      return null;
    }
    return data.inputRowMeta.getString(row, index);
  }

  private int parseChunkIndex(String chunkIndex) throws HopException {
    if (Utils.isEmpty(chunkIndex)) {
      return 0;
    }
    try {
      return Integer.parseInt(chunkIndex.trim());
    } catch (NumberFormatException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorUpsert.Error.InvalidChunkIndex", chunkIndex), e);
    }
  }

  /**
   * Executes and commits the pending batch, then releases the buffered rows downstream. When error
   * handling is on, {@link BatchUpdateException#getUpdateCounts()} is used to divert only the rows
   * the database actually rejected.
   */
  private void commitBatch() throws HopException {
    if (data.insertStatement == null || data.batchRows.isEmpty()) {
      return;
    }

    List<Object[]> rows = new ArrayList<>(data.batchRows);
    data.batchRows.clear();

    boolean[] failed = new boolean[rows.size()];
    HopException failure = null;

    try {
      data.insertStatement.executeBatch();
    } catch (BatchUpdateException e) {
      failure = new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Committing"), e);
      markFailedRows(e, failed);
    } catch (Exception e) {
      failure = new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Committing"), e);
      java.util.Arrays.fill(failed, true);
    }

    if (failure != null && !getTransformMeta().isDoingErrorHandling()) {
      throw failure;
    }

    try {
      if (meta.getCommitSize() > 0) {
        data.database.commit();
      }
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Committing"), e);
    }

    String errorMessage = failure != null ? failure.getMessage() : null;
    for (int i = 0; i < rows.size(); i++) {
      if (failed[i]) {
        putError(data.inputRowMeta, rows.get(i), 1, errorMessage, null, "PGVECTORUPSERT002");
      } else {
        putRow(data.inputRowMeta, rows.get(i));
      }
    }
  }

  /**
   * Maps a batch failure back onto individual rows. Drivers may report fewer counts than statements
   * and use {@link Statement#EXECUTE_FAILED}; anything not positively reported as succeeding is
   * treated as failed.
   */
  private static void markFailedRows(BatchUpdateException e, boolean[] failed) {
    int[] counts = e.getUpdateCounts();
    if (counts == null) {
      java.util.Arrays.fill(failed, true);
      return;
    }
    for (int i = 0; i < failed.length; i++) {
      failed[i] = i >= counts.length || counts[i] == Statement.EXECUTE_FAILED;
    }
  }

  private void closeDatabase() {
    if (data.insertStatement != null) {
      try {
        data.insertStatement.close();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "PgVectorUpsert.Error.ClosingStatement"), e);
      }
      data.insertStatement = null;
    }
    if (data.deleteStatement != null) {
      try {
        data.deleteStatement.close();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "PgVectorUpsert.Error.ClosingStatement"), e);
      }
      data.deleteStatement = null;
    }
    if (data.database != null) {
      data.database.disconnect();
      data.database = null;
    }
  }

  @Override
  public void dispose() {
    // Only commit on a clean end of stream. On a stop or a failure the pending batch is rolled
    // back with the connection instead of being silently written.
    if (isStopped() || getErrors() > 0) {
      data.batchRows.clear();
      if (data.database != null) {
        try {
          data.database.rollback();
        } catch (Exception e) {
          logDebug(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Rollback"), e);
        }
      }
    } else {
      try {
        commitBatch();
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Committing"), e);
        setErrors(getErrors() + 1);
      }
    }
    closeDatabase();
    super.dispose();
  }
}
