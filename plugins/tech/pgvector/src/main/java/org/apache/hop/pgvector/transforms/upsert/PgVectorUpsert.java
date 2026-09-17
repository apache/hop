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
import org.apache.hop.core.Const;
import org.apache.hop.core.database.IDatabase;
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
   * Upper bound on the per-document delete cache, which holds one entry per distinct document id so
   * that each document is deleted once. The cap exists because the set would otherwise grow for the
   * lifetime of the run. It is a hard limit rather than an eviction policy: evicting an entry would
   * let the same document be deleted a second time, taking with it the chunks this run had already
   * written for it.
   */
  private static final int MAX_DELETED_DOCUMENT_CACHE = 1_000_000;

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
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorUpsert.Error.BindingRow", String.valueOf(id)), e);
    }

    if (!data.batchMode) {
      executeSingleRow(row, id);
      return;
    }

    try {
      data.insertStatement.addBatch();
      data.batchRows.add(row);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorUpsert.Error.BindingRow", String.valueOf(id)), e);
    }

    if (data.batchRows.size() >= data.commitSize) {
      commitBatch();
    }
  }

  /** One statement at a time, so a rejected row can be diverted without losing the rest. */
  private void executeSingleRow(Object[] row, String id) throws HopException {
    try {
      if (data.useSafePoints) {
        data.savepoint = data.database.setSavepoint();
      }
      data.insertStatement.executeUpdate();
      if (data.useSafePoints && data.releaseSavepoint) {
        data.database.releaseSavepoint(data.savepoint);
      }
    } catch (Exception e) {
      if (!getTransformMeta().isDoingErrorHandling()) {
        rollbackQuietly();
        throw new HopException(
            BaseMessages.getString(PKG, "PgVectorUpsert.Error.BindingRow", String.valueOf(id)), e);
      }
      // Without this the transaction stays aborted and every later row fails too.
      revertToSavepoint();
      putError(data.inputRowMeta, row, 1, e.getMessage(), null, "PGVECTORUPSERT002");
      return;
    }
    putRow(data.inputRowMeta, row);
    commitIfCommitSizeReached();
  }

  /** Always called while handling another failure, so it reports rather than throws. */
  private void revertToSavepoint() {
    if (!data.useSafePoints || data.savepoint == null) {
      return;
    }
    try {
      data.database.rollback(data.savepoint);
      if (data.releaseSavepoint) {
        data.database.releaseSavepoint(data.savepoint);
      }
    } catch (Exception e) {
      logDetailed("Unable to roll back to the savepoint: " + e.getMessage());
    }
  }

  /**
   * Commits every commit-size rows outside batch mode, the way Table Output does. A commit size of
   * 0 resolves to {@link Integer#MAX_VALUE}, which leaves the run as a single transaction.
   */
  private void commitIfCommitSizeReached() throws HopException {
    if (++data.rowsSinceCommit < data.commitSize) {
      return;
    }
    data.rowsSinceCommit = 0;
    try {
      data.database.commit();
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Committing"), e);
    }
  }

  private void rollbackQuietly() {
    try {
      data.database.rollback();
    } catch (Exception e) {
      logDetailed("Unable to roll back the aborted transaction: " + e.getMessage());
    }
  }

  private boolean hasColumnMappings() {
    return meta.getColumnMappings() != null && !meta.getColumnMappings().isEmpty();
  }

  private boolean shouldDeleteDocument(String documentId) {
    return meta.isDeleteDocumentBeforeUpsert()
        && data.deleteStatement != null
        && !Utils.isEmpty(documentId)
        && !data.deletedDocuments.contains(documentId);
  }

  private void deleteDocument(String documentId) throws HopException {
    if (data.deletedDocuments.size() >= MAX_DELETED_DOCUMENT_CACHE) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "PgVectorUpsert.Error.TooManyDeletedDocuments",
              String.valueOf(MAX_DELETED_DOCUMENT_CACHE)));
    }
    try {
      if (data.useSafePoints) {
        data.savepoint = data.database.setSavepoint();
      }
      data.deleteStatement.setString(1, documentId);
      data.deleteStatement.executeUpdate();
      if (data.useSafePoints && data.releaseSavepoint) {
        data.database.releaseSavepoint(data.savepoint);
      }
      data.deletedDocuments.add(documentId);
    } catch (Exception e) {
      // A failed delete aborts the transaction just as a failed insert does. Without this the
      // caller diverts this row and every row after it fails on the aborted transaction.
      revertToSavepoint();
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

  /**
   * Decides how rows reach the database: in JDBC batches, or one statement at a time with
   * savepoints so a rejected row can be diverted.
   *
   * <p>Batching and an error hop are mutually exclusive. A failed {@code executeBatch} cannot be
   * mapped back onto individual rows here, so a rejected batch would leave its rows neither written
   * nor diverted. That holds for every dialect, which is why it does not consult {@code iDatabase},
   * unlike the savepoints, which only some databases need.
   */
  void configureCommitStrategy(IDatabase iDatabase) {
    data.commitSize = Const.toInt(resolve(meta.getCommitSize()), 100);
    boolean errorHandling = getTransformMeta().isDoingErrorHandling();
    data.batchMode = data.commitSize > 0 && !errorHandling;
    // PostgreSQL aborts the whole transaction on a failed statement, so a row can only be diverted
    // if the transform can roll back to just before it. Table Output does the same.
    data.useSafePoints = iDatabase.isUseSafePoints() && errorHandling;
    data.releaseSavepoint = iDatabase.isReleaseSavepoint();
    if (data.commitSize <= 0) {
      // As in Table Output: one transaction for the whole run rather than JDBC autocommit, which
      // would split a document delete from the inserts that replace it.
      data.commitSize = Integer.MAX_VALUE;
    }
  }

  /**
   * Runs the schema DDL, then leaves the connection in the commit mode the data rows need.
   *
   * <p>The DDL runs with autocommit on. Inside the data transaction a table or index created here
   * would be undone by the rollback of a single failed row, and an index would hold its locks for
   * the length of the load.
   */
  void prepareSchemaAndCommitMode(String schemaName, String tableName) throws HopException {
    // The HNSW index and the mapped-column ALTERs are independent options, so they have to run on
    // an existing table too, not only when this transform creates one.
    if (meta.isCreateTableIfMissing() || meta.isCreateHnswIndex() || hasColumnMappings()) {
      data.database.setCommit(0);
      VectorDistanceMetric indexMetric =
          meta.getIndexMetric() != null ? meta.getIndexMetric() : VectorDistanceMetric.COSINE;
      PgVectorDatabase.ensureSchema(
          data.database,
          meta,
          schemaName,
          tableName,
          indexMetric,
          Const.toInt(resolve(meta.getEmbeddingDimensions()), 768));
    }
    data.database.setCommit(data.commitSize);
  }

  private void openDatabase() throws HopException {
    data.database =
        PgVectorDatabase.connect(this, this, getMetadataProvider(), meta.getConnection());

    try {
      configureCommitStrategy(data.database.getDatabaseMeta().getIDatabase());
      String schemaName = resolve(meta.getSchemaName());
      String tableName = resolve(meta.getTableName());
      prepareSchemaAndCommitMode(schemaName, tableName);
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
    // A chunk index on its own repeats across documents, so it cannot stand in for a key. The
    // document id is the part that has to be there; a missing chunk index is read as 0 below.
    if (Utils.isEmpty(documentId)) {
      throw new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.NoIdAvailable"));
    }
    // parseChunkIndex maps a missing value to 0, which is what gets bound to chunk_index, so the
    // key has to be built from that and not from the raw string. Otherwise a null index produces
    // the key "doc_null" beside a stored chunk_index of 0.
    return documentId + "_" + parseChunkIndex(chunkIndex);
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
   * Executes and commits the pending batch, then releases the buffered rows downstream so that no
   * downstream transform sees a row as written before it actually is. Batching is only used when no
   * error hop is attached, so a failure here aborts the transform rather than diverting rows.
   */
  private void commitBatch() throws HopException {
    if (data.insertStatement == null || data.batchRows.isEmpty()) {
      return;
    }

    List<Object[]> rows = new ArrayList<>(data.batchRows);
    data.batchRows.clear();

    try {
      data.insertStatement.executeBatch();
      data.database.commit();
    } catch (Exception e) {
      // Batching is only used when no error hop is attached, so there is nothing to divert to.
      // The transaction is already aborted by PostgreSQL: discard the batch and roll back rather
      // than committing on top of a failure.
      discardFailedBatch();
      throw new HopException(BaseMessages.getString(PKG, "PgVectorUpsert.Error.Committing"), e);
    }

    for (Object[] row : rows) {
      putRow(data.inputRowMeta, row);
    }
  }

  private void discardFailedBatch() {
    try {
      data.insertStatement.clearBatch();
    } catch (Exception e) {
      logDetailed("Unable to clear the failed batch: " + e.getMessage());
    }
    rollbackQuietly();
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
