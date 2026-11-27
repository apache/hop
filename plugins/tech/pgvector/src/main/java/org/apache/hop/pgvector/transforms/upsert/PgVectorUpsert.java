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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
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
  private static final String CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW =
      "PgVectorUpsert.Error.BindingRow";
  private static final String CONST_PG_VECTOR_UPSERT_ERROR_COMMITTING =
      "PgVectorUpsert.Error.Committing";

  private static final Class<?> PKG = PgVectorUpsertMeta.class;

  /**
   * A row that fails before its upsert reaches the database (no key, a chunk index or embedding
   * that can't be read, too many documents to track), or whose document delete fails.
   */
  private static final String CODE_BEFORE_THE_DATABASE = "PGVECTORUPSERT001";

  /** A row whose upsert the database refused: on its own, or in a recovered batch. */
  private static final String CODE_REFUSED = "PGVECTORUPSERT002";

  /** A row of a recovered batch that was never sent to the database, which could not be asked. */
  private static final String CODE_NOT_ATTEMPTED = "PGVECTORUPSERT003";

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
      if (data.recoveredBatches && !flushRecoveredBatchOrStop()) {
        setOutputDone();
        return false;
      }
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

    if (data.recoveredBatches) {
      return processRecoveredRow(row);
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
          BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW, String.valueOf(id)),
          e);
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
          BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW, String.valueOf(id)),
          e);
    }

    if (data.batchRows.size() >= data.commitSize) {
      commitBatch();
    }
  }

  /** One statement at a time, so a rejected row can be diverted without losing the rest. */
  private void executeSingleRow(Object[] row, String id) throws HopException {
    try {
      executeUpdate(data.insertStatement);
    } catch (HopException | RuntimeException e) {
      Throwable failure = statementFailure(e);
      if (!getTransformMeta().isDoingErrorHandling()) {
        rollbackQuietly();
        throw new HopException(
            BaseMessages.getString(
                PKG, CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW, String.valueOf(id)),
            failure);
      }
      // The savepoint already took the row back: the transaction is usable for the next rows.
      putError(data.inputRowMeta, row, 1, failure.getMessage(), null, "PGVECTORUPSERT002");
      return;
    }
    putRow(data.inputRowMeta, row);
    commitIfCommitSizeReached();
  }

  /**
   * Runs the statement of one row. Where a failed statement aborts the transaction (PostgreSQL) and
   * there is an error hop, it runs behind a savepoint, so a failure only undoes this row. Only the
   * statement is inside the savepoint: the row is passed on and committed afterwards, as on a
   * connection shared by a transactional pipeline the other transforms wait while it is open.
   */
  private void executeUpdate(PreparedStatement statement) throws HopException {
    if (data.useSafePoints) {
      data.database.runInSavepoint(
          () -> {
            executeNow(statement);
            return null;
          });
    } else {
      executeNow(statement);
    }
  }

  private static void executeNow(PreparedStatement statement) throws HopDatabaseException {
    try {
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new HopDatabaseException(e.getMessage(), e);
    }
  }

  /** The driver's own failure, for the error hop, rather than the exception wrapping it. */
  private static Throwable statementFailure(Exception e) {
    return e instanceof HopDatabaseException && e.getCause() instanceof SQLException sql ? sql : e;
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
      throw new HopException(
          BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_COMMITTING), e);
    }
  }

  private void rollbackQuietly() {
    try {
      data.database.rollback();
    } catch (Exception e) {
      logDetailed("Unable to roll back the aborted transaction: " + e.getMessage());
    }
  }

  /**
   * Creates the table, its HNSW index and the mapped columns once for the whole run, before any
   * copy, Spark partition or Beam bundle writes a row. Otherwise every one of them runs the DDL,
   * and they race each other creating the same table. The DDL commits on its own, as on the first
   * row.
   */
  @Override
  public void prepareOnce() throws HopException {
    Database database =
        PgVectorDatabase.connect(this, this, getMetadataProvider(), meta.getConnection());
    try {
      database.setCommit(0);
      VectorDistanceMetric indexMetric =
          meta.getIndexMetric() != null ? meta.getIndexMetric() : VectorDistanceMetric.COSINE;
      PgVectorDatabase.ensureSchema(
          database,
          meta,
          resolve(meta.getSchemaName()),
          resolve(meta.getTableName()),
          indexMetric,
          Const.toInt(resolve(meta.getEmbeddingDimensions()), 768));
    } finally {
      database.disconnect();
    }
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
      data.deleteStatement.setString(1, documentId);
      // A failed delete aborts the transaction just as a failed insert does: behind a savepoint
      // only the delete is undone, and the rows after it can still be written.
      executeUpdate(data.deleteStatement);
      data.deletedDocuments.add(documentId);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorUpsert.Error.DeletingDocument", documentId),
          e instanceof HopException hop ? statementFailure(hop) : e);
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
   * unlike the savepoints, which only some databases need. Where the connection's batch failure
   * contract is validated, {@link #configureRecoveredBatches()} batches with an error hop instead.
   */
  void configureCommitStrategy(IDatabase iDatabase) {
    data.commitSize = Const.toInt(resolve(meta.getCommitSize()), 100);
    boolean errorHandling = getTransformMeta().isDoingErrorHandling();
    data.batchMode = data.commitSize > 0 && !errorHandling;
    // PostgreSQL aborts the whole transaction on a failed statement, so a row can only be diverted
    // if the transform can roll back to just before it. Table Output does the same.
    data.useSafePoints = iDatabase.isUseSafePoints() && errorHandling;
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
   *
   * <p>Not on a connection shared by a transactional pipeline: switching it to autocommit commits
   * the open transaction, with the rows the other transforms wrote so far, and a later rollback of
   * the pipeline can no longer undo them. There the DDL runs in the pipeline's transaction (it is
   * transactional in PostgreSQL), and the commit mode is the pipeline's to keep.
   */
  void prepareSchemaAndCommitMode(String schemaName, String tableName) throws HopException {
    boolean sharedConnection = !Utils.isEmpty(data.database.getConnectionGroup());
    // The HNSW index and the mapped-column ALTERs are independent options, so they have to run on
    // an existing table too, not only when this transform creates one. Not when the engine ran
    // them before the run (see prepareOnce).
    if (meta.hasSchemaDdl() && !isPreparedOnce()) {
      if (!sharedConnection) {
        data.database.setCommit(0);
      }
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
    if (!sharedConnection) {
      data.database.setCommit(data.commitSize);
    }
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
      configureRecoveredBatches();
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
      throw new HopException(
          BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_COMMITTING), e);
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
  public void batchComplete() throws HopException {
    // The single threaded engine ends an iteration here: no row may wait for the next one.
    if (data.recoveredBatches) {
      flushOrDiscardRecoveredBatch();
    }
  }

  @Override
  public void dispose() {
    if (data.recoveredBatches && data.database != null) {
      try {
        flushOrDiscardRecoveredBatch();
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_COMMITTING), e);
        setErrors(getErrors() + 1);
      }
    }
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
        logError(BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_COMMITTING), e);
        setErrors(getErrors() + 1);
      }
    }
    closeDatabase();
    super.dispose();
  }

  // Recovered batches: with an error hop, on a connection whose batch failure contract is validated

  /**
   * Decides, once the connection's commit mode is set, whether the upserts go out in a {@link
   * RecoverableBatch} with an error hop attached: the connection's batch failure contract is
   * validated against the database (PostgreSQL's is), the transform has an error hop and a commit
   * size, and the connection is its own and not in auto-commit, so not shared by a transactional
   * pipeline. The contract then tells which rows of a failed batch the database refused, so no row
   * needs a savepoint. Every other case keeps {@link #configureCommitStrategy(IDatabase)} as it is:
   * without an error hop the rows are batched as they always were, and with one every row runs on
   * its own.
   */
  void configureRecoveredBatches() throws HopException {
    data.batchSupport = BatchSupport.of(data.database.getDatabaseMeta().getIDatabase(), this);
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "PgVectorUpsert.Log.BatchSupport", data.batchSupport.describe()));
    }
    data.recoveredBatches =
        data.batchSupport.isRecovered()
            && getTransformMeta().isDoingErrorHandling()
            && Const.toInt(resolve(meta.getCommitSize()), 100) > 0
            && !data.database.isAutoCommit()
            && data.database.getUseBatchInsert(true);
    if (!data.recoveredBatches) {
      return;
    }
    // Batching stays on with the error hop, without a savepoint per row.
    data.useSafePoints = false;
    data.batchMode = false;
    data.batch =
        RecoverableBatch.of(
                data.database, data.insertStatement, this::bindInsertValues, data.batchSupport)
            .withStatementFactory(
                () -> {
                  // A driver that closes the statement of a failed batch gets a new one.
                  data.insertStatement = prepareInsertStatement();
                  return data.insertStatement;
                });
  }

  private PreparedStatement prepareInsertStatement() throws SQLException {
    String qualifiedTable =
        PgVectorSqlBuilder.qualifiedTable(
            resolve(meta.getSchemaName()), resolve(meta.getTableName()));
    return data.database
        .getConnection()
        .prepareStatement(PgVectorSqlBuilder.upsertSql(qualifiedTable, data.tableColumns));
  }

  /**
   * One row on the path of recovered batches. Its upsert waits in the batch, and every row that
   * comes in while the batch holds rows waits with it, also a row that stores nothing or fails
   * before it reaches the database. So the rows leave in the order they came in, each one after the
   * commit that includes it.
   *
   * @return false when the transform was stopped
   */
  private boolean processRecoveredRow(Object[] row) throws HopException {
    String documentId;
    Object[] values;
    try {
      documentId = getFieldAsString(row, data.documentIdFieldIndex);
      String content = getFieldAsString(row, data.contentFieldIndex);
      Object embedding = row[data.embeddingFieldIndex];
      if (Utils.isEmpty(content) || EmbeddingJsonParser.isEmpty(embedding)) {
        // Nothing to store for this row, but it still belongs in the output stream.
        putRowInOrder(row);
        return flushRecoveredBatchWhenFull();
      }
      String id = resolveId(row);
      String chunkIndex = getFieldAsString(row, data.chunkIndexFieldIndex);
      // Before any delete: a row that can't be bound must not delete its document.
      values = insertValues(row, id, documentId, chunkIndex, content, embedding);
      if (shouldDeleteDocument(documentId)
          && data.deletedDocuments.size() >= MAX_DELETED_DOCUMENT_CACHE) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "PgVectorUpsert.Error.TooManyDeletedDocuments",
                String.valueOf(MAX_DELETED_DOCUMENT_CACHE)));
      }
    } catch (HopException e) {
      putErrorInOrder(row, e.getMessage(), CODE_BEFORE_THE_DATABASE);
      return flushRecoveredBatchWhenFull();
    }

    if (shouldDeleteDocument(documentId)) {
      // The rows before it are committed first, so that the transaction holds nothing else.
      if (!flushRecoveredBatchOrStop()) {
        setOutputDone();
        return false;
      }
      replaceDocument(row, documentId, values);
      return true;
    }

    try {
      data.batch.add(values, row);
    } catch (HopDatabaseException e) {
      putErrorInOrder(
          row,
          new HopException(
                  BaseMessages.getString(
                      PKG, CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW, String.valueOf(values[0])),
                  e)
              .getMessage(),
          CODE_BEFORE_THE_DATABASE);
      return flushRecoveredBatchWhenFull();
    }
    PgVectorUpsertData.PendingRow pending = PgVectorUpsertData.PendingRow.output(row);
    data.batchBuffer.add(pending);
    data.pendingRows.add(pending);
    return flushRecoveredBatchWhenFull();
  }

  /**
   * Runs the batch once a commit size of rows waits. The rows that wait behind the batch count as
   * well, so that memory stays at one commit size.
   *
   * @return false when the transform was stopped
   */
  private boolean flushRecoveredBatchWhenFull() {
    if (data.pendingRows.size() >= data.commitSize && !flushRecoveredBatchOrStop()) {
      setOutputDone();
      return false;
    }
    return true;
  }

  /**
   * The first row of a document with "Delete document before upsert". The batch before it was just
   * committed, so the transaction holds nothing else: the delete and the upsert of the row run on
   * their own and are committed together, or both rolled back when the database refuses either. So
   * a refused row never leaves the delete of its document behind; the next row of the document
   * deletes it again.
   */
  private void replaceDocument(Object[] row, String documentId, Object[] values)
      throws HopException {
    try {
      data.deleteStatement.setString(1, documentId);
      data.deleteStatement.executeUpdate();
    } catch (SQLException e) {
      data.database.rollback();
      putErrorInOrder(
          row,
          new HopException(
                  BaseMessages.getString(PKG, "PgVectorUpsert.Error.DeletingDocument", documentId),
                  e)
              .getMessage(),
          CODE_BEFORE_THE_DATABASE);
      return;
    }
    try {
      bindInsertValues(data.insertStatement, values);
      data.insertStatement.executeUpdate();
    } catch (HopDatabaseException | SQLException e) {
      data.database.rollback();
      putErrorInOrder(row, statementFailure(e).toString(), CODE_REFUSED);
      return;
    }
    data.database.commit();
    data.deletedDocuments.add(documentId);
    putRowInOrder(row);
  }

  /** The upsert's parameter values of a row, one per mapping binding, in statement order. */
  private Object[] insertValues(
      Object[] row,
      String id,
      String documentId,
      String chunkIndex,
      String content,
      Object embedding)
      throws HopException {
    String vectorLiteral;
    try {
      vectorLiteral = EmbeddingJsonParser.toPgVectorLiteral(embedding);
    } catch (RuntimeException e) {
      throw new HopException(
          BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW, String.valueOf(id)),
          e);
    }
    Object[] values = new Object[data.mappingBindings.size()];
    int i = 0;
    for (PgVectorUpsertData.MappingBinding binding : data.mappingBindings) {
      values[i++] =
          switch (binding.column.name()) {
            case "id" -> id;
            case "document_id" -> documentId;
            case "chunk_index" -> parseChunkIndex(chunkIndex);
            case "content" -> content;
            case "embedding" -> vectorLiteral;
            default -> getFieldAsString(row, binding.streamFieldIndex);
          };
    }
    return values;
  }

  /**
   * Binds the values of {@link #insertValues} to the upsert. The recovered batch calls it for a row
   * when it is added and again for every retry, always with the same values.
   */
  private void bindInsertValues(PreparedStatement statement, Object[] values)
      throws HopDatabaseException {
    int i = 0;
    try {
      for (PgVectorUpsertData.MappingBinding binding : data.mappingBindings) {
        Object value = values[i++];
        if (binding.column.type() == PgVectorColumnType.INTEGER) {
          statement.setInt(binding.statementIndex, (Integer) value);
        } else {
          statement.setString(binding.statementIndex, value == null ? null : value.toString());
        }
      }
    } catch (SQLException e) {
      // The id is the first column of the upsert.
      throw new HopDatabaseException(
          BaseMessages.getString(
              PKG, CONST_PG_VECTOR_UPSERT_ERROR_BINDING_ROW, String.valueOf(values[0])),
          e);
    }
  }

  /** Passes a row on to the output, after the rows of the pending batch that came in before it. */
  private void putRowInOrder(Object[] row) throws HopTransformException {
    if (data.batchBuffer.isEmpty()) {
      putRow(data.inputRowMeta, row);
    } else {
      data.pendingRows.add(PgVectorUpsertData.PendingRow.output(row));
    }
  }

  /** Passes a row on to the error hop, after the rows of the pending batch that came before it. */
  private void putErrorInOrder(Object[] row, String errorMessage, String errorCode)
      throws HopTransformException {
    if (data.batchBuffer.isEmpty()) {
      putError(data.inputRowMeta, row, 1L, errorMessage, null, errorCode);
    } else {
      data.pendingRows.add(PgVectorUpsertData.PendingRow.error(row, errorMessage, errorCode));
    }
  }

  /**
   * Passes every pending row on in the order the rows came in: a failed row to the error hop, the
   * others to the output. Empties the batch.
   */
  private void releasePendingRows() throws HopTransformException {
    for (PgVectorUpsertData.PendingRow pending : data.pendingRows) {
      if (pending.isFailed()) {
        putError(
            data.inputRowMeta,
            pending.getRow(),
            1L,
            pending.getErrorMessage(),
            null,
            pending.getErrorCode());
      } else {
        putRow(data.inputRowMeta, pending.getRow());
      }
    }
    clearPendingRows();
  }

  private void clearPendingRows() {
    data.pendingRows.clear();
    data.batchBuffer.clear();
  }

  /**
   * Runs the recovered batch, stopping the transform when that fails.
   *
   * @return false when the transform was stopped
   */
  private boolean flushRecoveredBatchOrStop() {
    try {
      flushRecoveredBatch();
      return true;
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, CONST_PG_VECTOR_UPSERT_ERROR_COMMITTING), e);
      setErrors(1);
      stopAll();
      return false;
    }
  }

  /**
   * Runs and commits the recovered batch, then passes every pending row on in the order the rows
   * came in. The connection's batch failure contract tells which rows of a failed batch the
   * database refused: they go to the error hop, the others are committed and go to the output, each
   * exactly once. When the batch can't be run or recovered at all, none of the pending rows is
   * passed on and the failure is thrown.
   */
  private void flushRecoveredBatch() throws HopException {
    if (data.batch == null) {
      return;
    }
    if (!data.batch.isEmpty()) {
      data.settledRows = 0;
      try {
        data.batch.flush(recoveredBatchListener());
      } catch (HopException e) {
        clearPendingRows();
        throw e;
      } finally {
        data.insertStatement = data.batch.getStatement();
      }
    }
    releasePendingRows();
  }

  /**
   * Settles the rows of {@link PgVectorUpsertData#batchBuffer}: the batch gives every row its
   * verdict once, in the order the rows were added. {@link #releasePendingRows()} passes them on
   * afterwards.
   */
  private IBatchRowListener recoveredBatchListener() {
    return new IBatchRowListener() {
      @Override
      public void rowWritten(Object[] outputRow) {
        data.settledRows++;
      }

      @Override
      public void rowRejected(Object[] outputRow, String message, boolean attempted) {
        data.batchBuffer
            .get(data.settledRows++)
            .fail(message, attempted ? CODE_REFUSED : CODE_NOT_ATTEMPTED);
      }
    };
  }

  /**
   * At the end of a single-threaded iteration, and when the transform is disposed: runs what waits
   * in the recovered batch, or drops it when the transform failed or was stopped, so that no row is
   * committed that is not passed on.
   */
  private void flushOrDiscardRecoveredBatch() throws HopException {
    if (data.batch == null) {
      return;
    }
    if (getErrors() > 0 || isStopped()) {
      int dropped = data.batch.discard().size();
      clearPendingRows();
      if (dropped > 0) {
        logError(
            BaseMessages.getString(
                PKG, "PgVectorUpsert.Log.BatchDiscarded", String.valueOf(dropped)));
      }
      return;
    }
    flushRecoveredBatch();
  }
}
