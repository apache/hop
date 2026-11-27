/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.database.batch;

import java.io.Serial;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

/**
 * Writes rows with INSERT statements that carry several rows each ({@code INSERT ... VALUES (...),
 * (...)}), and can still tell, after a failure, which rows are in the database and which are not.
 * It is the multi-row counterpart of {@link RecoverableBatch}, with the same {@link
 * IBatchRowListener} contract: every row added reaches exactly one verdict, once, after the
 * database committed or refused it, in the order the rows were added.
 *
 * <h2>Chunks</h2>
 *
 * Added rows are cut into chunks of at most {@link IStatements#getRowsPerStatement()} rows. A
 * duplicate key among them is the database's to refuse, as it is with one row per statement.
 *
 * <p>When the statements have a size limit ({@link IStatements#getMaxStatementBytes()}, MySQL's
 * {@code max_allowed_packet} for one), a chunk is also closed before a row would take it past the
 * limit, at the largest power of two of its rows, so that it is still one statement; the rows after
 * them start the next chunk. A row too large to share a statement is a statement of its own, as it
 * is with one row per statement.
 *
 * <p>A full chunk is one statement. A shorter chunk (the remainder at a flush) is sent as
 * statements of power-of-two sizes, largest first: 37 rows as 32 + 4 + 1. So at most log2(N) + 2
 * statements are ever prepared and cached for N rows per statement, and a remainder never costs
 * more than log2(N) + 1 executions. Preparing a statement for every chunk size instead could
 * prepare N statements, each parsed by the server; sending the remainder row by row could cost N -
 * 1 executions.
 *
 * <h2>Flush</h2>
 *
 * {@link #flush(IBatchRowListener)} sends the statements of every chunk in input order, as one JDBC
 * batch per run of statements of the same size, then commits. A row is reported with the update
 * count it is known to have: 1 for a row of a statement of several rows, whatever the driver
 * reports for the statement, and its own count for a statement of one row.
 *
 * <h2>Failure</h2>
 *
 * A statement of several rows succeeds or fails as a whole, on every database. When one fails, the
 * statements before it are kept, the rows of the failing statement are run one by one through a
 * {@link RecoverableBatch} on the one-row statement, so that exactly the bad rows are rejected, and
 * the statements after it are sent again as they were. The {@link BatchFailureContract} decides how
 * much has to be re-run:
 *
 * <ul>
 *   <li>{@link BatchFailureContract#KEEPS_PREFIX}: the driver stopped at the failing statement and
 *       the transaction holds exactly the statements before it, which are committed as they are.
 *   <li>{@link BatchFailureContract#MARKS_FAILED_ROWS}: the failing statement is known from the
 *       counts, but the statements after it ran too. The transaction is rolled back and the
 *       statements before the failing one are sent again, unless it was the last one that ran.
 *   <li>{@link BatchFailureContract#LOSES_WHOLE_BATCH} and {@link BatchFailureContract#UNKNOWN}:
 *       the transaction is rolled back. The statement the driver blamed ({@link
 *       IDatabase#getFailedBatchEntry}) is taken as the failing one and the statements before it
 *       are sent again; without a hint all rows of the failed JDBC batch are run one by one. A
 *       wrong hint costs time only: a re-sent statement that fails again sends every remaining row
 *       through the one-row path.
 * </ul>
 *
 * <p>The preconditions are those of {@link RecoverableBatch}: no connection group, no auto-commit,
 * and nothing else written on the connection between flushes. On a connection without transactions,
 * or with one row per statement, every flush simply goes through a {@link RecoverableBatch} on the
 * one-row statement.
 */
public class MultiRowBatch implements IRowBatch {

  /** The statements of a multi-row batch: built, cached and bound by the caller's DML layer. */
  public interface IStatements {

    /**
     * @return the most rows one statement carries, at least 1
     */
    int getRowsPerStatement();

    /**
     * Prepares the statement that carries this many rows. The batch caches what it gets and asks
     * again only when the driver closed the statement.
     *
     * @param rows 1 for the one-row statement, or up to {@link #getRowsPerStatement()}
     */
    PreparedStatement prepare(int rows) throws SQLException, HopDatabaseException;

    /**
     * Sets the parameters of one row of a statement.
     *
     * @param statement a statement prepared for {@code rows} rows
     * @param rows the rows the statement carries
     * @param row the row within the statement, zero-based
     * @param bindRow the values of the row
     */
    void bind(PreparedStatement statement, int rows, int row, Object[] bindRow)
        throws HopDatabaseException;

    /**
     * @return the most bytes the rows of one statement may take together, as {@link #estimateBytes}
     *     counts them; 0 when the database has no limit that matters
     */
    default long getMaxStatementBytes() {
      return 0;
    }

    /**
     * @param bindRow the values of a row
     * @return at least the bytes the row adds to a statement on the wire; only asked when {@link
     *     #getMaxStatementBytes()} gives a limit
     */
    default long estimateBytes(Object[] bindRow) {
      return 0;
    }
  }

  /**
   * A row whose values can't be bound to the statement, thrown by {@link #flushOrFail} (or by a
   * flush without a listener): the row never reached the database, and the transaction is rolled
   * back.
   */
  public static final class UnboundRowException extends HopDatabaseException {
    @Serial private static final long serialVersionUID = 0x4D52424E4F54424EL;

    /** The output row that was added with the row. */
    @Getter private final transient Object[] outputRow;

    UnboundRowException(Object[] outputRow, HopDatabaseException cause) {
      super("Unable to bind a row of the batch", cause);
      this.outputRow = outputRow;
    }
  }

  /** One statement to execute: rows {@code [start, start + size)} of the pending rows. */
  private static final class Piece {
    private final int start;
    private final int size;
    private int count = Statement.SUCCESS_NO_INFO;

    private Piece(int start, int size) {
      this.start = start;
      this.size = size;
    }
  }

  /**
   * A failed execution of pieces {@code [runStart, runEnd)}: the failing piece when it is known,
   * and whether the transaction holds exactly the pieces before it.
   */
  private static final class PieceFailure extends Exception {
    private final int runStart;
    private final int runEnd;
    private final int failedPiece;
    private final boolean prefixApplied;
    private final Exception failure;

    private PieceFailure(
        int runStart, int runEnd, int failedPiece, boolean prefixApplied, Exception failure) {
      super(failure.getMessage(), failure, false, false);
      this.runStart = runStart;
      this.runEnd = runEnd;
      this.failedPiece = failedPiece;
      this.prefixApplied = prefixApplied;
      this.failure = failure;
    }
  }

  private final Connection connection;
  private final IStatements statements;
  private final BatchFailureContract contract;
  private final boolean transactional;

  /** Whether the connection supports savepoints; asked on the first recovered flush. */
  private Boolean savepoints;

  /**
   * Set before a recovered flush, until the first commit: a rollback goes back to it, so work the
   * transaction held before the flush (the truncate of the table, say) stays, as {@link
   * RecoverableBatch} does.
   */
  private Savepoint savepoint;

  private final int rowsPerStatement;

  /** The most bytes the rows of one statement may take; 0 for no limit. */
  private final long maxStatementBytes;

  private RecoverableBatch.IFailedEntryLocator failedEntryLocator;

  private final Map<Integer, PreparedStatement> prepared = new HashMap<>();

  private final List<Object[]> bindRows = new ArrayList<>();
  private final List<Object[]> outputRows = new ArrayList<>();
  private final List<Integer> chunkEnds = new ArrayList<>();
  private int openStart;

  /** The estimated bytes of every pending row; only kept with a byte limit. */
  private final List<Long> rowBytes = new ArrayList<>();

  /** The estimated bytes of the rows of the open chunk. */
  private long openBytes;

  private long statementsExecuted;
  private long executions;
  private long recoveredFlushes;
  private long fallbackRows;
  private long rejectedRows;

  /**
   * @param connection the connection the statements belong to, not in auto-commit mode
   * @param statements builds, prepares and binds the statements
   * @param contract how the driver behaves when a batch fails
   * @param transactional true if the connection supports transactions; without them every flush
   *     writes one row per statement
   */
  public MultiRowBatch(
      Connection connection,
      IStatements statements,
      BatchFailureContract contract,
      boolean transactional) {
    this.connection = connection;
    this.statements = statements;
    this.contract = contract == null ? BatchFailureContract.UNKNOWN : contract;
    this.transactional = transactional;
    this.rowsPerStatement = transactional ? Math.max(1, statements.getRowsPerStatement()) : 1;
    this.maxStatementBytes =
        rowsPerStatement > 1 ? Math.max(0, statements.getMaxStatementBytes()) : 0;
  }

  /**
   * Creates a batch on an open {@link Database}, with the dialect's failed batch entry locator.
   *
   * @param database the connected database, not in auto-commit mode nor in a connection group
   * @param statements builds, prepares and binds the statements
   * @param support the resolved batch support of the connection
   * @throws HopDatabaseException if the connection is shared or in auto-commit mode
   */
  public static MultiRowBatch of(Database database, IStatements statements, BatchSupport support)
      throws HopDatabaseException {
    if (!Utils.isEmpty(database.getConnectionGroup())) {
      throw new HopDatabaseException(
          "A multi-row batch can't be used on a connection shared by a transactional pipeline");
    }
    if (database.isAutoCommit()) {
      throw new HopDatabaseException(
          "A multi-row batch needs a connection with auto-commit switched off");
    }
    boolean transactional;
    try {
      transactional = database.getDatabaseMetaData().supportsTransactions();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to find out if the database supports transactions", e);
    }
    MultiRowBatch batch =
        new MultiRowBatch(database.getConnection(), statements, support.contract(), transactional);
    if (database.getDatabaseMeta() != null && database.getDatabaseMeta().getIDatabase() != null) {
      IDatabase dialect = database.getDatabaseMeta().getIDatabase();
      batch.withFailedEntryLocator(dialect::getFailedBatchEntry);
    }
    return batch;
  }

  /**
   * Lets a recovery find the failing statement of a failed batch, see {@link
   * RecoverableBatch#withFailedEntryLocator}.
   *
   * @return this batch
   */
  public MultiRowBatch withFailedEntryLocator(
      RecoverableBatch.IFailedEntryLocator failedEntryLocator) {
    this.failedEntryLocator = failedEntryLocator;
    return this;
  }

  /**
   * Adds a row. Nothing is sent to the database until {@link #flush}.
   *
   * @param bindRow the values of the row, as {@link IStatements#bind} expects them
   * @param outputRow the row to report back once its fate is known
   */
  @Override
  public void add(Object[] bindRow, Object[] outputRow) {
    if (maxStatementBytes > 0) {
      long bytes = Math.max(0, statements.estimateBytes(bindRow));
      while (bindRows.size() > openStart && openBytes + bytes > maxStatementBytes) {
        closeChunkWithinBytes();
      }
      rowBytes.add(bytes);
      openBytes += bytes;
    }
    bindRows.add(bindRow);
    outputRows.add(outputRow);
    if (bindRows.size() - openStart >= rowsPerStatement) {
      closeChunk();
    }
  }

  private void closeChunk() {
    if (bindRows.size() > openStart) {
      chunkEnds.add(bindRows.size());
      openStart = bindRows.size();
    }
    openBytes = 0;
  }

  /**
   * Closes a chunk of the open rows that the next row doesn't fit with: the largest power of two of
   * them, so that the chunk is one statement. The rows after them stay open.
   */
  private void closeChunkWithinBytes() {
    int size = Integer.highestOneBit(bindRows.size() - openStart);
    for (int r = openStart; r < openStart + size; r++) {
      openBytes -= rowBytes.get(r);
    }
    openStart += size;
    chunkEnds.add(openStart);
  }

  /** Forgets the pending rows. */
  private void clearPending() {
    bindRows.clear();
    outputRows.clear();
    chunkEnds.clear();
    rowBytes.clear();
    openStart = 0;
    openBytes = 0;
  }

  /**
   * @return the number of rows added since the last flush
   */
  @Override
  public int size() {
    return bindRows.size();
  }

  @Override
  public boolean isEmpty() {
    return bindRows.isEmpty();
  }

  /**
   * Sends and commits the pending rows, then reports every row to the listener. A failure is
   * recovered: only the rows the database refuses on their own are rejected.
   *
   * @param listener receives the verdict per row, or null to fail on the first error
   */
  @Override
  public void flush(IBatchRowListener listener) throws HopException {
    flush(listener, listener != null);
  }

  /**
   * Sends and commits the pending rows without recovering a failure: when it succeeds every row is
   * reported as written, when it fails the transaction is rolled back, nothing is reported and the
   * failure is thrown.
   *
   * @param listener receives the written rows, never a rejected one; may be null
   */
  @Override
  public void flushOrFail(IBatchRowListener listener) throws HopException {
    flush(listener, false);
  }

  private void flush(IBatchRowListener listener, boolean recover) throws HopException {
    if (bindRows.isEmpty()) {
      return;
    }
    closeChunk();
    try {
      if (recover) {
        protectEarlierWork();
      }
      if (rowsPerStatement == 1) {
        singleRows(0, bindRows.size(), listener, recover);
        return;
      }
      List<Piece> pieces = pieces();
      int from = 0;
      boolean recovered = false;
      while (from < pieces.size()) {
        try {
          executePieces(pieces, from, pieces.size());
          commit();
        } catch (PieceFailure failure) {
          if (!recover) {
            rollbackQuietly();
            throw failed(failure.failure);
          }
          if (!recovered) {
            recovered = true;
            recoveredFlushes++;
          }
          from = recover(pieces, from, failure, listener);
          continue;
        } catch (SQLException commitFailure) {
          // A deferred constraint or a serialization failure at commit time: the transaction is
          // gone, and nothing says which row caused it.
          rollbackQuietly();
          if (!recover) {
            throw failed(commitFailure);
          }
          if (!recovered) {
            recoveredFlushes++;
          }
          Piece first = pieces.get(from);
          singleRows(first.start, bindRows.size(), listener, true);
          return;
        }
        report(pieces, from, pieces.size(), listener);
        return;
      }
    } finally {
      releaseSavepointQuietly();
      clearPending();
    }
  }

  /**
   * Deals with a failed execution. Afterwards every row up to the end of the suspect statements is
   * committed or refused and reported.
   *
   * @param from the first piece of this attempt; every piece before it is committed and reported
   * @return the piece to carry on from
   */
  private int recover(
      List<Piece> pieces, int from, PieceFailure failure, IBatchRowListener listener)
      throws HopException {
    int failed = failure.failedPiece;
    boolean intact = failed >= 0 && failure.prefixApplied && !abortsTransaction(failure);
    if (failed < 0 && failedEntryLocator != null && failure.failure instanceof SQLException sql) {
      int hint = failedEntryLocator.locate(sql);
      if (hint >= 0 && hint < failure.runEnd - failure.runStart) {
        failed = failure.runStart + hint;
      }
    }
    int prefixEnd = failed >= 0 ? failed : failure.runStart;
    int suspectsEnd = failed >= 0 ? failed + 1 : failure.runEnd;

    if (intact) {
      // The transaction holds exactly the pieces before the failed one.
      commitOrThrow();
    } else {
      rollback();
      if (prefixEnd > from) {
        try {
          executePieces(pieces, from, prefixEnd);
          commit();
        } catch (PieceFailure | SQLException e) {
          // What worked a moment ago fails now, or the hint was wrong: trust nothing and send
          // every remaining row on its own.
          rollback();
          singleRows(pieces.get(from).start, bindRows.size(), listener, true);
          return pieces.size();
        }
      }
    }
    report(pieces, from, prefixEnd, listener);

    int start = pieces.get(prefixEnd).start;
    Piece last = pieces.get(suspectsEnd - 1);
    singleRows(start, last.start + last.size, listener, true);
    return suspectsEnd;
  }

  /**
   * Whether the failure took the rest of the transaction with it, also on a database whose failed
   * statement is otherwise rolled back on its own.
   */
  private boolean abortsTransaction(PieceFailure failure) {
    if (!(failure.failure instanceof SQLException)) {
      // A value Hop couldn't bind: nothing reached the database.
      return false;
    }
    if (contract != BatchFailureContract.MARKS_FAILED_ROWS
        && contract != BatchFailureContract.KEEPS_PREFIX) {
      return true;
    }
    SQLException e = (SQLException) failure.failure;
    while (e != null) {
      String state = e.getSQLState();
      if (state != null && (state.startsWith("40") || "25P02".equals(state))) {
        return true;
      }
      SQLException next = e.getNextException();
      e = next == e ? null : next;
    }
    return false;
  }

  /**
   * Runs rows {@code [from, to)} one row per statement through a {@link RecoverableBatch}, which
   * commits them and reports each row in order.
   */
  private void singleRows(int from, int to, IBatchRowListener listener, boolean recover)
      throws HopException {
    if (from >= to) {
      return;
    }
    if (recover && rowsPerStatement > 1) {
      fallbackRows += to - from;
    }
    PreparedStatement statement = statement(1);
    RecoverableBatch batch =
        new RecoverableBatch(
                connection,
                statement,
                (ps, row) -> statements.bind(ps, 1, 0, row),
                contract,
                transactional)
            .withStatementFactory(() -> statements.prepare(1))
            .withFailedEntryLocator(failedEntryLocator);
    try {
      for (int i = from; i < to; i++) {
        try {
          batch.add(bindRows.get(i), outputRows.get(i));
        } catch (HopDatabaseException e) {
          if (!recover) {
            batch.discard();
            rollbackQuietly();
            throw new UnboundRowException(outputRows.get(i), e);
          }
          // A value Hop can't bind never reaches the database. Settle the rows before it first,
          // so the verdicts stay in input order.
          batch.flush(listener);
          rejectedRows++;
          listener.rowNotBound(outputRows.get(i), e);
        }
      }
      if (recover) {
        batch.flush(listener);
      } else {
        batch.flushOrFail(listener);
      }
    } finally {
      // The one-row batch committed (or rolled back) the transaction: no savepoint of this flush
      // is left to go back to.
      savepoint = null;
      executions += batch.getExecutions();
      rejectedRows += batch.getRejectedRows();
      prepared.put(1, batch.getStatement());
    }
  }

  /** Cuts the closed chunks into statements: full chunks whole, the others in powers of two. */
  private List<Piece> pieces() {
    List<Piece> pieces = new ArrayList<>();
    int start = 0;
    for (int end : chunkEnds) {
      int size = end - start;
      if (size == rowsPerStatement) {
        pieces.add(new Piece(start, size));
      } else {
        for (int bit = Integer.highestOneBit(size); bit > 0; bit >>= 1) {
          if ((size & bit) != 0) {
            pieces.add(new Piece(start, bit));
            start += bit;
          }
        }
      }
      start = end;
    }
    return pieces;
  }

  /**
   * Executes pieces {@code [from, to)} in order, each run of pieces of the same size as one JDBC
   * batch, and keeps the count of each piece. Nothing is committed.
   */
  private void executePieces(List<Piece> pieces, int from, int to) throws PieceFailure {
    int i = from;
    while (i < to) {
      int size = pieces.get(i).size;
      int end = i + 1;
      while (end < to && pieces.get(end).size == size) {
        end++;
      }
      PreparedStatement statement;
      try {
        statement = statement(size);
      } catch (HopDatabaseException e) {
        throw new PieceFailure(i, end, i, true, e);
      }
      if (end - i == 1 || contract == BatchFailureContract.NO_BATCH_SUPPORT) {
        for (int p = i; p < end; p++) {
          executeOne(statement, pieces.get(p), p);
        }
      } else {
        executeRun(statement, pieces, i, end);
      }
      i = end;
    }
  }

  private void executeOne(PreparedStatement statement, Piece piece, int index) throws PieceFailure {
    try {
      bindPiece(statement, piece);
    } catch (HopDatabaseException e) {
      throw new PieceFailure(index, index + 1, index, true, e);
    }
    try {
      executions++;
      statementsExecuted++;
      piece.count = statement.executeUpdate();
    } catch (SQLException e) {
      // The statement failed on its own; abortsTransaction() says whether it took more with it.
      throw new PieceFailure(index, index + 1, index, true, e);
    }
  }

  private void executeRun(PreparedStatement statement, List<Piece> pieces, int from, int to)
      throws PieceFailure {
    for (int p = from; p < to; p++) {
      try {
        bindPiece(statement, pieces.get(p));
        statement.addBatch();
      } catch (HopDatabaseException | SQLException e) {
        clearBatchQuietly(statement);
        // Nothing of this run reached the database, also not the pieces before p.
        throw new PieceFailure(from, to, p, false, e);
      }
    }
    int[] counts;
    try {
      executions++;
      statementsExecuted += to - from;
      counts = statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      clearBatchQuietly(statement);
      throw runFailure(from, to, e);
    }
    // A count belongs to a piece only when there is one per statement. A driver that folds the
    // statements of a batch into fewer ones (pgjdbc's reWriteBatchedInserts) may report fewer
    // counts, and which statements each one covers is unknown.
    boolean aligned = counts != null && counts.length == to - from;
    for (int p = from; p < to; p++) {
      pieces.get(p).count = aligned ? counts[p - from] : Statement.SUCCESS_NO_INFO;
    }
  }

  /** Works out from the update counts, when they can be trusted, which piece of a run failed. */
  private PieceFailure runFailure(int from, int to, SQLException e) {
    int size = to - from;
    int[] counts = e instanceof BatchUpdateException bue ? bue.getUpdateCounts() : null;
    if (counts != null && contract == BatchFailureContract.KEEPS_PREFIX && counts.length < size) {
      return new PieceFailure(from, to, from + counts.length, true, e);
    }
    if (counts != null
        && contract == BatchFailureContract.MARKS_FAILED_ROWS
        && counts.length == size) {
      int failed = -1;
      boolean laterApplied = false;
      for (int i = 0; i < size; i++) {
        if (counts[i] == Statement.EXECUTE_FAILED) {
          if (failed < 0) {
            failed = i;
          }
        } else if (failed >= 0) {
          laterApplied = true;
        }
      }
      if (failed >= 0) {
        return new PieceFailure(from, to, from + failed, !laterApplied, e);
      }
    }
    return new PieceFailure(from, to, -1, false, e);
  }

  private void bindPiece(PreparedStatement statement, Piece piece) throws HopDatabaseException {
    for (int r = 0; r < piece.size; r++) {
      int row = piece.start + r;
      try {
        statements.bind(statement, piece.size, r, bindRows.get(row));
      } catch (HopDatabaseException e) {
        throw new UnboundRowException(outputRows.get(row), e);
      }
    }
  }

  /** Reports the rows of pieces {@code [from, to)} as written. */
  private void report(List<Piece> pieces, int from, int to, IBatchRowListener listener)
      throws HopException {
    if (listener == null) {
      return;
    }
    for (int p = from; p < to; p++) {
      Piece piece = pieces.get(p);
      // A row of an INSERT of several rows inserted exactly one row.
      int count = piece.size == 1 ? piece.count : 1;
      for (int r = piece.start; r < piece.start + piece.size; r++) {
        listener.rowWritten(outputRows.get(r), count);
      }
    }
  }

  private PreparedStatement statement(int rows) throws HopDatabaseException {
    PreparedStatement statement = prepared.get(rows);
    try {
      if (statement == null || statement.isClosed()) {
        // Some drivers (DuckDB among them) close a statement when it fails.
        statement = statements.prepare(rows);
        prepared.put(rows, statement);
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to prepare the statement for " + rows + " rows", e);
    }
    return statement;
  }

  /**
   * Drops the rows that were added but not flushed, without running them.
   *
   * @return the output rows that were dropped, in the order they were added
   */
  @Override
  public List<Object[]> discard() {
    List<Object[]> dropped = new ArrayList<>(outputRows);
    clearPending();
    return dropped;
  }

  /** Closes the statements the batch prepared. */
  @Override
  public void close() {
    for (PreparedStatement statement : prepared.values()) {
      try {
        statement.close();
      } catch (SQLException e) {
        // closing anyway
      }
    }
    prepared.clear();
  }

  /**
   * @return how many statements the batch executed itself, each entry of a JDBC batch once; the
   *     rows run one by one through a {@link RecoverableBatch} are counted by {@link
   *     #getFallbackRows()} and in {@link #getExecutions()}
   */
  public long getStatements() {
    return statementsExecuted;
  }

  /**
   * @return how many round trips wrote rows: JDBC batches plus single executions
   */
  @Override
  public long getExecutions() {
    return executions;
  }

  /**
   * @return how many flushes failed and were recovered
   */
  public long getRecoveredFlushes() {
    return recoveredFlushes;
  }

  /**
   * @return how many rows were run one row per statement after a multi-row statement failed
   */
  public long getFallbackRows() {
    return fallbackRows;
  }

  /**
   * @return how many rows were rejected
   */
  @Override
  public long getRejectedRows() {
    return rejectedRows;
  }

  /**
   * @return the number of rows per statement used: 1 on a connection without transactions
   */
  @Override
  public int getRowsPerStatement() {
    return rowsPerStatement;
  }

  /**
   * @return the most bytes the rows of one statement may take, 0 for no limit
   */
  public long getMaxStatementBytes() {
    return maxStatementBytes;
  }

  /**
   * @return the sizes of the statements prepared so far
   */
  public Set<Integer> getPreparedSizes() {
    return Set.copyOf(prepared.keySet());
  }

  @Override
  public BatchFailureContract getContract() {
    return contract;
  }

  private static HopDatabaseException failed(Exception cause) {
    if (cause instanceof SQLException sql) {
      return Database.createHopDatabaseBatchException("Error updating batch", sql);
    }
    if (cause instanceof HopDatabaseException hop) {
      return hop;
    }
    return new HopDatabaseException("Error updating batch", cause);
  }

  private void commit() throws SQLException {
    if (transactional) {
      connection.commit();
      // The commit ended the transaction and its savepoints: from here on it holds only this batch.
      savepoint = null;
    }
  }

  /**
   * Keeps what the transaction already holds out of reach of the rollbacks of a recovery: a
   * savepoint to roll back to, or, without savepoints, a commit.
   */
  private void protectEarlierWork() throws HopDatabaseException {
    savepoint = null;
    if (!transactional) {
      return;
    }
    try {
      if (savepoints == null) {
        savepoints = connection.getMetaData().supportsSavepoints();
      }
      if (savepoints) {
        savepoint = connection.setSavepoint();
      } else {
        connection.commit();
      }
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to prepare the transaction for a recoverable batch", e);
    }
  }

  private void releaseSavepointQuietly() {
    if (savepoint == null) {
      return;
    }
    try {
      connection.releaseSavepoint(savepoint);
    } catch (SQLException | RuntimeException e) {
      // The next commit or rollback ends it anyway.
    } finally {
      savepoint = null;
    }
  }

  private void commitOrThrow() throws HopDatabaseException {
    try {
      commit();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to commit the rows of a recovered batch", e);
    }
  }

  private void rollback() throws HopDatabaseException {
    try {
      if (savepoint != null) {
        connection.rollback(savepoint);
        // Some drivers (mssql-jdbc) forget a savepoint once it is rolled back to, and PostgreSQL
        // would nest a new one deeper: release it and set a new one, as RecoverableBatch does.
        releaseSavepointQuietly();
        savepoint = connection.setSavepoint();
      } else {
        connection.rollback();
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to roll back a failed batch", e);
    }
  }

  private void rollbackQuietly() {
    // A rollback of the whole transaction, other work included, as a failed load always did.
    savepoint = null;
    try {
      if (transactional) {
        connection.rollback();
      }
    } catch (SQLException e) {
      // The failure that got us here is the error worth reporting.
    }
  }

  private static void clearBatchQuietly(PreparedStatement statement) {
    try {
      statement.clearBatch();
    } catch (SQLException e) {
      // Nothing left to clear, or the statement is unusable; the next execute says so.
    }
  }

  @Override
  public String toString() {
    return "MultiRowBatch["
        + contract.name()
        + ", "
        + rowsPerStatement
        + " rows per statement, "
        + bindRows.size()
        + " pending"
        + (transactional ? "" : ", no transactions")
        + "]";
  }
}
