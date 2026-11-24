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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

/**
 * A JDBC batch on one prepared statement that can tell, after a failure, which of its rows are in
 * the database and which are not.
 *
 * <p>Each row is added as a pair: the bind row that goes to the statement, and the output row the
 * transform passes on. {@link #flush(IBatchRowListener)} runs the batch and commits it. When the
 * batch succeeds every row is reported as written. When it fails, the rows are worked out according
 * to the connection's {@link BatchFailureContract}:
 *
 * <ul>
 *   <li>{@link BatchFailureContract#MARKS_FAILED_ROWS} and {@link
 *       BatchFailureContract#KEEPS_PREFIX}: the update counts are trusted. The rows they mark as
 *       applied are committed and reported as written, the failing row is rejected, and rows the
 *       driver never ran are sent again as a new batch. That costs one extra round trip per bad
 *       row.
 *   <li>{@link BatchFailureContract#LOSES_WHOLE_BATCH} and {@link BatchFailureContract#UNKNOWN}:
 *       the counts are not trusted. The batch is rolled back and split in halves, each half run and
 *       committed on its own, until the failing rows are isolated and rejected one by one. With few
 *       bad rows that costs a handful of round trips instead of one per row.
 * </ul>
 *
 * <p>The transaction may hold other work when a flush starts, such as the truncate of the table
 * before the first batch. A failure that is recovered never undoes that work: the flush sets a
 * savepoint first and rolls back to it, and once something is committed the transaction holds
 * nothing but this batch. A database without savepoints (DuckDB) commits that work before the batch
 * runs instead. A flush that is not recovered rolls back the whole transaction, other work
 * included, as a failed load always did.
 *
 * <p>This class must not be used on a connection in a connection group (a transactional pipeline)
 * or in auto-commit mode; {@link Database#getUseBatchInsert(boolean)} already refuses to batch
 * there.
 *
 * <p>Rows are replayed through the {@link IBatchRowBinder}, so a retried row is bound from its own
 * bind row and never from the output row.
 */
public class RecoverableBatch implements IRowBatch {
  private static final Class<?> PKG = RecoverableBatch.class;

  private final Connection connection;
  private PreparedStatement statement;
  private IStatementFactory statementFactory;
  private IFailedEntryLocator failedEntryLocator;
  private final IBatchRowBinder binder;
  private final BatchFailureContract contract;
  private final boolean transactional;

  private final List<Object[]> bindRows = new ArrayList<>();
  private final List<Object[]> outputRows = new ArrayList<>();

  /** Whether the connection supports savepoints; asked on the first recovered flush. */
  private Boolean savepoints;

  /**
   * Set before a recovered flush, until the first commit: a rollback goes back to it, so work the
   * transaction held before the flush stays.
   */
  private Savepoint savepoint;

  private long executions;
  private long recoveredBatches;
  private long rejectedRows;

  /**
   * @param connection the connection the statement belongs to, not in auto-commit mode
   * @param statement the prepared statement to batch on
   * @param binder sets the statement's parameters from a bind row
   * @param contract how the driver behaves when a batch fails
   * @param transactional true if the connection supports transactions, so a failed batch can be
   *     rolled back; without transactions only the update counts can be used
   */
  public RecoverableBatch(
      Connection connection,
      PreparedStatement statement,
      IBatchRowBinder binder,
      BatchFailureContract contract,
      boolean transactional) {
    this.connection = connection;
    this.statement = statement;
    this.binder = binder;
    this.contract = contract == null ? BatchFailureContract.UNKNOWN : contract;
    this.transactional = transactional;
  }

  /**
   * Creates a batch for a statement on an open {@link Database}.
   *
   * @param database the connected database
   * @param statement the prepared statement to batch on
   * @param binder sets the statement's parameters from a bind row
   * @param support the resolved batch support of the connection
   */
  public static RecoverableBatch of(
      Database database, PreparedStatement statement, IBatchRowBinder binder, BatchSupport support)
      throws HopDatabaseException {
    if (!Utils.isEmpty(database.getConnectionGroup())) {
      // In a transactional pipeline the connection is shared: committing or rolling back a batch
      // here would commit or undo the work of other transforms too.
      throw new HopDatabaseException(
          "A recoverable batch can't be used on a connection shared by a transactional pipeline");
    }
    if (database.isAutoCommit()) {
      throw new HopDatabaseException(
          "A recoverable batch needs a connection with auto-commit switched off");
    }
    boolean transactional;
    try {
      transactional = database.getDatabaseMetaData().supportsTransactions();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to find out if the database supports transactions", e);
    }
    RecoverableBatch batch =
        new RecoverableBatch(
            database.getConnection(), statement, binder, support.contract(), transactional);
    if (database.getDatabaseMeta() != null && database.getDatabaseMeta().getIDatabase() != null) {
      IDatabase dialect = database.getDatabaseMeta().getIDatabase();
      batch.withFailedEntryLocator(dialect::getFailedBatchEntry);
    }
    return batch;
  }

  /**
   * Binds a row and adds it to the batch. Nothing is sent to the database until {@link #flush}.
   *
   * @param bindRow the values for the statement's parameters
   * @param outputRow the row to report back once its fate is known
   */
  @Override
  public void add(Object[] bindRow, Object[] outputRow) throws HopDatabaseException {
    try {
      ensureOpen();
      binder.bind(statement, bindRow);
      statement.addBatch();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to add a row to the batch", e);
    }
    bindRows.add(bindRow);
    outputRows.add(outputRow);
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
   * Runs and commits the batch, then reports every row to the listener.
   *
   * <p>Without a listener nothing is recovered: a failure rolls the batch back and is thrown, as a
   * transform without an error hop expects.
   *
   * @param listener receives the verdict per row, or null to fail on the first error
   * @throws HopDatabaseBatchException if the batch fails and there is no listener
   */
  @Override
  public void flush(IBatchRowListener listener) throws HopException {
    flush(listener, listener != null);
  }

  /**
   * Runs and commits the batch without recovering a failure: when it succeeds every row is reported
   * as written, when it fails the batch is rolled back and the failure is thrown. This is what a
   * transform without an error hop needs, since it must stop at the first error but still pass on
   * the rows that were committed.
   *
   * @param listener receives the written rows, never a rejected one; may be null
   * @throws HopDatabaseBatchException if the batch fails
   */
  @Override
  public void flushOrFail(IBatchRowListener listener) throws HopException {
    flush(listener, false);
  }

  private void flush(IBatchRowListener listener, boolean recover) throws HopException {
    if (bindRows.isEmpty()) {
      return;
    }
    try {
      if (recover) {
        protectEarlierWork();
      }
      int[] counts;
      try {
        executions++;
        counts = statement.executeBatch();
        statement.clearBatch();
        commitIfTransactional();
      } catch (SQLException e) {
        clearBatchQuietly();
        if (!recover) {
          rollbackQuietly();
          throw Database.createHopDatabaseBatchException("Error updating batch", e);
        }
        recoveredBatches++;
        recover(e, listener);
        return;
      }
      if (listener != null) {
        for (int i = 0; i < outputRows.size(); i++) {
          listener.rowWritten(outputRows.get(i), countAt(counts, i));
        }
      }
    } finally {
      releaseSavepointQuietly();
      bindRows.clear();
      outputRows.clear();
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
      if (supportsSavepoints()) {
        savepoint = connection.setSavepoint();
      } else {
        connection.commit();
      }
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to prepare the transaction for a recoverable batch", e);
    }
  }

  private boolean supportsSavepoints() throws SQLException {
    if (savepoints == null) {
      savepoints = connection.getMetaData().supportsSavepoints();
    }
    return savepoints;
  }

  /**
   * Drops the rows that were added but not flushed, without running them. Use it when the
   * transaction is being rolled back anyway.
   *
   * @return the output rows that were dropped, in the order they were added
   */
  @Override
  public List<Object[]> discard() {
    clearBatchQuietly();
    List<Object[]> dropped = new ArrayList<>(outputRows);
    bindRows.clear();
    outputRows.clear();
    return dropped;
  }

  /**
   * @return how many times a batch or a single row was sent to the database
   */
  @Override
  public long getExecutions() {
    return executions;
  }

  /**
   * @return how many flushes failed and were recovered
   */
  public long getRecoveredBatches() {
    return recoveredBatches;
  }

  /**
   * @return how many rows were rejected
   */
  @Override
  public long getRejectedRows() {
    return rejectedRows;
  }

  /**
   * Some drivers (DuckDB among them) close a prepared statement when its batch fails. Recovery has
   * to run rows again, so give the batch a way to prepare the statement anew. Without one, a closed
   * statement makes recovery fail with an error.
   *
   * @param statementFactory prepares the same statement on the same connection
   * @return this batch
   */
  public RecoverableBatch withStatementFactory(IStatementFactory statementFactory) {
    this.statementFactory = statementFactory;
    return this;
  }

  /**
   * @return the statement this batch runs on now; it differs from the one it was created with when
   *     the driver closed that one and the batch prepared it again
   */
  public PreparedStatement getStatement() {
    return statement;
  }

  /**
   * Lets the batch ask which entry of a failed batch the driver blamed, so it can skip most of the
   * search for the failing row. See {@link IDatabase#getFailedBatchEntry(SQLException)}.
   *
   * @param failedEntryLocator returns the zero-based index of the failing entry, or -1
   * @return this batch
   */
  public RecoverableBatch withFailedEntryLocator(IFailedEntryLocator failedEntryLocator) {
    this.failedEntryLocator = failedEntryLocator;
    return this;
  }

  /** Finds the entry a driver blamed for a failed batch, or -1 when it doesn't say. */
  @FunctionalInterface
  public interface IFailedEntryLocator {
    int locate(SQLException failure);
  }

  /** Prepares the statement of a batch again, on the same connection. */
  @FunctionalInterface
  public interface IStatementFactory {
    PreparedStatement prepare() throws SQLException, HopDatabaseException;
  }

  private void ensureOpen() throws SQLException, HopDatabaseException {
    if (!statement.isClosed()) {
      return;
    }
    if (statementFactory == null) {
      throw new HopDatabaseException(
          "The database closed the prepared statement after a failed batch, and the batch has no"
              + " way to prepare it again");
    }
    statement = statementFactory.prepare();
  }

  /**
   * @return 1: every row is a statement of its own
   */
  @Override
  public int getRowsPerStatement() {
    return 1;
  }

  @Override
  public BatchFailureContract getContract() {
    return contract;
  }

  private void recover(SQLException failure, IBatchRowListener listener) throws HopException {
    int size = bindRows.size();
    int[] counts = failure instanceof BatchUpdateException bue ? bue.getUpdateCounts() : null;
    Verdicts verdicts = new Verdicts(listener);

    // A full array in which every row failed is what a driver that aborts the whole batch returns.
    // Believing it would reject good rows when a dialect is declared wrongly, so check it instead.
    boolean allFailed = counts != null && counts.length == size && allExecuteFailed(counts);

    if (contract.hasTrustworthyCounts()
        && counts != null
        && counts.length <= size
        && !(allFailed && transactional && size > 1)) {
      recoverByCounts(0, counts, failure, verdicts);
    } else if (transactional) {
      rollback();
      locateAndRetry(range(0, size), failure, verdicts);
    } else {
      // Nothing can be undone and nothing is retried blindly: the counts, if any, are all there is.
      recoverByCounts(
          0, counts != null && counts.length <= size ? counts : null, failure, verdicts);
    }
    verdicts.finish();
  }

  /**
   * Routes rows {@code [from, size)} by an update counts array that describes them from {@code
   * from} on, then re-sends whatever the driver never ran.
   */
  private void recoverByCounts(int from, int[] counts, SQLException failure, Verdicts verdicts)
      throws HopException {
    int size = bindRows.size();
    List<String> messages = messages(failure);
    String batchMessage = failure.toString();

    while (true) {
      // What the driver did apply is committed before it is reported: a row isn't written until
      // the commit says so.
      commit();

      int counted = counts == null ? 0 : counts.length;
      int messageNr = 0;
      for (int i = 0; i < counted; i++) {
        if (counts[i] == Statement.EXECUTE_FAILED) {
          String message = messageNr < messages.size() ? messages.get(messageNr++) : batchMessage;
          verdicts.rejected(from + i, message, true);
        } else {
          // A row count, zero, or SUCCESS_NO_INFO: anything but EXECUTE_FAILED is a success.
          verdicts.written(from + i, counts[i]);
        }
      }
      from += counted;
      if (from >= size) {
        return;
      }

      if (counts == null) {
        // Only reached without transactions: the driver didn't say which rows it applied and the
        // batch can't be undone, so retrying could write a row twice.
        String message =
            BaseMessages.getString(PKG, "BatchRecovery.Error.RowNotAttempted", batchMessage);
        for (int i = from; i < size; i++) {
          verdicts.rejected(i, message, false);
        }
        return;
      }

      // A short array: the driver stopped at this row, and this is the row it refused.
      verdicts.rejected(
          from, messageNr < messages.size() ? messages.get(messageNr) : batchMessage, true);
      from++;
      if (from >= size) {
        return;
      }

      // The rows behind it were never sent. Send them again as one batch; if that fails too, this
      // loop deals with the new failure the same way.
      int[] tail = range(from, size);
      try {
        int[] tailCounts = execute(tail);
        commitIfTransactional();
        verdicts.writtenAll(tail, tailCounts);
        return;
      } catch (SQLException e) {
        clearBatchQuietly();
        int[] retryCounts = e instanceof BatchUpdateException bue ? bue.getUpdateCounts() : null;
        if (retryCounts == null || retryCounts.length > size - from) {
          if (transactional) {
            rollback();
            bisect(tail, 0, tail.length, verdicts);
            return;
          }
          retryCounts = null;
        }
        counts = retryCounts;
        failure = e;
        messages = messages(e);
        batchMessage = e.toString();
      }
    }
  }

  /**
   * Works through the given rows after a failed batch that has been rolled back, using the entry
   * the driver blamed when it names one. The blamed row is held back and all the other rows, before
   * and after it, are sent again as one batch and committed; if that batch fails too, its own
   * blamed row is held back the same way. Only once everything else is in the database are the held
   * rows run, one by one in input order, and a held row is rejected only if it fails there. So a
   * wrong hint never rejects a row that is fine, also not one that depends on an earlier row of the
   * same batch, and of two rows with the same key the second is still the one refused. A bad row
   * costs two executions this way: the next batch, and its own.
   *
   * <p>A hint that is not about rows would hold back one good row per execution. pgjdbc with
   * reWriteBatchedInserts=true, for one, counts its rewritten multi-row statements instead. So when
   * the hint points before the previous one (impossible for a correct hint: the rows in front of it
   * ran fine last time), or names the same position again, or every time the number of held rows
   * doubles from 8, the newly blamed row is run on its own and rolled back. If it succeeds there,
   * the hint is not trusted any more, and everything left is bisected. A row that fails on its own
   * is not rejected for that: it may only need an earlier row of the batch. When the driver names
   * nothing, the rows are bisected right away.
   *
   * @param rows the indexes of the rows to work through, in the order they were added
   */
  private void locateAndRetry(int[] rows, SQLException failure, Verdicts verdicts)
      throws HopException {
    List<Integer> held = new ArrayList<>();
    int previous = -1;
    int checkAt = 8;
    while (true) {
      int index = failedEntryLocator == null ? -1 : failedEntryLocator.locate(failure);
      if (index < previous || index < 0 || index >= rows.length) {
        bisect(merge(rows, held), verdicts);
        return;
      }
      int blamed = rows[index];
      held.add(blamed);
      rows = without(rows, index);
      boolean check = index == previous || held.size() >= checkAt;
      previous = index;
      if (check) {
        if (held.size() >= checkAt) {
          checkAt *= 2;
        }
        if (succeedsAlone(blamed)) {
          bisect(merge(rows, held), verdicts);
          return;
        }
      }
      if (rows.length == 0) {
        break;
      }
      try {
        int[] counts = execute(rows);
        commitIfTransactional();
        verdicts.writtenAll(rows, counts);
        break;
      } catch (SQLException e) {
        clearBatchQuietly();
        rollback();
        failure = e;
      }
    }
    held.sort(null);
    for (int index : held) {
      runSingle(index, verdicts);
    }
  }

  /** Bisects the given rows, none of which is in the database yet. */
  private void bisect(int[] rows, Verdicts verdicts) throws HopException {
    bisect(rows, 0, rows.length, verdicts);
  }

  private static int[] merge(int[] rows, List<Integer> held) {
    int[] all = Arrays.copyOf(rows, rows.length + held.size());
    for (int i = 0; i < held.size(); i++) {
      all[rows.length + i] = held.get(i);
    }
    Arrays.sort(all);
    return all;
  }

  /**
   * Finds the failing rows in {@code rows[from, to)}, a range known to contain at least one, by
   * running each half as its own batch. The transaction must hold none of these rows when this is
   * called.
   */
  private void bisect(int[] rows, int from, int to, Verdicts verdicts) throws HopException {
    if (to - from <= 1) {
      runSingle(rows[from], verdicts);
      return;
    }
    int middle = (from + to) >>> 1;
    runRange(rows, from, middle, verdicts);
    runRange(rows, middle, to, verdicts);
  }

  private void runRange(int[] rows, int from, int to, Verdicts verdicts) throws HopException {
    if (to - from <= 1) {
      runSingle(rows[from], verdicts);
      return;
    }
    int[] part = Arrays.copyOfRange(rows, from, to);
    int[] counts;
    try {
      counts = execute(part);
      commitIfTransactional();
    } catch (SQLException e) {
      clearBatchQuietly();
      rollback();
      bisect(rows, from, to, verdicts);
      return;
    }
    verdicts.writtenAll(part, counts);
  }

  /** Runs one row on its own and commits it, or rejects it when the database refuses it. */
  private void runSingle(int index, Verdicts verdicts) throws HopException {
    int count;
    try {
      executions++;
      ensureOpen();
      binder.bind(statement, bindRows.get(index));
      count = statement.executeUpdate();
      commitIfTransactional();
    } catch (SQLException e) {
      rollback();
      verdicts.rejected(index, e.toString(), true);
      return;
    }
    verdicts.written(index, count);
  }

  /**
   * Runs one row on its own and rolls it back whatever happens, to check a hint. The transaction
   * must hold none of the batch's rows when this is called, and again holds none afterwards. A
   * failure says nothing for sure: the row may only need an earlier row of the batch.
   *
   * @return true when the row succeeds on its own
   */
  private boolean succeedsAlone(int index) throws HopException {
    try {
      executions++;
      ensureOpen();
      binder.bind(statement, bindRows.get(index));
      statement.executeUpdate();
    } catch (SQLException e) {
      rollback();
      return false;
    }
    rollback();
    return true;
  }

  /** Binds and runs the given rows as one batch. */
  private int[] execute(int[] rows) throws SQLException, HopDatabaseException {
    executions++;
    ensureOpen();
    for (int index : rows) {
      binder.bind(statement, bindRows.get(index));
      statement.addBatch();
    }
    int[] counts = statement.executeBatch();
    statement.clearBatch();
    return counts;
  }

  private static int[] range(int from, int to) {
    int[] rows = new int[to - from];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = from + i;
    }
    return rows;
  }

  private static int[] without(int[] rows, int position) {
    int[] rest = new int[rows.length - 1];
    System.arraycopy(rows, 0, rest, 0, position);
    System.arraycopy(rows, position + 1, rest, position, rows.length - position - 1);
    return rest;
  }

  /**
   * The update count of entry {@code i} of a batch, or {@link Statement#SUCCESS_NO_INFO} when the
   * driver returned no entry for it.
   */
  private static int countAt(int[] counts, int i) {
    return counts != null && i < counts.length ? counts[i] : Statement.SUCCESS_NO_INFO;
  }

  private static boolean allExecuteFailed(int[] counts) {
    for (int count : counts) {
      if (count != Statement.EXECUTE_FAILED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Passes the verdicts of a recovery to the listener in the order the rows were added, whatever
   * the order in which they are settled: a verdict is held until every row before it has one.
   */
  private final class Verdicts {
    private static final byte NONE = 0;
    private static final byte WRITTEN = 1;
    private static final byte REJECTED = 2;

    private final IBatchRowListener listener;
    private final byte[] kind;
    private final int[] counts;
    private final String[] messages;
    private final boolean[] attempted;
    private int next;

    private Verdicts(IBatchRowListener listener) {
      this.listener = listener;
      int size = outputRows.size();
      kind = new byte[size];
      counts = new int[size];
      messages = new String[size];
      attempted = new boolean[size];
    }

    void written(int index, int count) throws HopException {
      kind[index] = WRITTEN;
      counts[index] = count;
      release();
    }

    /** Reports the rows of a batch that was committed, each with its entry of the counts. */
    void writtenAll(int[] rows, int[] batchCounts) throws HopException {
      for (int i = 0; i < rows.length; i++) {
        written(rows[i], countAt(batchCounts, i));
      }
    }

    void rejected(int index, String message, boolean wasAttempted) throws HopException {
      kind[index] = REJECTED;
      messages[index] = message;
      attempted[index] = wasAttempted;
      release();
    }

    /** Reports every settled row that no unsettled row precedes. */
    private void release() throws HopException {
      while (next < kind.length && kind[next] != NONE) {
        int index = next++;
        if (kind[index] == WRITTEN) {
          listener.rowWritten(outputRows.get(index), counts[index]);
        } else {
          rejectedRows++;
          listener.rowRejected(outputRows.get(index), messages[index], attempted[index]);
        }
      }
    }

    /** Checks that every row was reported: a recovery must settle them all. */
    void finish() throws HopException {
      if (next < kind.length) {
        throw new HopDatabaseException(
            "Batch recovery left row " + next + " of " + kind.length + " without a verdict");
      }
    }
  }

  /**
   * The per-row messages a driver chains onto a batch failure, in order. When there are none the
   * caller falls back to the batch's own message.
   */
  private static List<String> messages(SQLException failure) {
    List<String> messages = new ArrayList<>();
    SQLException next = failure.getNextException();
    SQLException previous = null;
    // Some drivers return the same exception from getNextException() forever, hence != and not
    // equals().
    while (next != null && next != previous && next != failure) {
      messages.add(next.toString());
      previous = next;
      next = next.getNextException();
    }
    return messages;
  }

  private void commitIfTransactional() throws SQLException {
    if (transactional) {
      connection.commit();
      // The commit ended the transaction and its savepoints: from here on it holds only this batch
      savepoint = null;
    }
  }

  private void commit() throws HopDatabaseException {
    try {
      commitIfTransactional();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to commit the rows of a recovered batch", e);
    }
  }

  /**
   * Undoes the rows of this batch that are not committed. Before the first commit of a flush that
   * goes back to its savepoint, which stays set, so the work the transaction held before stays.
   */
  private void rollback() throws HopDatabaseException {
    try {
      if (savepoint != null) {
        connection.rollback(savepoint);
      } else {
        connection.rollback();
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to roll back a failed batch", e);
    }
  }

  private void releaseSavepointQuietly() {
    if (savepoint == null) {
      return;
    }
    try {
      connection.releaseSavepoint(savepoint);
    } catch (SQLException e) {
      // The next commit or rollback ends it anyway.
    } finally {
      savepoint = null;
    }
  }

  private void rollbackQuietly() {
    try {
      if (transactional) {
        connection.rollback();
      }
    } catch (SQLException e) {
      // The batch failure that got us here is the error worth reporting.
    }
  }

  private void clearBatchQuietly() {
    try {
      statement.clearBatch();
    } catch (SQLException e) {
      // Nothing left to clear, or the statement is unusable; either way the next execute says so.
    }
  }

  @Override
  public String toString() {
    return "RecoverableBatch["
        + contract.name()
        + ", "
        + bindRows.size()
        + " pending"
        + (transactional ? "" : ", no transactions")
        + "]";
  }
}
