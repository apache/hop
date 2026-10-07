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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.maskfields.store.DatabaseMaskingStore;
import org.apache.hop.pipeline.transforms.maskfields.store.MemoryMaskingStore;

/** One place for the sequences and mapping tables used by every copy of Mask fields in this JVM. */
public final class MaskingRuntime {

  private static final MaskingRuntime INSTANCE = new MaskingRuntime();

  private final ConcurrentHashMap<String, ExecutionState> executions = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, DatabaseEntry> databases = new ConcurrentHashMap<>();

  public static MaskingRuntime getInstance() {
    return INSTANCE;
  }

  private MaskingRuntime() {}

  /** Borrows the state for one pipeline execution. Release the lease from every copy. */
  public Lease acquire(String executionId) {
    while (true) {
      ExecutionState state = executions.computeIfAbsent(executionId, id -> new ExecutionState());
      synchronized (state) {
        if (!state.closed) {
          state.refs++;
          return new Lease(executionId, state);
        }
      }
    }
  }

  /** Remembered values and sequences for one pipeline execution. */
  public final class Lease {
    private final String executionId;
    private final ExecutionState state;
    private final Set<String> databaseKeys = ConcurrentHashMap.newKeySet();
    private boolean released;

    private Lease(String executionId, ExecutionState state) {
      this.executionId = executionId;
      this.state = state;
    }

    public MemoryMaskingStore memory() {
      return state.memory;
    }

    /**
     * Sequence for one field that does not remember values. Copies of that field share it. A second
     * field keeps its own counter.
     */
    public AtomicLong sequence(String transformName, String fieldName, long start) {
      String key = transformName + "\0" + fieldName;
      return state.sequences.computeIfAbsent(key, name -> new AtomicLong(start));
    }

    /**
     * One open connection for this database target, shared by every copy in the JVM. The target is
     * the resolved URL and user, so two projects with a connection of the same name that point at
     * different databases do not share a store.
     */
    public DatabaseMaskingStore database(
        ILoggingObject parent,
        IVariables variables,
        DatabaseMeta databaseMeta,
        String schemaName,
        String tableName)
        throws HopException {
      String key = databaseKey(variables, databaseMeta, schemaName, tableName);
      while (true) {
        DatabaseEntry entry = databases.computeIfAbsent(key, name -> new DatabaseEntry());
        synchronized (entry) {
          if (entry.closed) {
            databases.remove(key, entry);
            continue;
          }
          if (entry.store == null) {
            entry.store =
                new DatabaseMaskingStore(parent, variables, databaseMeta, schemaName, tableName);
            try {
              entry.store.open();
            } catch (HopException e) {
              entry.closed = true;
              databases.remove(key, entry);
              entry.store.close();
              throw e;
            }
          }
          if (databaseKeys.add(key)) {
            entry.refs++;
          }
          return entry.store;
        }
      }
    }

    /** Drops this copy's claim. The last claim closes what it was holding. */
    public void release() {
      synchronized (this) {
        if (released) {
          return;
        }
        released = true;
      }
      synchronized (state) {
        state.refs--;
        if (state.refs == 0) {
          state.closed = true;
          executions.remove(executionId, state);
          state.memory.close();
        }
      }
      for (String key : databaseKeys) {
        DatabaseEntry entry = databases.get(key);
        if (entry == null) {
          continue;
        }
        synchronized (entry) {
          entry.refs--;
          if (entry.refs == 0) {
            entry.closed = true;
            databases.remove(key, entry);
            if (entry.store != null) {
              entry.store.close();
            }
          }
        }
      }
    }
  }

  static String databaseKey(
      IVariables variables, DatabaseMeta databaseMeta, String schemaName, String tableName)
      throws HopException {
    String url = Const.NVL(databaseMeta.getURL(variables), "");
    String user = Const.NVL(variables.resolve(databaseMeta.getUsername()), "");
    String schema = Const.NVL(schemaName, "");
    String table = Const.NVL(tableName, "");
    return url + "\0" + user + "\0" + schema + "\0" + table;
  }

  private static final class ExecutionState {
    private final MemoryMaskingStore memory = new MemoryMaskingStore();
    private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<>();
    private int refs;
    private boolean closed;
  }

  private static final class DatabaseEntry {
    private DatabaseMaskingStore store;
    private int refs;
    private boolean closed;
  }
}
