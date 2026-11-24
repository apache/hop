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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * What a JDBC driver does to a prepared-statement batch when one of its rows fails, and what {@link
 * java.sql.BatchUpdateException#getUpdateCounts()} means afterwards.
 *
 * <p>Declare a value for a dialect only after measuring it against a real server with {@link
 * BatchContractProbe}: a batch with duplicate primary keys in it, then the counts array and the
 * rows that survive the commit. Without that, the dialect stays {@link #UNKNOWN}, which keeps the
 * way Hop always handled a failed batch.
 */
public enum BatchFailureContract implements IEnumHasCodeAndDescription {

  /**
   * The driver runs the whole batch and returns one count per row, with {@link
   * java.sql.Statement#EXECUTE_FAILED} at the rows that failed. The other rows stay applied.
   */
  MARKS_FAILED_ROWS,

  /**
   * The driver stops at the first failure. The counts array holds only the rows before it, so its
   * length is the index of the failing row; the rows behind it were never sent. A failure on the
   * first row gives a zero-length array.
   */
  KEEPS_PREFIX,

  /** The failure aborts the transaction: no row of the batch survives, whatever the counts say. */
  LOSES_WHOLE_BATCH,

  /** The driver or database can't run batches at all. */
  NO_BATCH_SUPPORT,

  /**
   * Not validated against this database, or switched back on purpose: a failed batch is handled the
   * way Hop always handled it, without {@link RecoverableBatch}.
   */
  UNKNOWN;

  private static final Class<?> PKG = BatchFailureContract.class;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, "BatchFailureContract." + name() + ".Description");
  }

  /**
   * @return true if the update counts in a failed batch can be trusted to say which rows were
   *     applied, so recovery can keep those rows and only deal with the others.
   */
  public boolean hasTrustworthyCounts() {
    return this == MARKS_FAILED_ROWS || this == KEEPS_PREFIX;
  }

  /**
   * @return true if a failed batch is recovered with {@link RecoverableBatch}; false for {@link
   *     #UNKNOWN} and {@link #NO_BATCH_SUPPORT}, which keep the handling Hop always had
   */
  public boolean isRecovered() {
    return this == MARKS_FAILED_ROWS || this == KEEPS_PREFIX || this == LOSES_WHOLE_BATCH;
  }

  /**
   * Finds a contract by its code, ignoring case.
   *
   * @param code the code, usually as stored in a connection attribute or variable
   * @return the matching contract, or null if the code is empty or unknown
   */
  public static BatchFailureContract lookupCode(String code) {
    if (code == null || code.isBlank()) {
      return null;
    }
    for (BatchFailureContract contract : values()) {
      if (contract.name().equalsIgnoreCase(code.trim())) {
        return contract;
      }
    }
    return null;
  }
}
