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

package org.apache.hop.pipeline.transforms.dbproc;

import java.util.function.BooleanSupplier;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;

/**
 * Decides whether Get fields can use driver metadata or has to run the procedure. The confirmation
 * supplier is called only when the driver did not describe the result set.
 */
final class DBProcResultFieldLookup {
  private DBProcResultFieldLookup() {}

  enum Choice {
    DESCRIBE_WITHOUT_RUNNING,
    EXECUTE,
    CANCELLED
  }

  @FunctionalInterface
  interface ResultFieldSource {
    IRowMeta get() throws HopDatabaseException;
  }

  static Choice choose(boolean describedWithoutRunning, BooleanSupplier confirmExecution) {
    if (describedWithoutRunning) {
      return Choice.DESCRIBE_WITHOUT_RUNNING;
    }
    if (confirmExecution != null && confirmExecution.getAsBoolean()) {
      return Choice.EXECUTE;
    }
    return Choice.CANCELLED;
  }

  /**
   * @return the columns the driver already described, the columns from {@code execute}, or {@code
   *     null} when the procedure would have to run and the user declined
   */
  static IRowMeta fields(
      IRowMeta describedWithoutRunning, BooleanSupplier confirmExecution, ResultFieldSource execute)
      throws HopDatabaseException {
    return switch (choose(describedWithoutRunning != null, confirmExecution)) {
      case DESCRIBE_WITHOUT_RUNNING -> describedWithoutRunning;
      case CANCELLED -> null;
      case EXECUTE -> execute.get();
    };
  }
}
