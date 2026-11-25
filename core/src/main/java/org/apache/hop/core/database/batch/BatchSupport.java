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

import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.variables.IVariables;

/**
 * The resolved answer to "what happens when a batch fails on this connection", with where that
 * answer came from. Transforms ask this instead of reading the dialect flags themselves, so the
 * dialect declaration, the connection option and the variable are combined in one place.
 *
 * <p>Only {@link BatchFailureContract#isRecovered() a validated contract} makes a transform recover
 * failed batches with {@link RecoverableBatch}. {@link BatchFailureContract#UNKNOWN} keeps the way
 * a failed batch was always handled; it is what every dialect gets until its driver's behaviour has
 * been measured, and what a user picks to go back to that behaviour.
 *
 * @param contract the contract to recover by, never null
 * @param source where the contract came from, for the log
 */
public record BatchSupport(BatchFailureContract contract, Source source) {

  /**
   * Connection attribute that overrides the dialect's declared contract for one connection. The
   * value is the code of a {@link BatchFailureContract}, or a variable expression; empty means the
   * dialect's own. {@link BatchFailureContract#UNKNOWN} goes back to the way a failed batch was
   * always handled. It is deliberately not an extra JDBC option, so it never reaches the driver.
   */
  public static final String ATTRIBUTE_BATCH_FAILURE_CONTRACT = "BATCH_FAILURE_CONTRACT";

  /**
   * Value of {@link Const#HOP_DATABASE_BATCH_RECOVERY} that makes every connection handle a failed
   * batch the way it always was, whatever its dialect or connection option says.
   */
  public static final String RECOVERY_LEGACY = "LEGACY";

  /** Where a resolved contract came from. */
  public enum Source {
    /** {@link Const#HOP_DATABASE_BATCH_RECOVERY} is set to {@link #RECOVERY_LEGACY}. */
    VARIABLE,
    /** The connection's {@link #ATTRIBUTE_BATCH_FAILURE_CONTRACT} attribute. */
    CONNECTION,
    /** The dialect's {@link IDatabase#getBatchFailureContract()}. */
    DIALECT,
    /** The dialect declares no contract: it has not been validated. */
    NOT_DECLARED
  }

  /**
   * Resolves the contract for a connection.
   *
   * <p>In order: a dialect that can't batch never batches; {@code
   * HOP_DATABASE_BATCH_RECOVERY=LEGACY} gives {@link BatchFailureContract#UNKNOWN} everywhere; the
   * connection attribute wins over the dialect; a dialect that declares nothing is {@link
   * BatchFailureContract#UNKNOWN}.
   *
   * @param dialect the database dialect
   * @param variables used to resolve the override variable and attribute values, may be null
   * @return the resolved support, never null
   */
  public static BatchSupport of(IDatabase dialect, IVariables variables) {
    if (!dialect.isSupportsBatchUpdates()) {
      return new BatchSupport(BatchFailureContract.NO_BATCH_SUPPORT, Source.DIALECT);
    }

    if (isLegacyRecovery(variables)) {
      return new BatchSupport(BatchFailureContract.UNKNOWN, Source.VARIABLE);
    }

    Map<String, String> attributes = dialect.getAttributes();
    if (attributes != null) {
      String value = attributes.get(ATTRIBUTE_BATCH_FAILURE_CONTRACT);
      if (variables != null && value != null) {
        value = variables.resolve(value);
      }
      BatchFailureContract override = BatchFailureContract.lookupCode(value);
      if (override != null && override != BatchFailureContract.NO_BATCH_SUPPORT) {
        return new BatchSupport(override, Source.CONNECTION);
      }
    }

    BatchFailureContract declared = dialect.getBatchFailureContract();
    if (declared != null) {
      return new BatchSupport(declared, Source.DIALECT);
    }
    return new BatchSupport(BatchFailureContract.UNKNOWN, Source.NOT_DECLARED);
  }

  /**
   * Whether {@link Const#HOP_DATABASE_BATCH_RECOVERY} asks for the handling as it always was, on
   * every connection. A transform that changes more than how a failed batch is recovered, such as
   * when it passes rows on, keeps that as before too.
   *
   * @param variables used to resolve the variable, may be null
   * @return true if the variable is set to {@link #RECOVERY_LEGACY}
   */
  public static boolean isLegacyRecovery(IVariables variables) {
    String recovery =
        variables == null ? null : variables.getVariable(Const.HOP_DATABASE_BATCH_RECOVERY);
    return RECOVERY_LEGACY.equalsIgnoreCase(Const.NVL(recovery, "").trim());
  }

  /**
   * @return true if failed batches on this connection are recovered with {@link RecoverableBatch}
   */
  public boolean isRecovered() {
    return contract.isRecovered();
  }

  /**
   * @return a line for the transform log that says which contract is used and why
   */
  public String describe() {
    return "batch failure contract " + contract.name() + " (from " + source.name() + ")";
  }
}
