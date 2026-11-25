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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BatchSupportTest {

  private IDatabase dialect;
  private Map<String, String> attributes;
  private IVariables variables;

  @BeforeEach
  void setUp() {
    // A mock of the interface, not of BaseDatabaseMeta: third-party dialects implement IDatabase
    // directly, and the resolution must not depend on the base class.
    dialect = mock(IDatabase.class);
    attributes = new HashMap<>();
    when(dialect.getAttributes()).thenReturn(attributes);
    when(dialect.isSupportsBatchUpdates()).thenReturn(true);
    variables = new Variables();
  }

  @Test
  void aDialectThatDeclaresNothingIsUnknownAndKeepsTheOldHandling() {
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.UNKNOWN, support.contract());
    assertEquals(BatchSupport.Source.NOT_DECLARED, support.source());
    assertFalse(support.isRecovered());
  }

  @Test
  void neitherTheOldErrorHandlingFlagNorSavepointsAreTakenAsAContract() {
    // A contract is only ever declared from a measurement, never derived from older flags.
    when(dialect.IsSupportsErrorHandlingOnBatchUpdates()).thenReturn(true);
    when(dialect.isUseSafePoints()).thenReturn(true);
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.UNKNOWN, support.contract());
    assertFalse(support.isRecovered());
  }

  @Test
  void aDeclaredContractIsRecovered() {
    for (BatchFailureContract contract :
        new BatchFailureContract[] {
          BatchFailureContract.MARKS_FAILED_ROWS,
          BatchFailureContract.KEEPS_PREFIX,
          BatchFailureContract.LOSES_WHOLE_BATCH
        }) {
      when(dialect.getBatchFailureContract()).thenReturn(contract);
      BatchSupport support = BatchSupport.of(dialect, variables);
      assertEquals(contract, support.contract());
      assertEquals(BatchSupport.Source.DIALECT, support.source());
      assertTrue(support.isRecovered(), contract.name());
    }
  }

  @Test
  void aDialectThatDeclaresUnknownKeepsTheOldHandling() {
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.UNKNOWN);
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.UNKNOWN, support.contract());
    assertFalse(support.isRecovered());
  }

  @Test
  void theConnectionAttributeWinsOverTheDialect() {
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.KEEPS_PREFIX);
    variables.setVariable("CONTRACT", "marks_failed_rows");
    attributes.put(BatchSupport.ATTRIBUTE_BATCH_FAILURE_CONTRACT, "${CONTRACT}");
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.MARKS_FAILED_ROWS, support.contract());
    assertEquals(BatchSupport.Source.CONNECTION, support.source());
  }

  @Test
  void unknownOnTheConnectionGoesBackToTheOldHandlingOfAValidatedDialect() {
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.LOSES_WHOLE_BATCH);
    attributes.put(BatchSupport.ATTRIBUTE_BATCH_FAILURE_CONTRACT, "UNKNOWN");
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.UNKNOWN, support.contract());
    assertEquals(BatchSupport.Source.CONNECTION, support.source());
    assertFalse(support.isRecovered());
  }

  @Test
  void anEmptyUnknownOrNoBatchAttributeMeansTheDialects() {
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.KEEPS_PREFIX);
    for (String value : new String[] {"", "  ", "nonsense", "NO_BATCH_SUPPORT"}) {
      attributes.put(BatchSupport.ATTRIBUTE_BATCH_FAILURE_CONTRACT, value);
      assertEquals(
          BatchFailureContract.KEEPS_PREFIX, BatchSupport.of(dialect, variables).contract(), value);
    }
  }

  @Test
  void theLegacyVariableWinsOverEverythingButMissingBatchSupport() {
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.MARKS_FAILED_ROWS);
    attributes.put(BatchSupport.ATTRIBUTE_BATCH_FAILURE_CONTRACT, "KEEPS_PREFIX");
    variables.setVariable(Const.HOP_DATABASE_BATCH_RECOVERY, " legacy ");
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.UNKNOWN, support.contract());
    assertEquals(BatchSupport.Source.VARIABLE, support.source());
    assertFalse(support.isRecovered());

    when(dialect.isSupportsBatchUpdates()).thenReturn(false);
    assertEquals(
        BatchFailureContract.NO_BATCH_SUPPORT, BatchSupport.of(dialect, variables).contract());
  }

  @Test
  void anyOtherValueOfTheVariableFollowsTheConnection() {
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.MARKS_FAILED_ROWS);
    for (String value : new String[] {"", "AUTO", "SAFE"}) {
      variables.setVariable(Const.HOP_DATABASE_BATCH_RECOVERY, value);
      assertEquals(
          BatchFailureContract.MARKS_FAILED_ROWS,
          BatchSupport.of(dialect, variables).contract(),
          value);
    }
  }

  @Test
  void aDialectWithoutBatchSupportIsNeverRecovered() {
    when(dialect.isSupportsBatchUpdates()).thenReturn(false);
    when(dialect.getBatchFailureContract()).thenReturn(BatchFailureContract.MARKS_FAILED_ROWS);
    BatchSupport support = BatchSupport.of(dialect, variables);
    assertEquals(BatchFailureContract.NO_BATCH_SUPPORT, support.contract());
    assertFalse(support.isRecovered());
  }

  @Test
  void nullAttributesAndVariablesAreTolerated() {
    when(dialect.getAttributes()).thenReturn(null);
    assertEquals(BatchFailureContract.UNKNOWN, BatchSupport.of(dialect, null).contract());
  }
}
