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

package org.apache.hop.execution.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.core.failure.FailureCause;
import org.apache.hop.core.failure.FailureComponent;
import org.apache.hop.core.failure.FailureRun;
import org.junit.jupiter.api.Test;

class ExecutionFailuresDescribeTest {

  @Test
  void noFailure() {
    assertNull(ExecutionFailures.describe(null));
  }

  @Test
  void theRootCauseIsDescribed() {
    FailureCause cause = new FailureCause();
    // Hop messages start with a line break and repeat the nested messages
    cause.setMessage("\nError reading from the database\nConnection refused\n");
    cause.setRootCauseMessage("Connection refused");
    ExecutionFailure transformFailure = new ExecutionFailure();
    transformFailure.setCategory(FailureCategory.CONNECTIVITY);
    transformFailure.setComponent(
        new FailureComponent(FailureComponent.KIND_TRANSFORM, "Read orders", "TableInput", 0, "t"));
    transformFailure.setCause(cause);
    ExecutionFailure pipelineFailure = new ExecutionFailure();
    pipelineFailure.setCausedBy(transformFailure);

    assertEquals(
        "Transform 'Read orders' (TableInput) failed [CONNECTIVITY]: Connection refused",
        ExecutionFailures.describe(pipelineFailure));
  }

  @Test
  void firstLineOfTheMessageWithoutARootCauseMessage() {
    FailureCause cause = new FailureCause();
    cause.setMessage("\nUnable to open the file\nmore detail\n");
    ExecutionFailure failure = new ExecutionFailure();
    failure.setCause(cause);
    failure.setRun(new FailureRun("id", "PIPELINE", "orders", null, null));

    assertEquals(
        "'orders' failed [UNKNOWN]: Unable to open the file", ExecutionFailures.describe(failure));
  }

  @Test
  void nothingKnown() {
    assertEquals("The run failed [UNKNOWN]", ExecutionFailures.describe(new ExecutionFailure()));
  }
}
