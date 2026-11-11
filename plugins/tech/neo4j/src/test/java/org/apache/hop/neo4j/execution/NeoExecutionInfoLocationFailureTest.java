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

package org.apache.hop.neo4j.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.core.failure.FailureComponent;
import org.junit.jupiter.api.Test;

class NeoExecutionInfoLocationFailureTest {

  @Test
  void failureIsStoredAsJsonAndReadBack() {
    ExecutionFailure transformFailure = new ExecutionFailure();
    transformFailure.setCategory(FailureCategory.CONSTRAINT_VIOLATION);
    transformFailure.setComponent(
        new FailureComponent(
            FailureComponent.KIND_TRANSFORM, "Write orders", "TableOutput", 0, "t1"));
    ExecutionFailure pipelineFailure = new ExecutionFailure();
    pipelineFailure.setCategory(FailureCategory.CONSTRAINT_VIOLATION);
    pipelineFailure.setCausedBy(transformFailure);

    String json = NeoExecutionInfoLocation.failureToJson(pipelineFailure);
    ExecutionFailure read = NeoExecutionInfoLocation.failureFromJson(json);

    assertEquals(FailureCategory.CONSTRAINT_VIOLATION, read.getCategory());
    assertEquals("Write orders", read.getRootCause().getComponent().getName());
  }

  @Test
  void noFailureAndUnreadableFailures() {
    assertNull(NeoExecutionInfoLocation.failureToJson(null));
    assertNull(NeoExecutionInfoLocation.failureFromJson(null));
    assertNull(NeoExecutionInfoLocation.failureFromJson(""));
    assertNull(NeoExecutionInfoLocation.failureFromJson("not json"));
    // Unknown fields from a newer Hop are skipped
    ExecutionFailure read =
        NeoExecutionInfoLocation.failureFromJson("{\"category\":\"RESOURCE\",\"later\":1}");
    assertSame(FailureCategory.RESOURCE, read.getCategory());
  }
}
