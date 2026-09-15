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

package org.apache.hop.workflow.actions.dbt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DbtRunResultTest {

  private static final String JSON =
      """
      {
        "metadata": {"dbt_version": "1.8.0"},
        "results": [
          {"unique_id": "model.shop.stg_orders", "status": "success",
           "execution_time": 0.42, "message": null,
           "relation_name": "\\"analytics\\".\\"staging\\".\\"orders\\""},
          {"unique_id": "test.shop.not_null_orders_id", "status": "fail",
           "execution_time": 0.11, "message": "Got 3 results, expected 0.",
           "relation_name": null}
        ],
        "elapsed_time": 1.23
      }
      """;

  @Test
  void parsesNodesAndElapsed() throws IOException {
    DbtRunResult r = DbtRunResult.parse(JSON);
    assertEquals(2, r.getNodes().size());
    assertEquals(1.23, r.getElapsedTime(), 0.0001);

    DbtNodeResult ok = r.getNodes().get(0);
    assertEquals("model.shop.stg_orders", ok.getUniqueId());
    assertEquals("success", ok.getStatus());
    assertEquals(0.42, ok.getExecutionTime(), 0.0001);
    assertEquals("\"analytics\".\"staging\".\"orders\"", ok.getRelationName());
    assertFalse(ok.isFailure());
  }

  @Test
  void detectsFailures() throws IOException {
    DbtRunResult r = DbtRunResult.parse(JSON);
    assertTrue(r.hasFailures());
    assertEquals(1, r.countFailures());
    assertTrue(r.getNodes().get(1).isFailure());
  }

  @TempDir Path targetDir;

  @Test
  void readsRunResultsFromDisk() throws IOException {
    Path file = targetDir.resolve("run_results.json");
    Files.writeString(file, JSON);

    DbtRunResult r = DbtRunResult.fromFile(file);

    assertEquals(2, r.getNodes().size());
    assertTrue(r.hasFailures());
  }

  @Test
  void missingRunResultsIsReportedAsAnIoError() {
    assertThrows(
        IOException.class, () -> DbtRunResult.fromFile(targetDir.resolve("run_results.json")));
  }

  @Test
  void emptyResultsHaveNoFailures() throws IOException {
    DbtRunResult r = DbtRunResult.parse("{\"results\": [], \"elapsed_time\": 0.0}");
    assertFalse(r.hasFailures());
    assertEquals(0, r.getNodes().size());
  }
}
