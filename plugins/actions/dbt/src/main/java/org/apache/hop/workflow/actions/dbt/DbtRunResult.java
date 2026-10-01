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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Parsed view of dbt's {@code target/run_results.json}: the per-node statuses, timings and
 * messages. Used to surface structured per-model outcomes into the Hop log and to drive the
 * action's success/error routing.
 */
public final class DbtRunResult {

  private final List<DbtNodeResult> nodes;
  private final double elapsedTime;

  private DbtRunResult(List<DbtNodeResult> nodes, double elapsedTime) {
    this.nodes = nodes;
    this.elapsedTime = elapsedTime;
  }

  public List<DbtNodeResult> getNodes() {
    return nodes;
  }

  public double getElapsedTime() {
    return elapsedTime;
  }

  public boolean hasFailures() {
    return nodes.stream().anyMatch(DbtNodeResult::isFailure);
  }

  public long countFailures() {
    return nodes.stream().filter(DbtNodeResult::isFailure).count();
  }

  /** Reads and parses a {@code run_results.json} written by dbt. */
  public static DbtRunResult fromFile(Path file) throws IOException {
    if (!Files.isRegularFile(file)) {
      throw new IOException("dbt run_results.json not found at " + file);
    }
    return parse(Files.readString(file));
  }

  static DbtRunResult parse(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json);
    List<DbtNodeResult> nodes = new ArrayList<>();
    JsonNode results = root.path("results");
    if (results.isArray()) {
      for (JsonNode r : results) {
        nodes.add(
            new DbtNodeResult(
                text(r, "unique_id"),
                text(r, "status"),
                r.path("execution_time").asDouble(0.0),
                text(r, "message"),
                text(r, "relation_name")));
      }
    }
    double elapsed = root.path("elapsed_time").asDouble(0.0);
    return new DbtRunResult(nodes, elapsed);
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.path(field);
    return v.isMissingNode() || v.isNull() ? null : v.asText();
  }
}
