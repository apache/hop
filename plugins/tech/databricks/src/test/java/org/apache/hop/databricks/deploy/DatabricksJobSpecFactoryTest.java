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

package org.apache.hop.databricks.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class DatabricksJobSpecFactoryTest {

  @Test
  void createJobJsonContainsMainSparkAndPaths() throws Exception {
    String json =
        DatabricksJobSpecFactory.buildCreateJobJson(
            "hop-job",
            "cluster-1",
            "/FileStore/hop/j.jar",
            "/FileStore/hop/p.hpl",
            "/FileStore/hop/m.json",
            "spark-local");
    assertTrue(json.contains("hop-job"), json);
    assertTrue(json.contains("cluster-1"), json);
    assertTrue(json.contains("MainSpark"), json);
    assertTrue(json.contains("j.jar"), json);
    assertTrue(json.contains("p.hpl"), json);
    assertTrue(json.contains("spark-local"), json);
  }

  @Test
  void resetJobJsonIncludesJobId() throws Exception {
    String json =
        DatabricksJobSpecFactory.buildResetJobJson(
            42L, "n", "c", "/a.jar", "/b.hpl", "/c.json", "rc");
    assertTrue(json.contains("\"job_id\":42") || json.contains("\"job_id\": 42"));
    assertTrue(json.contains("new_settings"));
  }

  @Test
  void toDbfsUriNormalizes() throws Exception {
    assertEquals("dbfs:/x", DatabricksJobSpecFactory.toDbfsUri("/x"));
    assertEquals("dbfs:/x", DatabricksJobSpecFactory.toDbfsUri("dbfs:/x"));
  }

  @Test
  void missingClusterFails() {
    assertThrows(
        HopException.class,
        () -> DatabricksJobSpecFactory.buildCreateJobJson("n", "", "/j", "/p", "/m", "rc"));
  }
}
