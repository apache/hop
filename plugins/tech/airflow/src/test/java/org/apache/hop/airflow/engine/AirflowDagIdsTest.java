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
package org.apache.hop.airflow.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.airflow.launcher.BundleManifest;
import org.apache.hop.airflow.launcher.HopWorkflowFile;
import org.junit.jupiter.api.Test;

class AirflowDagIdsTest {

  /** Hop and the launcher on the workers must name the DAG of a workflow the same way. */
  @Test
  void namesDagsAsTheLauncherDoes() {
    String[] projects = {"demo", "My Project", "sales-2026"};
    String[] paths = {"hello.hwf", "flows/load data.hwf", "a/b/c (copy).hwf", "x.y.hwf"};
    for (String project : projects) {
      for (String path : paths) {
        assertEquals(
            HopWorkflowFile.dagId(BundleManifest.defaultDagIdPrefix(project), path),
            AirflowDagIds.dagId(null, project, path),
            project + " " + path);
        assertEquals(
            HopWorkflowFile.dagId("team.", path), AirflowDagIds.dagId("team.", project, path));
      }
    }
  }

  @Test
  void usesTheProjectAsTheDefaultPrefix() {
    assertEquals(
        "My_Project__flows__load_data",
        AirflowDagIds.dagId("", "My Project", "flows/load data.hwf"));
  }
}
