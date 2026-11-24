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
package org.apache.hop.airflow.bundle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AirflowBundleWriterRegistryTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  /** The stub lists what IAction.isEvaluation() says, also for actions of other plugins. */
  @Test
  void listsTheRegisteredActionsThatDoNotEvaluate() {
    List<String> notEvaluating = AirflowBundleWriter.registeredNotEvaluatingActions();

    assertTrue(notEvaluating.contains("SPECIAL"), notEvaluating.toString());
    assertTrue(notEvaluating.contains("DUMMY"), notEvaluating.toString());
    assertFalse(notEvaluating.contains("AIRFLOW_TRIGGER_DAG_RUN"), notEvaluating.toString());
  }
}
