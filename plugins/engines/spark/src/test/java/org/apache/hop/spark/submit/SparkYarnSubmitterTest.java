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

package org.apache.hop.spark.submit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.Test;

class SparkYarnSubmitterTest {

  @Test
  void anAcceptedOrRunningApplicationIsNotFinal() {
    for (YarnApplicationState state :
        new YarnApplicationState[] {
          YarnApplicationState.NEW,
          YarnApplicationState.SUBMITTED,
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.RUNNING
        }) {
      SparkDriverStatus status =
          SparkYarnSubmitter.toStatus(state, FinalApplicationStatus.UNDEFINED, "", "");
      assertFalse(status.isFinal(), state.name());
      assertEquals(state.name(), status.state());
      assertNull(status.location());
    }
  }

  @Test
  void onlyAFinishedApplicationThatSucceededIsASuccess() {
    SparkDriverStatus succeeded =
        SparkYarnSubmitter.toStatus(
            YarnApplicationState.FINISHED,
            FinalApplicationStatus.SUCCEEDED,
            "http://rm:8088/proxy/application_1_0001/",
            "");
    assertTrue(succeeded.isFinal());
    assertTrue(succeeded.isSuccess());
    assertEquals("http://rm:8088/proxy/application_1_0001/", succeeded.location());

    // The application master ends FINISHED also when the user class (MainSpark) failed
    SparkDriverStatus failed =
        SparkYarnSubmitter.toStatus(
            YarnApplicationState.FINISHED,
            FinalApplicationStatus.FAILED,
            null,
            "User class threw exception");
    assertTrue(failed.isFinal());
    assertFalse(failed.isSuccess());
    assertEquals("FINISHED/FAILED", failed.state());
    assertEquals("User class threw exception", failed.message());

    for (YarnApplicationState state :
        new YarnApplicationState[] {YarnApplicationState.FAILED, YarnApplicationState.KILLED}) {
      SparkDriverStatus status =
          SparkYarnSubmitter.toStatus(state, FinalApplicationStatus.KILLED, null, null);
      assertTrue(status.isFinal());
      assertFalse(status.isSuccess());
    }
  }

  @Test
  void theMasterPicksTheSubmitter() throws Exception {
    SparkClusterRun run =
        new SparkClusterRun(
            LogChannel.GENERAL,
            new Variables(),
            null,
            new PipelineMeta(),
            "cluster",
            new SparkPipelineRunConfiguration());
    assertInstanceOf(SparkYarnSubmitter.class, run.createSubmitter("yarn", Map.of()));
    ISparkClusterSubmitter standalone = run.createSubmitter("spark://spark:7077", Map.of());
    assertInstanceOf(SparkStandaloneRestSubmitter.class, standalone);
    assertEquals("http://spark:6066", standalone.describe());
    assertInstanceOf(
        SparkKubernetesSubmitter.class, run.createSubmitter("k8s://https://k8s:6443", Map.of()));
    assertThrows(HopException.class, () -> run.createSubmitter("mesos://m:5050", Map.of()));
  }
}
