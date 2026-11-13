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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.junit.jupiter.api.Test;

class SparkKubernetesSubmitterTest {

  @Test
  void aPendingOrRunningDriverPodIsNotFinal() {
    for (String phase : new String[] {"Pending", "Running", "Unknown"}) {
      SparkDriverStatus status = SparkKubernetesSubmitter.toStatus(pod(phase), "ns/hop-1");
      assertFalse(status.isFinal(), phase);
      assertEquals(phase.toUpperCase(), status.state());
      assertEquals("ns/hop-1", status.location());
    }
  }

  @Test
  void onlyASucceededDriverPodIsASuccess() {
    SparkDriverStatus succeeded = SparkKubernetesSubmitter.toStatus(pod("Succeeded"), "ns/hop-1");
    assertTrue(succeeded.isFinal());
    assertTrue(succeeded.isSuccess());
    assertNull(succeeded.message());

    Pod failedPod =
        new PodBuilder(pod("Failed"))
            .editStatus()
            .addNewContainerStatus()
            .withName("spark-kubernetes-driver")
            .withNewState()
            .withNewTerminated()
            .withExitCode(1)
            .withReason("Error")
            .endTerminated()
            .endState()
            .endContainerStatus()
            .endStatus()
            .build();
    SparkDriverStatus failed = SparkKubernetesSubmitter.toStatus(failedPod, "ns/hop-1");
    assertTrue(failed.isFinal());
    assertFalse(failed.isSuccess());
    assertEquals(
        "container spark-kubernetes-driver ended with exit code 1 (Error)", failed.message());
  }

  @Test
  void aDriverPodThatIsGoneIsAFailure() {
    SparkDriverStatus gone = SparkKubernetesSubmitter.toStatus(null, "ns/hop-1");
    assertTrue(gone.isFinal());
    assertFalse(gone.isSuccess());
    assertEquals("GONE", gone.state());
  }

  @Test
  void theApiServerComesFromTheMaster() {
    assertEquals(
        "https://k8s.example.com:6443",
        new SparkKubernetesSubmitter(LogChannel.GENERAL, "k8s://https://k8s.example.com:6443")
            .apiServer());
    assertEquals(
        "https://10.0.0.1:443",
        new SparkKubernetesSubmitter(LogChannel.GENERAL, "k8s://10.0.0.1:443").apiServer());
  }

  @Test
  void anImageIsRequired() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                new SparkKubernetesSubmitter(LogChannel.GENERAL, "k8s://https://k8s:6443")
                    .submit("http://staging/hop.jar", "Main", List.of(), Map.of(), Map.of()));
    assertTrue(e.getMessage().contains("spark.kubernetes.container.image"), e.getMessage());
  }

  /**
   * A spark-submit that hangs keeps its output open. The timeout still ends it: the output is read
   * on a thread of its own.
   */
  @Test
  void aSubmissionThatHangsEndsAtTheTimeout() {
    SparkKubernetesSubmitter submitter =
        new SparkKubernetesSubmitter(LogChannel.GENERAL, "k8s://https://k8s:6443");
    long start = System.currentTimeMillis();
    HopException e =
        assertThrows(
            HopException.class, () -> submitter.runSubmitProcess(javaCommand(Hangs.class), 2000L));
    assertTrue(e.getMessage().contains("did not finish within 2 s"), e.getMessage());
    assertTrue(e.getMessage().contains("submitting"), "keeps what it wrote: " + e.getMessage());
    assertTrue(System.currentTimeMillis() - start < 30_000L, "ended at the timeout");
  }

  /** Stop while spark-submit runs: the process ends and the submission fails as cancelled. */
  @Test
  void aCancelEndsTheSubmission() throws Exception {
    SparkKubernetesSubmitter submitter =
        new SparkKubernetesSubmitter(LogChannel.GENERAL, "k8s://https://k8s:6443");
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread submit =
        new Thread(
            () -> {
              try {
                submitter.runSubmitProcess(javaCommand(Hangs.class), 300_000L);
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    submit.start();
    Thread.sleep(1000L);
    submitter.cancelSubmission();
    submit.join(30_000L);
    assertFalse(submit.isAlive(), "the cancel ended the submission");
    assertTrue(failure.get() instanceof HopException, String.valueOf(failure.get()));
    assertTrue(
        failure.get().getMessage().contains(SparkKubernetesSubmitter.CANCELLED),
        failure.get().getMessage());
  }

  @Test
  void aFailedSubmissionShowsWhatSparkSubmitWrote() {
    SparkKubernetesSubmitter submitter =
        new SparkKubernetesSubmitter(LogChannel.GENERAL, "k8s://https://k8s:6443");
    HopException e =
        assertThrows(
            HopException.class,
            () -> submitter.runSubmitProcess(javaCommand(Fails.class), 60_000L));
    assertTrue(e.getMessage().contains("exit code 3"), e.getMessage());
    assertTrue(e.getMessage().contains("no such image"), e.getMessage());
  }

  /** Stands in for a spark-submit that never comes back. */
  public static class Hangs {
    public static void main(String[] args) throws Exception {
      System.out.println("submitting");
      System.out.flush();
      Thread.sleep(600_000L);
    }
  }

  /** Stands in for a spark-submit that fails. */
  public static class Fails {
    public static void main(String[] args) {
      System.out.println("no such image");
      System.exit(3);
    }
  }

  private static List<String> javaCommand(Class<?> main) {
    return List.of(
        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp",
        System.getProperty("java.class.path"),
        main.getName());
  }

  private static Pod pod(String phase) {
    return new PodBuilder()
        .withNewMetadata()
        .withName("hop-1")
        .withNamespace("ns")
        .endMetadata()
        .withNewStatus()
        .withPhase(phase)
        .endStatus()
        .build();
  }
}
