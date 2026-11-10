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

package org.apache.hop.pipeline.transforms.language;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import java.lang.reflect.Field;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

@Timeout(10)
class DetectLanguageAsyncTest {
  private DetectLanguage transform;
  private TransformMeta transformMeta;
  private final Object[] row = {"<p>text</p>"};

  @BeforeEach
  void setUp() throws Exception {
    org.apache.hop.core.logging.HopLogStore.init();
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.setCorpusField("input");
    meta.setParallelism(true);
    DetectLanguageData data = new DetectLanguageData();
    data.outputRowMeta = new RowMeta();
    PipelineMeta pipelineMeta = new PipelineMeta();
    transformMeta = spy(new TransformMeta("test", meta));
    pipelineMeta.addTransform(transformMeta);
    try (MockedStatic<LanguageDetectorBuilder> factory =
        mockStatic(LanguageDetectorBuilder.class)) {
      LanguageDetectorBuilder builder = mock(LanguageDetectorBuilder.class, RETURNS_SELF);
      LanguageDetector detector = mock(LanguageDetector.class);
      factory.when(LanguageDetectorBuilder::fromAllSpokenLanguages).thenReturn(builder);
      when(builder.build()).thenReturn(detector);
      when(detector.computeLanguageConfidenceValues(anyString())).thenReturn(new TreeMap<>());
      transform = spy(new DetectLanguage(transformMeta, meta, data, 0, pipelineMeta, null));
    }
    transform.first = false;
    data.indexOfCorpusField = 0;
    data.indexOfDetectedLanguage = 1;
    data.indexOfDetectedLanguageConfidence = 2;

    doReturn(row, (Object[]) null).when(transform).getRow();
    doNothing().when(transform).setOutputDone();
    doNothing().when(transform).putRow(any(), any());
  }

  @Test
  void inlineCompletionDoesNotLeavePhantomJob() throws Exception {
    setField("executor", new InlineExecutor());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    verify(transform).putRow(any(), any());
    verify(transform).setOutputDone();
  }

  @Test
  void workerExceptionIsVisibleAtEof() throws Exception {
    setField("executor", new InlineExecutor());
    doAnswer(
            invocation -> {
              throw new HopTransformException("worker failed");
            })
        .when(transform)
        .putRow(any(), any());
    assertTrue(transform.processRow());
    HopException failure = assertThrows(HopException.class, transform::processRow);
    assertTrue(failure.getMessage().contains("Asynchronous processing failed"));
  }

  @Test
  void workerErrorAlsoReleasesReservation() throws Exception {
    setField("executor", new InlineExecutor());
    doAnswer(
            invocation -> {
              throw new AssertionError("worker error");
            })
        .when(transform)
        .putRow(any(), any());
    assertTrue(transform.processRow());
    assertThrows(HopException.class, transform::processRow);
    // Disposal must not hang even when submit captured an Error in its Future.
    doNothing().when(transform).stopAll();
    transform.dispose();
    assertEquals(1, transform.getErrors());
  }

  @Test
  void rejectedSubmissionRollsBackReservation() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.shutdown();
    setField("executor", executor);
    assertThrows(HopException.class, transform::processRow);
    doNothing().when(transform).stopAll();
    transform.dispose();
    assertEquals(1, transform.getErrors());
  }

  @Test
  void synchronousFailureUsesConfiguredErrorHandling() throws Exception {
    setParallelism(false);
    doReturn(true).when(transformMeta).isDoingErrorHandling();
    doAnswer(
            invocation -> {
              throw new HopTransformException("bad row");
            })
        .when(transform)
        .putRow(any(), any());
    doNothing()
        .when(transform)
        .putError(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyLong(),
            anyString(),
            anyString(),
            anyString());
    assertTrue(transform.processRow());
    verify(transform)
        .putError(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyLong(),
            anyString(),
            anyString(),
            anyString());
  }

  @Test
  void asynchronousFailureUsesConfiguredErrorHandling() throws Exception {
    setField("executor", new InlineExecutor());
    doReturn(true).when(transformMeta).isDoingErrorHandling();
    doAnswer(
            invocation -> {
              throw new HopTransformException("bad async row");
            })
        .when(transform)
        .putRow(any(), any());
    doNothing()
        .when(transform)
        .putError(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyLong(),
            anyString(),
            anyString(),
            anyString());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    verify(transform)
        .putError(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyLong(),
            anyString(),
            anyString(),
            anyString());
  }

  @Test
  void failureWakesAdmissionAtCapacity() throws Exception {
    ExecutorService workers = Executors.newSingleThreadExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      setField("executor", workers);
      setField("maxJobs", 1);
      doReturn(row).when(transform).getRow();
      doAnswer(
              invocation -> {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                throw new HopTransformException("worker failed while admission waits");
              })
          .when(transform)
          .putRow(any(), any());
      assertTrue(transform.processRow());
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      CountDownLatch admitting = new CountDownLatch(1);
      Future<?> nextRow =
          caller.submit(
              () -> {
                admitting.countDown();
                return assertThrows(HopException.class, transform::processRow);
              });
      assertTrue(admitting.await(5, TimeUnit.SECONDS));
      assertFalse(nextRow.isDone());
      release.countDown();
      nextRow.get(5, TimeUnit.SECONDS);
      doNothing().when(transform).stopAll();
      transform.dispose();
      assertEquals(1, transform.getErrors());
    } finally {
      release.countDown();
      workers.shutdownNow();
      caller.shutdownNow();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(caller.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void eofDrainsAcceptedWorkBeforeOutputDone() throws Exception {
    assertDrains(false);
  }

  @Test
  void disposalDrainsAcceptedWorkWithoutShuttingDownExecutor() throws Exception {
    assertDrains(true);
  }

  private void assertDrains(boolean dispose) throws Exception {
    ExecutorService workers = Executors.newSingleThreadExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger outputs = new AtomicInteger();
    try {
      setField("executor", workers);
      doAnswer(
              invocation -> {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                outputs.incrementAndGet();
                return null;
              })
          .when(transform)
          .putRow(any(), any());
      assertTrue(transform.processRow());
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      CountDownLatch draining = new CountDownLatch(1);
      Future<?> completion =
          caller.submit(
              () -> {
                draining.countDown();
                if (dispose) {
                  transform.dispose();
                } else {
                  try {
                    assertFalse(transform.processRow());
                  } catch (HopException e) {
                    throw new AssertionError(e);
                  }
                }
              });
      assertTrue(draining.await(5, TimeUnit.SECONDS));
      assertFalse(completion.isDone());
      release.countDown();
      completion.get(5, TimeUnit.SECONDS);
      assertEquals(1, outputs.get());
      assertFalse(workers.isShutdown());
      if (!dispose) {
        verify(transform).setOutputDone();
      }
    } finally {
      release.countDown();
      workers.shutdownNow();
      caller.shutdownNow();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(caller.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void eofWaitsForSlowInFlightJobAndDeliversRowBeforeOutputDone() throws Exception {
    // Regression: the 30 s drain cap used to apply on the normal end-of-input path too, so finish()
    // abandoned jobs still running after 30 s and dropped their rows. finish() must wait without a
    // cap and only then close the output.
    ExecutorService workers = Executors.newSingleThreadExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger outputs = new AtomicInteger();
    try {
      setField("executor", workers);
      doAnswer(
              invocation -> {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                outputs.incrementAndGet();
                return null;
              })
          .when(transform)
          .putRow(any(), any());
      assertTrue(transform.processRow());
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      // Run the EOF step on its own thread so we can observe that, while a job is still in flight,
      // it keeps waiting instead of dropping the job and closing the output on a short drain cap.
      CountDownLatch draining = new CountDownLatch(1);
      Future<?> completion =
          caller.submit(
              () -> {
                draining.countDown();
                try {
                  assertFalse(transform.processRow());
                } catch (HopException e) {
                  throw new AssertionError(e);
                }
              });
      assertTrue(draining.await(5, TimeUnit.SECONDS));
      // The in-flight job is still running; the EOF path must not have returned (closed the output)
      // yet, even though it has been waiting longer than any sub-second drain cap.
      Thread.sleep(100);
      assertFalse(completion.isDone());
      release.countDown();
      completion.get(5, TimeUnit.SECONDS);
      assertEquals(1, outputs.get());
      verify(transform).setOutputDone();
      assertFalse(workers.isShutdown());
    } finally {
      release.countDown();
      workers.shutdownNow();
      caller.shutdownNow();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(caller.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void eofHonoursStopToShortCircuitUncappedWait() throws Exception {
    // Regression: finish() waits without a cap for in-flight jobs, so a stop must still break the
    // wait and close the output, otherwise a never-completing job would hang the transform.
    ExecutorService workers = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      setField("executor", workers);
      doAnswer(
              invocation -> {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return null;
              })
          .when(transform)
          .putRow(any(), any());
      assertTrue(transform.processRow());
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      doReturn(true).when(transform).isStopped();
      assertFalse(transform.processRow());
      verify(transform).setOutputDone();
    } finally {
      release.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private void setParallelism(boolean parallelism) {
    ((DetectLanguageMeta) transformMeta.getTransform()).setParallelism(parallelism);
  }

  private void setField(String name, Object value) throws Exception {
    Field field = DetectLanguage.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(transform, value);
  }

  private static final class InlineExecutor extends AbstractExecutorService {
    @Override
    public void execute(Runnable command) {
      command.run();
    }

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return false;
    }
  }
}
