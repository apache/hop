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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelFacade;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

@Timeout(15)
class LanguageModelChatLifecycleTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<LanguageModelChatMeta, LanguageModelChatData> helper;

  @BeforeEach
  void setUp() throws Exception {
    helper =
        new TransformMockHelper<>("chat", LanguageModelChatMeta.class, LanguageModelChatData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    HopEnvironment.init();
  }

  @AfterEach
  void cleanUp() {
    helper.cleanUp();
  }

  private LanguageModelChat transform(final int parallelism) {
    final var meta = new LanguageModelChatMeta();
    meta.setDefault();
    meta.setMock(true);
    meta.setMockOutputValue("answer");
    meta.setParallelism(parallelism);
    final var transform =
        new LanguageModelChat(
            helper.transformMeta,
            meta,
            new LanguageModelChatData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    final var rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("input"));
    transform.setInputRowMeta(rowMeta);
    final var input = helper.getMockInputRowSet(new Object[] {"question"});
    when(input.getRowMeta()).thenReturn(rowMeta);
    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    return transform;
  }

  @Test
  void ownedPoolDrainsMockOutputBeforeCompletion() throws Exception {
    final var transform = transform(2);
    final var pool = new AtomicReference<ForkJoinPool>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(final IRowMeta rowMeta, final Object[] row) {
            pool.set(((ForkJoinWorkerThread) Thread.currentThread()).getPool());
          }
        });
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    final var output = transform.getOutputRowSets().getFirst();
    assertEquals(1, output.size());
    final var row = output.getRow();
    assertEquals("MOCK", row[transform.getData().indexOfFinishReason]);
    assertEquals("answer", row[transform.getData().indexOfOutput]);
    assertTrue(output.isDone());
    transform.dispose();
    transform.dispose();
    assertTrue(pool.get().isShutdown());
    assertTrue(pool.get().awaitTermination(5, TimeUnit.SECONDS));
    assertEquals(0, transform.getErrors());
  }

  @Test
  void providerExceptionStillProducesAnErrorResultRow() throws Exception {
    try (final var facades =
        mockConstruction(
            LanguageModelFacade.class,
            (facade, context) -> {
              when(facade.inputToChatMessages(any())).thenReturn(List.of());
              when(facade.chat(any())).thenThrow(new IllegalStateException("provider unavailable"));
            })) {
      final var transform = transform(1);
      transform.getMeta().setMock(false);
      assertTrue(transform.processRow());
      assertFalse(transform.processRow());
      final var row = transform.getOutputRowSets().getFirst().getRow();
      assertEquals("ERROR", row[transform.getData().indexOfFinishReason]);
      assertEquals("provider unavailable", row[transform.getData().indexOfOutput]);
      assertEquals(0, transform.getErrors());
      transform.dispose();
    }
  }

  @Test
  void rejectionDuringStopIsNotReportedAsARowFailure() throws Exception {
    final var executor = mock(ExecutorService.class);
    try (final var factory = mockStatic(Executors.class)) {
      factory.when(() -> Executors.newWorkStealingPool(1)).thenReturn(executor);
      final var transform = transform(1);
      when(executor.submit(any(Runnable.class)))
          .thenAnswer(
              invocation -> {
                transform.stopRunning();
                throw new RejectedExecutionException("stopping");
              });
      assertTrue(transform.processRow());
      transform.dispose();
      assertEquals(0, transform.getErrors());
    }
  }

  @Test
  void commonPoolRemainsUsableAfterCompletionAndDisposal() throws Exception {
    final var transform = transform(0);
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    transform.dispose();
    transform.stopRunning();
    assertFalse(ForkJoinPool.commonPool().isShutdown());
    assertEquals(42, ForkJoinPool.commonPool().submit(() -> 42).get(5, TimeUnit.SECONDS));
  }

  @Test
  void workerFailureCanStopThePipelineWithoutWaitingForItself() throws Exception {
    final var transform = transform(1);
    final var stopping = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              transform.stopRunning();
              stopping.countDown();
              return null;
            })
        .when(helper.pipeline)
        .stopAll();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(final IRowMeta rowMeta, final Object[] row)
              throws HopTransformException {
            throw new HopTransformException("output failed");
          }
        });
    assertTrue(transform.processRow());
    assertTrue(stopping.await(5, TimeUnit.SECONDS));
    transform.dispose();
    assertEquals(1, transform.getErrors());
  }

  @Test
  void disposeWaitsForAcceptedOutputWithoutHoldingItsMonitor() throws Exception {
    final var transform = transform(1);
    final var outputStarted = new CountDownLatch(1);
    final var releaseOutput = new CountDownLatch(1);
    final var workerFinished = new AtomicBoolean();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(final IRowMeta rowMeta, final Object[] row)
              throws HopTransformException {
            outputStarted.countDown();
            try {
              if (!releaseOutput.await(5, TimeUnit.SECONDS)) {
                throw new HopTransformException("output wait timed out");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new HopTransformException(e);
            }
            workerFinished.set(true);
          }
        });
    final var disposer = Executors.newSingleThreadExecutor();
    try {
      assertTrue(transform.processRow());
      assertTrue(outputStarted.await(5, TimeUnit.SECONDS));
      transform.stopRunning();
      final var disposed =
          disposer.submit(
              () -> {
                transform.dispose();
                assertTrue(
                    workerFinished.get(), "dispose returned before accepted output completed");
              });
      assertFalse(disposed.isDone());
      releaseOutput.countDown();
      disposed.get(5, TimeUnit.SECONDS);
      assertEquals(0, transform.getErrors());
    } finally {
      releaseOutput.countDown();
      disposer.shutdownNow();
    }
  }

  @Test
  void finishWaitsForSlowChatOutputBeforeClosingOutput() throws Exception {
    // Regression: the 30 s drain cap used to apply on the normal end-of-input path too, so finish()
    // abandoned a slow in-flight chat, cleared the facades and closed the output, dropping the row.
    // finish() must wait without a cap so the row is still delivered.
    final var chatStarted = new CountDownLatch(1);
    final var releaseChat = new CountDownLatch(1);
    try (final var facades =
        mockConstruction(
            LanguageModelFacade.class,
            (facade, context) -> {
              final var aiMessage = mock(AiMessage.class);
              when(aiMessage.text()).thenReturn("answer");
              final var aiResponse = mock(ChatResponse.class);
              when(aiResponse.aiMessage()).thenReturn(aiMessage);
              when(facade.inputToChatMessages(any())).thenReturn(List.of());
              when(facade.chat(any()))
                  .thenAnswer(
                      invocation -> {
                        chatStarted.countDown();
                        assertTrue(releaseChat.await(5, TimeUnit.SECONDS));
                        return aiResponse;
                      });
            })) {
      final var transform = transform(1);
      transform.getMeta().setMock(false);
      assertTrue(transform.processRow());
      assertTrue(chatStarted.await(5, TimeUnit.SECONDS));
      final var eof = Executors.newSingleThreadExecutor();
      try {
        final var completion =
            eof.submit(
                () -> {
                  try {
                    assertFalse(transform.processRow());
                  } catch (HopException e) {
                    throw new AssertionError(e);
                  }
                });
        // The end-of-input path must keep waiting for the in-flight chat rather than abandoning it
        // after a short drain cap and closing the output.
        Thread.sleep(100);
        assertFalse(completion.isDone());
        releaseChat.countDown();
        completion.get(5, TimeUnit.SECONDS);
        final var row = transform.getOutputRowSets().getFirst().getRow();
        assertEquals("answer", row[transform.getData().indexOfOutput]);
        assertEquals(0, transform.getErrors());
        transform.dispose();
      } finally {
        releaseChat.countDown();
        eof.shutdownNow();
      }
    }
  }
}
