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

package org.apache.hop.core.logging;

import static org.apache.hop.core.logging.LogLevel.BASIC;
import static org.apache.hop.core.logging.LogLevel.ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
class Slf4jLoggingEventListenerTest {

  @Mock private Logger pipelineLogger, workflowLogger, hopLogger;
  @Mock private HopLoggingEvent logEvent;
  @Mock private ILoggingObject loggingObject;
  @Mock private LogMessage message;
  @Mock private Function<String, ILoggingObject> logObjProvider;

  private String logChannelId = "logChannelId";
  private String msgText = "message";
  private String messageSub = "subject";
  private LogLevel logLevel = BASIC;

  private Slf4jLoggingEventListener listener = new Slf4jLoggingEventListener();

  @BeforeEach
  void before() {
    listener.pipelineLogger = pipelineLogger;
    listener.workflowLogger = workflowLogger;
    listener.hopLogger = hopLogger;
    listener.logObjProvider = logObjProvider;
    lenient().when(logEvent.getMessage()).thenReturn(message);
    lenient().when(message.getLogChannelId()).thenReturn(logChannelId);
    lenient().when(message.getLevel()).thenReturn(logLevel);
    lenient().when(message.getMessage()).thenReturn(msgText);
    lenient().when(message.getSubject()).thenReturn(messageSub);
  }

  @Test
  void testAddLogEventNoRegisteredLogObject() {
    listener.eventAdded(logEvent);
    verify(hopLogger).info(String.format("%s %s", messageSub, msgText));

    when(message.getLevel()).thenReturn(ERROR);
    listener.eventAdded(logEvent);
    verify(hopLogger).error(String.format("%s %s", messageSub, msgText));
    verifyNoInteractions(pipelineLogger);
    verifyNoInteractions(workflowLogger);
  }

  @Test
  void testAddLogEventPipeline() {
    when(logObjProvider.apply(logChannelId)).thenReturn(loggingObject);
    when(loggingObject.getObjectType()).thenReturn(LoggingObjectType.PIPELINE);
    when(loggingObject.getFilename()).thenReturn("filename");
    when(message.getLevel()).thenReturn(LogLevel.BASIC);
    listener.eventAdded(logEvent);

    verify(pipelineLogger).info(String.format("[filename]  %s", msgText));
    when(message.getLevel()).thenReturn(LogLevel.ERROR);
    listener.eventAdded(logEvent);
    verify(pipelineLogger).error(String.format("[filename]  %s", msgText));
    verifyNoInteractions(hopLogger);
    verifyNoInteractions(workflowLogger);
  }

  @Test
  void testAddLogEventJob() {
    when(logObjProvider.apply(logChannelId)).thenReturn(loggingObject);
    when(loggingObject.getObjectType()).thenReturn(LoggingObjectType.WORKFLOW);
    when(loggingObject.getFilename()).thenReturn("filename");
    when(message.getLevel()).thenReturn(LogLevel.BASIC);
    listener.eventAdded(logEvent);

    verify(workflowLogger).info(String.format("[filename]  %s", msgText));

    when(message.getLevel()).thenReturn(LogLevel.ERROR);
    listener.eventAdded(logEvent);
    verify(workflowLogger).error(String.format("[filename]  %s", msgText));
    verifyNoInteractions(hopLogger);
    verifyNoInteractions(pipelineLogger);
  }

  @Test
  void testAddLogEventPublishesHopContextToMdc() {
    when(logObjProvider.apply(logChannelId)).thenReturn(loggingObject);
    when(loggingObject.getObjectType()).thenReturn(LoggingObjectType.PIPELINE);
    when(loggingObject.getFilename()).thenReturn("filename");
    when(message.getLevel()).thenReturn(LogLevel.DEBUG);

    assertNull(MDC.get(Slf4jLoggingEventListener.MDC_CHANNEL_ID), "MDC must start clean");
    assertNull(MDC.get(Slf4jLoggingEventListener.MDC_LOG_LEVEL), "MDC must start clean");
    assertNull(MDC.get(Slf4jLoggingEventListener.MDC_SUBJECT), "MDC must start clean");

    StringBuilder channelId = new StringBuilder();
    StringBuilder logLevel = new StringBuilder();
    StringBuilder subject = new StringBuilder();
    doAnswer(
            invocation -> {
              // Capture the Hop context at the exact moment the record reaches SLF4J.
              channelId.append(MDC.get(Slf4jLoggingEventListener.MDC_CHANNEL_ID));
              logLevel.append(MDC.get(Slf4jLoggingEventListener.MDC_LOG_LEVEL));
              subject.append(MDC.get(Slf4jLoggingEventListener.MDC_SUBJECT));
              return null;
            })
        .when(pipelineLogger)
        .debug(anyString());

    listener.eventAdded(logEvent);

    assertEquals(logChannelId, channelId.toString());
    assertEquals(LogLevel.DEBUG.getCode(), logLevel.toString());
    assertEquals("filename", subject.toString());

    // MDC is scoped to the dispatch and must not leak to the caller.
    assertNull(MDC.get(Slf4jLoggingEventListener.MDC_CHANNEL_ID), "MDC must be cleared after");
    assertNull(MDC.get(Slf4jLoggingEventListener.MDC_LOG_LEVEL), "MDC must be cleared after");
    assertNull(MDC.get(Slf4jLoggingEventListener.MDC_SUBJECT), "MDC must be cleared after");
  }
}
