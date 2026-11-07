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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.HttpTransportResponseException;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.lineage.openlineage.OpenLineageEmitter.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Covers which delivery failures are worth retrying. Retrying a request the collector understood
 * and rejected cannot succeed — it only spends the backoff — so only transport-level and 5xx
 * failures (plus the two 4xx codes that invite a retry) get another attempt.
 */
class OpenLineageHttpClientTest {

  private static final OpenLineage OL = new OpenLineage(URI.create("https://example.com/producer"));
  private static final URI COLLECTOR = URI.create("http://collector:5000/api/v1/lineage");

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  private static RunEvent event() {
    return OL.newRunEventBuilder()
        .eventType(RunEvent.EventType.OTHER)
        .eventTime(Instant.ofEpochMilli(1L).atZone(ZoneOffset.UTC))
        .run(OL.newRun(UUID.randomUUID(), null))
        .job(OL.newJob("ns", "job", null))
        .build();
  }

  private static OpenLineageHttpClient clientThatFailsWith(
      OpenLineageClient delegate, Throwable failure) {
    doThrow(failure).when(delegate).emit(any(RunEvent.class));
    return new OpenLineageHttpClient(delegate, COLLECTOR, 3, 1, new LogChannel("olhttp"));
  }

  @Test
  void badRequestIsRejectedWithoutRetrying() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    OpenLineageHttpClient client =
        clientThatFailsWith(delegate, new HttpTransportResponseException(400, "malformed event"));

    assertEquals(Result.REJECTED, client.emit(event()));
    assertEquals(1, client.getFailureCount());
    verify(delegate, times(1)).emit(any(RunEvent.class));
  }

  // A wrong token is a configuration problem someone will fix: the event is kept for later, not
  // thrown away with everything sent until then, and not retried straight away either.
  @Test
  void unauthorizedIsKeptForLaterWithoutRetryingNow() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    OpenLineageHttpClient client =
        clientThatFailsWith(delegate, new HttpTransportResponseException(401, "no token"));

    assertEquals(Result.RETRY, client.emit(event()));
    verify(delegate, times(1)).emit(any(RunEvent.class));
  }

  @Test
  void serverErrorIsRetriedUpToTheConfiguredLimit() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    OpenLineageHttpClient client =
        clientThatFailsWith(delegate, new HttpTransportResponseException(503, "unavailable"));

    assertEquals(Result.RETRY, client.emit(event()));
    // maxRetries = 3 -> 4 attempts in total.
    verify(delegate, times(4)).emit(any(RunEvent.class));
  }

  @Test
  void tooManyRequestsIsRetried() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    OpenLineageHttpClient client =
        clientThatFailsWith(delegate, new HttpTransportResponseException(429, "slow down"));

    assertEquals(Result.RETRY, client.emit(event()));
    verify(delegate, times(4)).emit(any(RunEvent.class));
  }

  /** A failure with no HTTP response at all (connect timeout, DNS, reset) is transient. */
  @Test
  void transportFailureWithoutAResponseIsRetried() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    OpenLineageHttpClient client =
        clientThatFailsWith(delegate, new OpenLineageClientException("connection refused"));

    assertEquals(Result.RETRY, client.emit(event()));
    verify(delegate, times(4)).emit(any(RunEvent.class));
  }

  /** A rejection wrapped by the client still has its status recognised. */
  @Test
  void wrappedResponseExceptionIsInspectedThroughTheCause() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    OpenLineageHttpClient client =
        clientThatFailsWith(
            delegate,
            new OpenLineageClientException(
                "emit failed", new HttpTransportResponseException(422, "unprocessable")));

    assertEquals(Result.REJECTED, client.emit(event()));
    verify(delegate, times(1)).emit(any(RunEvent.class));
  }

  @Test
  void successIsCountedAndNotRetried() {
    OpenLineageClient delegate = mock(OpenLineageClient.class);
    doNothing().when(delegate).emit(any(RunEvent.class));
    OpenLineageHttpClient client =
        new OpenLineageHttpClient(delegate, COLLECTOR, 3, 1, new LogChannel("olhttp"));

    assertEquals(Result.SENT, client.emit(event()));
    assertEquals(1, client.getSuccessCount());
    assertEquals(0, client.getFailureCount());
    verify(delegate, times(1)).emit(any(RunEvent.class));
  }
}
