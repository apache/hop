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
 *
 */

package org.apache.hop.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Starts a real Flight server and checks that the credentials configured on it are actually
 * enforced. These tests use plain gRPC: they cover the authentication half of the security options,
 * which is independent of the transport.
 */
class ArrowFlightServerAuthenticationTest {

  private static final String USERNAME = "hop";
  private static final String PASSWORD = "s3cr3t";

  private final ILogChannel log = mock(ILogChannel.class);

  private ArrowFlightServer server;
  private RootAllocator clientAllocator;
  private FlightClient client;

  @AfterEach
  void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (clientAllocator != null) {
      clientAllocator.close();
    }
    if (server != null) {
      server.shutdown();
      server.getFlightServer().awaitTermination();
    }
  }

  private FlightClient startServerAndConnect(ArrowFlightSecurity security) throws Exception {
    // Port 0 lets the OS pick a free port for us.
    //
    server =
        new ArrowFlightServer(
            "localhost", 0, security, new Variables(), new MemoryMetadataProvider(), log);
    server.start();

    clientAllocator = new RootAllocator();
    client =
        FlightClient.builder(
                clientAllocator,
                Location.forGrpcInsecure("localhost", server.getFlightServer().getPort()))
            .build();
    return client;
  }

  private static ArrowFlightSecurity withCredentials() {
    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setUsername(USERNAME);
    security.setPassword(PASSWORD);
    return security;
  }

  /**
   * Any call made without credentials. {@code listFlights} is not implemented by the Hop producer,
   * so reaching the producer at all shows up as UNIMPLEMENTED rather than UNAUTHENTICATED.
   */
  private static FlightStatusCode callStatusCode(FlightClient client, CallOption... callOptions) {
    FlightRuntimeException exception =
        assertThrows(
            FlightRuntimeException.class,
            () -> client.listFlights(Criteria.ALL, callOptions).forEach(info -> {}));
    return exception.status().code();
  }

  @Test
  void aServerWithoutCredentialsAcceptsAnyClient() throws Exception {
    FlightClient flightClient = startServerAndConnect(ArrowFlightSecurity.NONE);

    // The call reaches the producer, which doesn't implement listFlights.
    //
    assertEquals(FlightStatusCode.UNIMPLEMENTED, callStatusCode(flightClient));
  }

  @Test
  void aServerWithCredentialsRejectsAnonymousClients() throws Exception {
    FlightClient flightClient = startServerAndConnect(withCredentials());

    assertEquals(FlightStatusCode.UNAUTHENTICATED, callStatusCode(flightClient));
  }

  @Test
  void aServerWithCredentialsRejectsTheWrongPassword() throws Exception {
    FlightClient flightClient = startServerAndConnect(withCredentials());

    FlightRuntimeException exception =
        assertThrows(
            FlightRuntimeException.class,
            () -> flightClient.authenticateBasicToken(USERNAME, "wrong"));
    assertEquals(FlightStatusCode.UNAUTHENTICATED, exception.status().code());
  }

  @Test
  void aServerWithCredentialsRejectsTheWrongUsername() throws Exception {
    FlightClient flightClient = startServerAndConnect(withCredentials());

    FlightRuntimeException exception =
        assertThrows(
            FlightRuntimeException.class,
            () -> flightClient.authenticateBasicToken("somebody-else", PASSWORD));
    assertEquals(FlightStatusCode.UNAUTHENTICATED, exception.status().code());
  }

  @Test
  void aServerWithCredentialsAcceptsTheRightOnes() throws Exception {
    FlightClient flightClient = startServerAndConnect(withCredentials());

    Optional<CredentialCallOption> bearerToken =
        flightClient.authenticateBasicToken(USERNAME, PASSWORD);
    assertTrue(bearerToken.isPresent(), "Expected a bearer token back from the Flight server");

    CredentialCallOption callOption = bearerToken.get();
    assertNotNull(callOption);

    // With the token the call gets through to the producer.
    //
    assertEquals(FlightStatusCode.UNIMPLEMENTED, callStatusCode(flightClient, callOption));
  }
}
