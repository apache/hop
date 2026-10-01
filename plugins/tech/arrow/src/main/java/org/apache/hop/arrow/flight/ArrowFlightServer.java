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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@Getter
@Setter
public class ArrowFlightServer {
  private String hostname;
  private int port;
  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final ArrowFlightSecurity security;

  private final RootAllocator allocator;
  private final Location location;
  private final ILogChannel log;

  private final FlightServer flightServer;

  public ArrowFlightServer(
      String hostname,
      int port,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log)
      throws HopException {
    this(hostname, port, ArrowFlightSecurity.NONE, variables, metadataProvider, log);
  }

  public ArrowFlightServer(
      String hostname,
      int port,
      ArrowFlightSecurity security,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log)
      throws HopException {
    this.hostname = hostname;
    this.port = port;
    this.security = security == null ? ArrowFlightSecurity.NONE : security;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;

    this.security.validate(log);

    this.allocator = new RootAllocator();

    this.location =
        this.security.isTlsEnabled()
            ? Location.forGrpcTls(hostname, port)
            : Location.forGrpcInsecure(hostname, port);

    FlightProducer producer = new HopFlightProducer(variables, metadataProvider, allocator);
    flightServer = buildFlightServer(producer);
  }

  private FlightServer buildFlightServer(FlightProducer producer) throws HopException {
    FlightServer.Builder builder = FlightServer.builder(allocator, location, producer);

    if (security.isTlsEnabled()) {
      byte[] certificate = ArrowFlightSecurity.readPemFile(security.getCertificateFile());
      byte[] privateKey = ArrowFlightSecurity.readPemFile(security.getPrivateKeyFile());
      try (InputStream certificateStream = new ByteArrayInputStream(certificate);
          InputStream privateKeyStream = new ByteArrayInputStream(privateKey)) {
        builder.useTls(certificateStream, privateKeyStream);
      } catch (IOException e) {
        throw new HopException(
            "Unable to configure TLS for the Flight server on " + hostname + ":" + port, e);
      }

      if (security.isMutualTlsEnabled()) {
        byte[] clientCa =
            ArrowFlightSecurity.readPemFile(security.getClientCertificateAuthorityFile());
        try (InputStream clientCaStream = new ByteArrayInputStream(clientCa)) {
          builder.useMTlsClientVerification(clientCaStream);
        } catch (IOException e) {
          throw new HopException(
              "Unable to configure client certificate verification for the Flight server on "
                  + hostname
                  + ":"
                  + port,
              e);
        }
      }
    }

    if (security.isAuthenticationEnabled()) {
      builder.headerAuthenticator(security.createAuthenticator());
    }

    return builder.build();
  }

  public void start() throws HopException {
    try {
      flightServer.start();
      log.logBasic(
          "Apache Arrow Flight server listening on "
              + hostname
              + ":"
              + port
              + " (TLS: "
              + (security.isTlsEnabled() ? "enabled" : "disabled")
              + ", authentication: "
              + (security.isAuthenticationEnabled() ? "enabled" : "disabled")
              + ")");
    } catch (Exception e) {
      throw new HopException("Unable to start Flight server on " + hostname + ":" + port, e);
    }
  }

  public void shutdown() throws HopException {
    try {
      flightServer.shutdown();
      log.logBasic("Apache Arrow Flight server was shut down");
    } catch (Exception e) {
      throw new HopException("Unable to shut down Flight server on " + hostname + ":" + port, e);
    }
  }
}
