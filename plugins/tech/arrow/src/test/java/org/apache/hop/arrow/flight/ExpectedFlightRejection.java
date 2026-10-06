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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

/**
 * Runs a call that is expected to be rejected by the Flight server. Arrow's handshake logs that
 * rejection at ERROR, with a stack trace, before the exception reaches the test. The log line below
 * is the signal that this was intentional.
 */
public final class ExpectedFlightRejection {

  private static final String HANDSHAKE_LOGGER =
      "org.apache.arrow.flight.auth2.ClientHandshakeWrapper";

  private ExpectedFlightRejection() {}

  public static void run(String reason, RejectionCall call) throws Exception {
    System.out.println(
        "[test] Expected UNAUTHENTICATED from the Arrow Flight server: "
            + reason
            + ". Arrow would log this rejection at ERROR; that stack trace is hidden.");
    Logger handshakeLogger = (Logger) LogManager.getLogger(HANDSHAKE_LOGGER);
    Level previousLevel = handshakeLogger.getLevel();
    handshakeLogger.setLevel(Level.OFF);
    try {
      call.run();
    } finally {
      handshakeLogger.setLevel(previousLevel);
    }
  }

  @FunctionalInterface
  public interface RejectionCall {
    void run() throws Exception;
  }
}
