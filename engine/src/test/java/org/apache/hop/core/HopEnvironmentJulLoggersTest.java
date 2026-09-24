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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The JUL LogManager only keeps weak references to loggers. A level set on an unreferenced logger
 * is lost at the next garbage collection, after which a driver logs at the root INFO level again
 * (#7297). These tests force a GC before looking at the levels.
 */
class HopEnvironmentJulLoggersTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "com.microsoft.sqlserver.jdbc.TDSTokenHandler",
        "org.apache.hc.client5.http.wire.Something"
      })
  void verboseLoggersStaySilencedAfterGarbageCollection(String childLoggerName) {
    HopEnvironment.silenceVerboseThirdPartyLoggers();

    for (int i = 0; i < 3; i++) {
      System.gc();
    }

    Logger childLogger = Logger.getLogger(childLoggerName);
    assertFalse(childLogger.isLoggable(Level.INFO));
    assertTrue(childLogger.isLoggable(Level.WARNING));
  }

  @ParameterizedTest
  @ValueSource(strings = {"com.microsoft.sqlserver.jdbc", "org.apache.hc.client5.http.wire"})
  void explicitlyConfiguredLevelIsPreserved(String loggerName) {
    Logger logger = Logger.getLogger(loggerName);
    Level previousLevel = logger.getLevel();
    try {
      logger.setLevel(Level.FINE);

      HopEnvironment.silenceVerboseThirdPartyLoggers();

      assertEquals(Level.FINE, logger.getLevel());
    } finally {
      logger.setLevel(previousLevel);
    }
  }
}
