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
 *
 */

package org.apache.hop.vfs.gs;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.StorageRetryStrategy;
import java.time.Duration;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.junit.jupiter.api.Test;

/**
 * The values in the Google Cloud options dialog must actually reach the client's {@link
 * RetrySettings}. Companion to {@link GoogleStorageTransportOptionsTest}, which covers the socket
 * timeouts on the same dialog.
 */
class GoogleStorageRetrySettingsTest {

  @Test
  void defaultConfigMatchesTheShippedDefaults() {
    RetrySettings settings = GoogleStorageFileSystem.buildRetrySettings(new GoogleCloudConfig());

    assertEquals(6, settings.getMaxAttempts());
    assertEquals(Duration.ofSeconds(1), settings.getInitialRetryDelayDuration());
    assertEquals(2.0, settings.getRetryDelayMultiplier());
    assertEquals(Duration.ofSeconds(32), settings.getMaxRetryDelayDuration());
    assertEquals(Duration.ofMinutes(50), settings.getTotalTimeoutDuration());
    assertEquals(Duration.ofSeconds(50), settings.getInitialRpcTimeoutDuration());
    assertEquals(1.0, settings.getRpcTimeoutMultiplier());
    assertEquals(Duration.ofSeconds(50), settings.getMaxRpcTimeoutDuration());
  }

  @Test
  void configuredValuesAreApplied() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setMaxAttempts("100");
    config.setInitialRetryDelay("3");
    config.setRetryDelayMultiplier("1.5");
    config.setMaxRetryDelay("64");
    config.setTotalTimeout("10");
    config.setInitialRpcTimeout("20");
    config.setRpcTimeoutMultiplier("2.0");
    config.setMaxRpcTimeout("120");

    RetrySettings settings = GoogleStorageFileSystem.buildRetrySettings(config);

    assertEquals(100, settings.getMaxAttempts());
    assertEquals(Duration.ofSeconds(3), settings.getInitialRetryDelayDuration());
    assertEquals(1.5, settings.getRetryDelayMultiplier());
    assertEquals(Duration.ofSeconds(64), settings.getMaxRetryDelayDuration());
    assertEquals(Duration.ofSeconds(20), settings.getInitialRpcTimeoutDuration());
    assertEquals(2.0, settings.getRpcTimeoutMultiplier());
    assertEquals(Duration.ofSeconds(120), settings.getMaxRpcTimeoutDuration());
  }

  /** Total timeout is in minutes while everything around it is in seconds. */
  @Test
  void totalTimeoutIsInterpretedAsMinutes() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setTotalTimeout("7");

    assertEquals(
        Duration.ofMinutes(7),
        GoogleStorageFileSystem.buildRetrySettings(config).getTotalTimeoutDuration());
  }

  /**
   * The max RPC timeout used to be left at the library default of 50s while the initial one was
   * applied, so raising the initial timeout past 50 threw an IllegalStateException out of the
   * client builder and took down all GCS access.
   */
  @Test
  void initialRpcTimeoutAboveTheConfiguredMaximumDoesNotBreakTheClient() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setInitialRpcTimeout("1000");
    config.setMaxRpcTimeout("50");

    RetrySettings settings =
        assertDoesNotThrow(() -> GoogleStorageFileSystem.buildRetrySettings(config));

    assertEquals(Duration.ofSeconds(1000), settings.getInitialRpcTimeoutDuration());
    assertEquals(
        Duration.ofSeconds(1000),
        settings.getMaxRpcTimeoutDuration(),
        "the ceiling should be raised to the initial timeout rather than rejected");
  }

  @Test
  void maxRpcTimeoutReachesTheSettings() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setMaxRpcTimeout("300");

    assertEquals(
        Duration.ofSeconds(300),
        GoogleStorageFileSystem.buildRetrySettings(config).getMaxRpcTimeoutDuration(),
        "the max RPC timeout field was stored but never applied");
  }

  /** A cleared field in the dialog used to throw a NumberFormatException on every GCS call. */
  @Test
  void blankAndInvalidValuesFallBackToTheDefaultInsteadOfThrowing() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setMaxAttempts("");
    config.setInitialRetryDelay(null);
    config.setRetryDelayMultiplier("not-a-number");
    config.setMaxRetryDelay("");
    config.setTotalTimeout("nope");
    config.setInitialRpcTimeout("");
    config.setRpcTimeoutMultiplier("");
    config.setMaxRpcTimeout(null);

    RetrySettings settings =
        assertDoesNotThrow(() -> GoogleStorageFileSystem.buildRetrySettings(config));

    assertEquals(6, settings.getMaxAttempts());
    assertEquals(Duration.ofSeconds(1), settings.getInitialRetryDelayDuration());
    assertEquals(2.0, settings.getRetryDelayMultiplier());
    assertEquals(Duration.ofMinutes(50), settings.getTotalTimeoutDuration());
  }

  @Test
  void retryStrategyFollowsTheNonIdempotentOption() {
    GoogleCloudConfig config = new GoogleCloudConfig();

    assertSame(
        StorageRetryStrategy.getDefaultStorageRetryStrategy(),
        GoogleStorageFileSystem.selectRetryStrategy(config),
        "writes should not be retried unless the option is switched on");

    config.setRetryNonIdempotentOperations(true);
    // The uniform strategy is handed out as a new instance per call, so compare by type.
    assertEquals(
        StorageRetryStrategy.getUniformStorageRetryStrategy().getClass(),
        GoogleStorageFileSystem.selectRetryStrategy(config).getClass());
  }
}
