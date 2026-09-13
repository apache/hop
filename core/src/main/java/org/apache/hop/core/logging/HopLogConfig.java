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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopResolvedSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Runtime wiring of the log4j2 backend used by Hop. It guarantees the in-memory {@link
 * LoggingBuffer} is always fed by attaching {@link HopLogBufferAppender} to the root logger, and it
 * honors {@link Const#HOP_DISABLE_CONSOLE_LOGGING} by disabling the console output of the {@link
 * HopLogBufferAppender}.
 */
public final class HopLogConfig {

  private static final AtomicBoolean BUFFER_ATTACHED = new AtomicBoolean(false);

  private HopLogConfig() {
    // utility
  }

  /**
   * Ensure the {@link HopLogBufferAppender} is attached to the root logger so Hop's in-memory
   * buffer is fed by the SLF4J/log4j2 pipeline. Safe to call multiple times.
   */
  public static void ensureBufferAppender() {
    if (!BUFFER_ATTACHED.compareAndSet(false, true)) {
      return;
    }
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig root = config.getRootLogger();
    if (!root.getAppenders().containsKey(HopLogBufferAppender.NAME)) {
      root.addAppender(HopLogBufferAppender.getInstance(), null, null);
      context.updateLoggers();
    }
  }

  /**
   * Wire the log4j2 backend: attach the buffer appender and apply the console on/off setting. This
   * is independent of the {@code log4j2.properties} shipped in core so the buffer stays fed even
   * when a user supplies their own log4j2 configuration.
   */
  public static void init() {
    ensureBufferAppender();

    String disableConsole =
        HopResolvedSettings.resolveString(Const.HOP_DISABLE_CONSOLE_LOGGING, "N");
    HopLogBufferAppender.getInstance().setConsoleEnabled(!"Y".equalsIgnoreCase(disableConsole));
  }
}
