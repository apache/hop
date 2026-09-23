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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Guards the shape of the {@code log4j2.xml} shipped in hop-core: Hop's own log lines are already
 * printed by {@link ConsoleLoggingEventListener}, so the {@code org.apache.hop} logger must not
 * reach the root console appender or every line shows up twice. Third-party warnings do go to the
 * console through the root logger.
 */
class DefaultLog4j2ConfigurationTest {

  private static final String HOP_LOGGER = "org.apache.hop";

  private LoggerContext context;
  private CapturingAppender rootAppender;

  @BeforeEach
  void loadShippedConfiguration() throws Exception {
    InputStream stream = getClass().getClassLoader().getResourceAsStream("log4j2.xml");
    assertNotNull(stream, "hop-core must ship a default log4j2.xml");

    context = new LoggerContext(getClass().getSimpleName());
    Configuration configuration = new XmlConfiguration(context, new ConfigurationSource(stream));
    context.start(configuration);

    rootAppender = new CapturingAppender();
    rootAppender.start();
    context.getConfiguration().getRootLogger().addAppender(rootAppender, null, null);
    context.updateLoggers();
  }

  @AfterEach
  void stopContext() {
    context.stop();
  }

  @Test
  void hopLoggerIsNotAdditiveAndHasNoAppender() {
    LoggerConfig hop = context.getConfiguration().getLoggerConfig(HOP_LOGGER);

    assertEquals(HOP_LOGGER, hop.getName(), "an explicit org.apache.hop logger must be declared");
    assertFalse(hop.isAdditive(), "org.apache.hop must not bubble up to the root console appender");
    assertTrue(hop.getAppenders().isEmpty(), "org.apache.hop must not print by default");
    assertEquals(Level.TRACE, hop.getLevel(), "all forwarded Hop levels must be available");
  }

  @Test
  void hopLinesAreNotPrintedTwiceButThirdPartyWarningsReachTheConsole() {
    context.getLogger("org.apache.hop.pipeline.Pipeline").info("a pipeline line");
    context.getLogger("org.apache.hop.workflow.Workflow").error("a workflow error");
    context.getLogger("org.eclipse.jetty.server.Server").warn("a third-party warning");
    context.getLogger("org.eclipse.jetty.server.Server").info("a third-party info line");

    assertEquals(1, rootAppender.events.size(), "only the third-party warning reaches the console");
    assertEquals(
        "a third-party warning", rootAppender.events.get(0).getMessage().getFormattedMessage());
  }

  private static final class CapturingAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super("capture", null, null, true, null);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }
}
