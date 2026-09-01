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

package org.apache.hop.git.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileBeforeCommitExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The behaviour these tests pin down is that the git plugin keeps working exactly as it did when no
 * plugin listens on the extension point, and that a listener which misbehaves costs a log entry
 * rather than the ability to commit.
 */
class PreCommitCheckTest {

  private static final String EXTENSION_POINT_ID = HopExtensionPoint.HopGuiFileBeforeCommit.id;
  private static final String PLUGIN_ID = "PreCommitCheckTestListener";
  private static final String GIT_DIR = "/projects/sales";
  private static final List<String> FILES = List.of("pipelines/load.hpl");

  private ILogChannel log;
  private IVariables variables;

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopClientEnvironment.init();
  }

  @AfterEach
  void removeTestListener() {
    PluginRegistry registry = PluginRegistry.getInstance();
    if (registry.getPlugin(ExtensionPointPluginType.class, PLUGIN_ID) != null) {
      registry.removePlugin(
          ExtensionPointPluginType.class,
          registry.getPlugin(ExtensionPointPluginType.class, PLUGIN_ID));
    }
  }

  private void registerListener(Class<? extends IExtensionPoint> listener) throws Exception {
    ExtensionPointPluginType.getInstance()
        .registerCustom(
            listener, "custom", PLUGIN_ID, EXTENSION_POINT_ID, "pre-commit test listener", null);
  }

  private HopGuiFileBeforeCommitExtension check() {
    log = mock(ILogChannel.class);
    variables = mock(IVariables.class);
    return PreCommitCheck.check(log, variables, GIT_DIR, FILES);
  }

  @Test
  void commitProceedsWhenNothingIsListening() {
    HopGuiFileBeforeCommitExtension result = check();

    assertFalse(result.isCancelled());
    assertNull(result.getCancelReason());
  }

  @Test
  void aListenerCanRefuseTheCommit() throws Exception {
    registerListener(RefusingListener.class);

    HopGuiFileBeforeCommitExtension result = check();

    assertTrue(result.isCancelled());
    assertEquals("2 hardcoded passwords", result.getCancelReason());
  }

  @Test
  void aListenerThatThrowsDoesNotBlockTheCommit() throws Exception {
    registerListener(ThrowingListener.class);

    HopGuiFileBeforeCommitExtension result = check();

    assertFalse(result.isCancelled());
    // Assert it was logged too: without this the test would also pass if the listener had never
    // been called at all, which is the failure this test exists to catch.
    verify(log).logError(anyString(), any(Exception.class));
  }

  @Test
  void listenersSeeTheFilesBeingCommitted() throws Exception {
    RecordingListener.seenFiles = null;
    RecordingListener.seenGitDirectory = null;
    registerListener(RecordingListener.class);

    check();

    assertEquals(FILES, RecordingListener.seenFiles);
    assertEquals(GIT_DIR, RecordingListener.seenGitDirectory);
  }

  public static class RefusingListener implements IExtensionPoint<HopGuiFileBeforeCommitExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiFileBeforeCommitExtension extension) {
      extension.cancel("2 hardcoded passwords");
    }
  }

  public static class ThrowingListener implements IExtensionPoint<HopGuiFileBeforeCommitExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiFileBeforeCommitExtension extension)
        throws HopException {
      throw new HopException("listener is broken");
    }
  }

  public static class RecordingListener
      implements IExtensionPoint<HopGuiFileBeforeCommitExtension> {
    static List<String> seenFiles;
    static String seenGitDirectory;

    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiFileBeforeCommitExtension extension) {
      seenFiles = extension.getFilenames();
      seenGitDirectory = extension.getGitDirectory();
    }
  }
}
