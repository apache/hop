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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Closing Show Lint Results disposes the panel without calling its dispose(). The panel stayed
 * registered, and every later lint update logged "Widget is disposed".
 *
 * @see <a href="https://github.com/apache/hop/issues/8872">#8872</a>
 */
@Tag("uitest")
class LintResultsPanelDisposeTest extends SwtBotTestBase {

  @Test
  void closingTheWindowRemovesThePanelsListener() throws Exception {
    AtomicReference<LintResultsPanel> panel = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          panel.set(new LintResultsPanel(shell, SWT.NONE));
        },
        bot -> {});

    assertTrue(panel.get().isDisposed(), "the window was closed");
    assertFalse(listeners().contains(panel.get()), "a closed panel is still notified");
  }

  @Test
  void anOpenPanelIsNotified() throws Exception {
    AtomicReference<Boolean> registered = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          LintResultsPanel open = new LintResultsPanel(shell, SWT.NONE);
          registered.set(listeners().contains(open));
        },
        bot -> {});

    assertTrue(registered.get());
  }

  private static List<?> listeners() {
    try {
      Field field = LintResultsManager.class.getDeclaredField("listeners");
      field.setAccessible(true);
      return (List<?>) field.get(LintResultsManager.getInstance());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
