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
 */

package org.apache.hop.ui.hopgui.perspective.execution;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.GraphicsEnvironment;
import java.lang.reflect.Field;
import java.util.List;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Double-clicking a workflow execution selects its viewer tab. A tab that was created without data
 * used to throw from {@code CTabItem.getData().equals} and the workflow never opened (issue #8601).
 */
@Tag("uitest")
class ExecutionPerspectiveActiveViewerTest {

  private Display display;
  private Shell shell;
  private CTabFolder folder;
  private ExecutionPerspective perspective;
  private ExecutionPerspective previousInstance;

  @BeforeEach
  void openFolder() throws Exception {
    Assumptions.assumeFalse(
        GraphicsEnvironment.isHeadless(),
        "No display available (headless); skipping SWT UI tests.");
    try {
      display = Display.getDefault();
    } catch (SWTException e) {
      Assumptions.abort("No SWT display: " + e.getMessage());
    }
    previousInstance = currentInstance();
    perspective = new ExecutionPerspective();
    shell = new Shell(display);
    folder = new CTabFolder(shell, SWT.CLOSE);
    setField(perspective, "tabFolder", folder);
  }

  @AfterEach
  void closeFolder() throws Exception {
    if (shell != null && !shell.isDisposed()) {
      shell.dispose();
    }
    setInstance(previousInstance);
  }

  @Test
  void nullDataTabDoesNotBlockTheWorkflowViewer() throws Exception {
    StubViewer workflow = new StubViewer("lees-van-kafka", "dae9c3e9-a6ee-4ef2-ad7b-a2e645881f4d");
    CTabItem empty = new CTabItem(folder, SWT.CLOSE);
    CTabItem workflowTab = new CTabItem(folder, SWT.CLOSE);
    workflowTab.setText(workflow.getName());
    workflowTab.setData(workflow);

    assertDoesNotThrow(() -> perspective.setActiveViewer(workflow));

    assertFalse(empty.isDisposed());
    assertNull(empty.getData());
    assertSame(workflow, perspective.getActiveViewer());
    assertSame(workflowTab, folder.getSelection());
    assertEquals(1, workflow.focusCount);
  }

  @Test
  void lostTabDataIsRestoredFromTheViewerControl() {
    StubViewer workflow = new StubViewer("lees-van-kafka", "dae9c3e9-a6ee-4ef2-ad7b-a2e645881f4d");
    Composite body = new Composite(folder, SWT.NONE);
    workflow.control = body;
    CTabItem tab = new CTabItem(folder, SWT.CLOSE);
    tab.setControl(body);

    assertTrue(perspective.activateViewer(workflow));

    assertSame(workflow, tab.getData());
    assertSame(workflow, perspective.getActiveViewer());
  }

  @Test
  void registeredViewerWithoutATabIsDroppedSoItCanBeOpenedAgain() throws Exception {
    StubViewer workflow = new StubViewer("lees-van-kafka", "dae9c3e9-a6ee-4ef2-ad7b-a2e645881f4d");
    viewers().add(workflow);
    CTabItem empty = new CTabItem(folder, SWT.CLOSE);

    assertFalse(perspective.keepExistingViewer(workflow.getLogChannelId(), workflow.getName()));

    assertFalse(empty.isDisposed());
    assertNull(perspective.findViewer(workflow.getLogChannelId(), workflow.getName()));
  }

  @Test
  void missingViewerDoesNothing() {
    assertFalse(perspective.activateViewer(null));
    assertFalse(perspective.keepExistingViewer(null, "lees-van-kafka"));
    assertFalse(perspective.keepExistingViewer("id", null));
    assertNull(perspective.getActiveViewer());
  }

  @SuppressWarnings("unchecked")
  private List<IExecutionViewer> viewers() throws Exception {
    Field field = ExecutionPerspective.class.getDeclaredField("viewers");
    field.setAccessible(true);
    return (List<IExecutionViewer>) field.get(perspective);
  }

  private static ExecutionPerspective currentInstance() throws Exception {
    Field field = ExecutionPerspective.class.getDeclaredField("instance");
    field.setAccessible(true);
    return (ExecutionPerspective) field.get(null);
  }

  private static void setInstance(ExecutionPerspective instance) throws Exception {
    Field field = ExecutionPerspective.class.getDeclaredField("instance");
    field.setAccessible(true);
    field.set(null, instance);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  /** Viewer stand-in that does not build a workflow canvas. */
  private static final class StubViewer implements IExecutionViewer {
    private final String name;
    private final String id;
    private Control control;
    private int focusCount;

    private StubViewer(String name, String id) {
      this.name = name;
      this.id = id;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getLogChannelId() {
      return id;
    }

    @Override
    public Image getTitleImage() {
      return null;
    }

    @Override
    public String getTitleToolTip() {
      return null;
    }

    @Override
    public boolean setFocus() {
      focusCount++;
      return true;
    }

    @Override
    public Control getControl() {
      return control;
    }

    @Override
    public void refresh() {}
  }
}
