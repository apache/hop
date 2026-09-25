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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Double-clicking a workflow execution selects its viewer tab. A tab with no data used to throw
 * from {@code CTabItem.getData().equals} (issue #8601). Opening a viewer whose setup fails must not
 * leave that composite parented to the folder.
 */
@Tag("uitest")
class ExecutionPerspectiveActiveViewerTest extends SwtBotTestBase {

  private Shell shell;
  private CTabFolder folder;
  private ExecutionPerspective perspective;
  private ExecutionPerspective previousInstance;

  @BeforeEach
  void openFolder() throws Exception {
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
  void nullDataTabDoesNotHideTheOpenViewer() {
    StubViewer workflow = new StubViewer("lees-van-kafka", "dae9c3e9");
    CTabItem empty = new CTabItem(folder, SWT.CLOSE);
    CTabItem workflowTab = new CTabItem(folder, SWT.CLOSE);
    workflowTab.setText(workflow.getName());
    workflowTab.setData(workflow);

    assertDoesNotThrow(() -> perspective.setActiveViewer(workflow));

    assertSame(workflow, perspective.getActiveViewer());
    assertSame(workflowTab, folder.getSelection());
    assertEquals(1, workflow.focusCount);
    assertNull(empty.getData());
  }

  @Test
  void nullViewerDoesNothing() {
    assertDoesNotThrow(() -> perspective.setActiveViewer(null));
    assertNull(perspective.getActiveViewer());
  }

  @Test
  void failedOpenLeavesNoTabAndDisposesTheViewer() {
    Composite body = new Composite(folder, SWT.NONE);
    StubViewer workflow = new StubViewer("lees-van-kafka", "dae9c3e9");
    workflow.control = body;
    workflow.imageFailure = new Error("icon");

    Error failure = assertThrows(Error.class, () -> perspective.addViewer(workflow));

    assertSame(workflow.imageFailure, failure);
    assertEquals(0, folder.getItemCount());
    assertNull(perspective.findViewer(workflow.getLogChannelId(), workflow.getName()));
    assertTrue(body.isDisposed());
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
    private Error imageFailure;

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
      if (imageFailure != null) {
        throw imageFailure;
      }
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
