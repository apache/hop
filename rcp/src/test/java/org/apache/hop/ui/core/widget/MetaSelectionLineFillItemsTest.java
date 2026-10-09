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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.awt.GraphicsEnvironment;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Refreshing a metadata combo is not an edit. Run configuration editors do this when their tab is
 * selected, and on Windows {@code CCombo.setText} notifies Modify even when the text is unchanged
 * (issue #8758).
 *
 * <p>Lives in hop-ui-rcp because constructing the widget needs the desktop look-and-feel
 * implementations ({@code TextSizeUtilFacadeImpl}, {@code ToolbarFacadeImpl}).
 */
@Tag("uitest")
class MetaSelectionLineFillItemsTest {

  private Display display;
  private Shell shell;

  @BeforeEach
  void openShell() throws Exception {
    assumeFalse(GraphicsEnvironment.isHeadless(), "No display available; skipping SWT test.");
    HopEnvironment.init();
    display = Display.getDefault();
    PropsUi.getInstance();
    GuiResource.getInstance();
    shell = new Shell(display, SWT.NONE);
  }

  @AfterEach
  void disposeShell() {
    if (shell != null && !shell.isDisposed()) {
      shell.dispose();
    }
  }

  @Test
  void refreshingTheListDoesNotNotifyModify() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setName("neo-location");
    provider.getSerializer(ExecutionInfoLocation.class).save(location);

    // Read-only makes setItems clear the text, so a refresh has to write it back. That write
    // notifies Modify unless the refresh detaches listeners.
    MetaSelectionLine<ExecutionInfoLocation> line =
        new MetaSelectionLine<>(
            Variables.getADefaultVariableSpace(),
            provider,
            ExecutionInfoLocation.class,
            shell,
            SWT.READ_ONLY,
            "Location",
            "Location tooltip");
    line.fillItems();
    line.setText("neo-location");

    AtomicInteger modifications = new AtomicInteger();
    AtomicInteger selections = new AtomicInteger();
    line.getComboWidget().addListener(SWT.Modify, event -> modifications.incrementAndGet());
    line.getComboWidget().addListener(SWT.Selection, event -> selections.incrementAndGet());

    line.fillItems();

    assertEquals("neo-location", line.getText());
    assertArrayEquals(new String[] {"neo-location"}, line.getItems());
    assertEquals(0, modifications.get());
    assertEquals(0, selections.get());

    ExecutionInfoLocation added = new ExecutionInfoLocation();
    added.setName("file-location");
    provider.getSerializer(ExecutionInfoLocation.class).save(added);
    line.fillItems();

    assertEquals("neo-location", line.getText());
    assertArrayEquals(new String[] {"file-location", "neo-location"}, line.getItems());
    assertEquals(0, modifications.get());
    assertEquals(0, selections.get());

    // Listeners are back: a real edit still marks the control changed.
    line.setText("file-location");
    assertEquals(1, modifications.get());
  }
}
