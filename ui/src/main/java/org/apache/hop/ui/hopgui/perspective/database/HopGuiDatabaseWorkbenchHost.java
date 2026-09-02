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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.function.BooleanSupplier;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/** {@link IDatabaseWorkbenchHost} backed by a {@link HopGui} session. */
public class HopGuiDatabaseWorkbenchHost implements IDatabaseWorkbenchHost {

  private final HopGui hopGui;
  private final BooleanSupplier alive;
  private final Runnable onActivate;

  public HopGuiDatabaseWorkbenchHost(HopGui hopGui, BooleanSupplier alive, Runnable onActivate) {
    this.hopGui = hopGui;
    this.alive = alive;
    this.onActivate = onActivate;
  }

  @Override
  public HopGui getHopGui() {
    return hopGui;
  }

  @Override
  public Shell getShell() {
    return hopGui.getShell();
  }

  @Override
  public Display getDisplay() {
    return hopGui.getDisplay();
  }

  @Override
  public IVariables getVariables() {
    return hopGui.getVariables();
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return hopGui.getMetadataProvider();
  }

  @Override
  public ILoggingObject getLoggingObject() {
    return hopGui.getLoggingObject();
  }

  @Override
  public void activate() {
    if (onActivate != null) {
      onActivate.run();
    }
  }

  @Override
  public void asyncExec(Runnable runnable) {
    Display display = hopGui.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (alive != null && !alive.getAsBoolean()) {
            return;
          }
          runnable.run();
        });
  }

  @Override
  public void updateGui(IHopFileTypeHandler handler) {
    if (hopGui == null || handler == null) {
      return;
    }
    hopGui.handleFileCapabilities(
        handler.getFileType(), handler, handler.hasChanged(), false, false);
  }
}
