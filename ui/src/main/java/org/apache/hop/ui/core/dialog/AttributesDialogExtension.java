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

package org.apache.hop.ui.core.dialog;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.variables.IVariables;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.widgets.Shell;

/**
 * Payload for dialog-tab contribution extension points (e.g. lifecycle environment dialog).
 *
 * <p>Optional plugins receive a {@link CTabFolder} to add tabs and a mutable {@link
 * AttributesContext} for namespaced settings. Register load/save callbacks so the host dialog can
 * refresh widgets from attributes and flush widget values back before OK.
 */
@Getter
public class AttributesDialogExtension {

  private final Shell shell;
  private final CTabFolder tabFolder;
  private final IVariables variables;
  private final AttributesContext context;
  private final List<Consumer<AttributesContext>> loadCallbacks = new ArrayList<>();
  private final List<Consumer<AttributesContext>> saveCallbacks = new ArrayList<>();

  public AttributesDialogExtension(
      Shell shell, CTabFolder tabFolder, IVariables variables, AttributesContext context) {
    this.shell = shell;
    this.tabFolder = tabFolder;
    this.variables = variables;
    this.context = context;
  }

  /** Called by the host after building standard tabs so plugins can create widgets. */
  public void addLoadCallback(Consumer<AttributesContext> callback) {
    if (callback != null) {
      loadCallbacks.add(callback);
    }
  }

  /** Called by the host before persisting so plugins can write widgets into attributes. */
  public void addSaveCallback(Consumer<AttributesContext> callback) {
    if (callback != null) {
      saveCallbacks.add(callback);
    }
  }

  public void runLoadCallbacks() {
    for (Consumer<AttributesContext> callback : loadCallbacks) {
      callback.accept(context);
    }
  }

  public void runSaveCallbacks() {
    for (Consumer<AttributesContext> callback : saveCallbacks) {
      callback.accept(context);
    }
  }
}
