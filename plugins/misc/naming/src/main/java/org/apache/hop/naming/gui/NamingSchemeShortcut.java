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

package org.apache.hop.naming.gui;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeSelector;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ITextWidgetShortcut;
import org.apache.hop.ui.core.widget.NamingSchemeColumnApplierRegistry;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.core.widget.TextWidgetShortcutContext;
import org.apache.hop.ui.core.widget.TextWidgetShortcutRegistry;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Shell;

/**
 * Registers the naming-scheme keyboard shortcut (CTRL-SHIFT-N, and CTRL-SPACE when variables are
 * disabled) on Hop Gui start.
 */
@ExtensionPoint(
    id = "NamingSchemeShortcutRegister",
    description = "Register the TextVar naming-scheme shortcut",
    extensionPointId = "HopGuiStart")
public class NamingSchemeShortcut implements IExtensionPoint, ITextWidgetShortcut {

  private static final Class<?> PKG = NamingSchemeShortcut.class;

  static final NamingSchemeShortcut INSTANCE = new NamingSchemeShortcut();

  /** Last scheme name chosen per type code. */
  private final Map<String, String> lastUsedByType = new ConcurrentHashMap<>();

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object hopGui)
      throws HopException {
    TextWidgetShortcutRegistry.getInstance().register(INSTANCE);
    NamingSchemeColumnApplierRegistry.getInstance().register(NamingSchemeColumnApplier.INSTANCE);
  }

  @Override
  public boolean isHotKey(KeyEvent event, boolean variablesEnabled) {
    if (isApplyShortcut(event)) {
      return true;
    }
    return !variablesEnabled && isControlSpace(event);
  }

  @Override
  public void apply(TextWidgetShortcutContext context) {
    if (context == null || context.getGetText() == null || context.getSetText() == null) {
      return;
    }
    String current = context.getGetText().get();
    if (shouldSkip(current)) {
      return;
    }

    String typeCode = context.getNamingSchemeType();
    Shell shell = context.getShell();
    if (shell == null || shell.isDisposed()) {
      return;
    }

    try {
      List<NamingScheme> schemes = loadSchemes(typeCode);
      if (schemes.isEmpty()) {
        return;
      }

      NamingScheme chosen = pickScheme(shell, schemes, typeCode, current);
      if (chosen == null) {
        return;
      }
      lastUsedByType.put(typeCode, chosen.getName());
      String rewritten = NamingEngine.apply(chosen, current);
      if (rewritten != null && !rewritten.equals(current)) {
        context.getSetText().accept(rewritten);
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "Naming.Error.Title"),
          BaseMessages.getString(PKG, "Naming.Error.Message"),
          e);
    }
  }

  @Override
  public void onIndicatorClick(TextWidgetShortcutContext context) {
    if (context == null) {
      return;
    }
    Shell shell = context.getShell();
    if (shell == null || shell.isDisposed()) {
      return;
    }
    String typeCode = context.getNamingSchemeType();
    try {
      List<NamingScheme> schemes = loadSchemes(typeCode);
      if (schemes.isEmpty()) {
        MessageBox box = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "Naming.NoSchemes.Title"));
        box.setMessage(BaseMessages.getString(PKG, "Naming.NoSchemes.Message"));
        box.open();
        return;
      }
      NamingScheme chosen = pickSchemeToOpen(shell, schemes, typeCode);
      if (chosen == null) {
        return;
      }
      lastUsedByType.put(typeCode, chosen.getName());
      IVariables vars =
          context.getVariables() != null
              ? context.getVariables()
              : HopGui.getInstance().getVariables();
      MetadataManager<NamingScheme> manager =
          new MetadataManager<>(
              vars, HopGui.getInstance().getMetadataProvider(), NamingScheme.class, shell);
      manager.editMetadata(chosen.getName());
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "Naming.Error.Title"),
          BaseMessages.getString(PKG, "Naming.Error.Message"),
          e);
    }
  }

  /**
   * Skip empty cells, table null markers, and values that already contain a variable expression so
   * NamingEngine cannot chew {@code ${...}}.
   */
  static boolean shouldSkip(String value) {
    return NamingEngine.shouldSkip(value);
  }

  static boolean isApplyShortcut(KeyEvent event) {
    if (event == null) {
      return false;
    }
    boolean control = (event.stateMask & SWT.CONTROL) != 0;
    boolean shift = (event.stateMask & SWT.SHIFT) != 0;
    boolean alt = (event.stateMask & SWT.ALT) != 0;
    if (!control || !shift || alt) {
      return false;
    }
    if (OsHelper.isMac()) {
      return event.keyCode == 'n' || event.keyCode == 'N';
    }
    return Character.toLowerCase(event.character) == 'n'
        || event.keyCode == 'n'
        || event.keyCode == 'N';
  }

  static boolean isControlSpace(KeyEvent event) {
    if (event == null) {
      return false;
    }
    if ("zh".equals(System.getProperty("user.language"))) {
      return event.character == ' '
          && ((event.stateMask & SWT.CONTROL) != 0)
          && ((event.stateMask & SWT.ALT) != 0);
    }
    if (OsHelper.isMac()) {
      return event.keyCode == 32
          && ((event.stateMask & SWT.CONTROL) != 0)
          && ((event.stateMask & SWT.ALT) == 0);
    }
    return event.character == ' '
        && ((event.stateMask & SWT.CONTROL) != 0)
        && ((event.stateMask & SWT.ALT) == 0);
  }

  private List<NamingScheme> loadSchemes(String typeCode) throws Exception {
    IHopMetadataSerializer<NamingScheme> serializer =
        HopGui.getInstance().getMetadataProvider().getSerializer(NamingScheme.class);
    return NamingSchemeSelector.matching(serializer.loadAll(), typeCode);
  }

  private NamingScheme pickScheme(
      Shell shell, List<NamingScheme> schemes, String typeCode, String input) {
    if (schemes.size() == 1) {
      return schemes.get(0);
    }

    String[] labels = new String[schemes.size()];
    int preselect = 0;
    String last = lastUsedByType.get(typeCode);
    for (int i = 0; i < schemes.size(); i++) {
      NamingScheme scheme = schemes.get(i);
      String preview = NamingEngine.apply(scheme, input);
      labels[i] = scheme.getName() + "  →  " + preview;
      if (last != null && last.equals(scheme.getName())) {
        preselect = i;
      }
    }

    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            shell,
            labels,
            BaseMessages.getString(PKG, "Naming.SchemeSelection.Title"),
            BaseMessages.getString(PKG, "Naming.Shortcut.Message"));
    dialog.setSelectedNrs(new int[] {preselect});
    if (dialog.open() == null) {
      return null;
    }
    int index = dialog.getSelectionNr();
    if (index < 0 || index >= schemes.size()) {
      return null;
    }
    return schemes.get(index);
  }

  private NamingScheme pickSchemeToOpen(Shell shell, List<NamingScheme> schemes, String typeCode) {
    if (schemes.size() == 1) {
      return schemes.get(0);
    }
    String[] names = new String[schemes.size()];
    int preselect = 0;
    String last = lastUsedByType.get(typeCode);
    for (int i = 0; i < schemes.size(); i++) {
      names[i] = schemes.get(i).getName();
      if (last != null && last.equals(names[i])) {
        preselect = i;
      }
    }
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            shell,
            names,
            BaseMessages.getString(PKG, "Naming.Open.Title"),
            BaseMessages.getString(PKG, "Naming.Open.Message"));
    dialog.setSelectedNrs(new int[] {preselect});
    if (dialog.open() == null) {
      return null;
    }
    int index = dialog.getSelectionNr();
    if (index < 0 || index >= schemes.size()) {
      return null;
    }
    return schemes.get(index);
  }
}
