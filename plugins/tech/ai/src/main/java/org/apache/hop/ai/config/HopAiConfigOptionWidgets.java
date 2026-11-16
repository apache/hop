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

package org.apache.hop.ai.config;

import static org.apache.hop.ai.config.HopAiConfigOptionPlugin.HINT_DATA_KEY;
import static org.apache.hop.ai.config.HopAiConfigOptionPlugin.WIDGET_CONTEXT_FILES;
import static org.apache.hop.ai.config.HopAiConfigOptionPlugin.WIDGET_DEFAULT_PROVIDER;
import static org.apache.hop.ai.config.HopAiConfigOptionPlugin.WIDGET_DEFAULT_PROVIDER_WARNING;
import static org.apache.hop.ai.config.HopAiConfigOptionPlugin.WIDGET_EXTRA_CONTEXT;

import java.util.Arrays;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Text;

/**
 * The widgets of the AI configuration options in Hop GUI. They live apart from {@link
 * HopAiConfigOptionPlugin}: hop-conf and the hop command line read that class by reflection, also
 * without SWT, and the lambdas here take SWT types.
 */
final class HopAiConfigOptionWidgets {

  private static final Class<?> PKG = HopAiConfigOptionPlugin.class;

  private HopAiConfigOptionWidgets() {}

  /** The widgets have their values: set them up, and follow the project that is open. */
  static void populated(GuiCompositeWidgets compositeWidgets) {
    addContextFilesHint(compositeWidgets);
    alignWithProviderLine(compositeWidgets);
    updateDefaultProviderWarning(compositeWidgets);
    // The providers are metadata of the open project; the list follows when another one opens.
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null && hopGui.getEventsHandler() != null) {
      hopGui
          .getEventsHandler()
          .addEventListener(
              HopAiConfigOptionPlugin.class.getName() + "ProjectActivated",
              e ->
                  hopGui
                      .getDisplay()
                      .asyncExec(
                          () -> reloadProviders(compositeWidgets, hopGui.getMetadataProvider())),
              HopGuiEvents.ProjectActivated.name());
    }
  }

  /** List the providers of the project that is open now, keeping the configured name. */
  static void reloadProviders(
      GuiCompositeWidgets compositeWidgets, IHopMetadataProvider metadataProvider) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER)
            instanceof MetaSelectionLine<?> line)
        || line.isDisposed()) {
      return;
    }
    String name = HopAiConfigSingleton.getConfig().getDefaultProviderName();
    line.setMetadataProvider(metadataProvider);
    try {
      line.fillItems();
    } catch (HopException e) {
      LogChannel.UI.logError("Unable to list the AI providers of the open project", e);
    }
    if (!Const.NVL(name, "").equals(Const.NVL(line.getText(), ""))) {
      line.setText(Const.NVL(name, ""));
    }
    updateDefaultProviderWarning(compositeWidgets);
  }

  /**
   * The default provider is a Hop-wide setting, the providers are project metadata. Say so when the
   * open project has no provider by that name: its sessions then use the provider picked last.
   */
  static void updateDefaultProviderWarning(GuiCompositeWidgets compositeWidgets) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER_WARNING)
            instanceof Link link)
        || link.isDisposed()) {
      return;
    }
    String name = null;
    if (compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER)
        instanceof MetaSelectionLine<?> line) {
      name = line.getText();
      if (!Utils.isEmpty(name) && Arrays.asList(line.getItems()).contains(name)) {
        name = null;
      }
    }
    String text =
        Utils.isEmpty(name)
            ? ""
            : BaseMessages.getString(
                PKG, "HopAiConfigOptionPlugin.DefaultProvider.Missing", name.replace("&", "&&"));
    if (text.equals(link.getText()) && link.getVisible() == !text.isEmpty()) {
      return;
    }
    link.setText(text);
    link.setVisible(!text.isEmpty());
    if (link.getLayoutData() instanceof FormData data) {
      data.height = text.isEmpty() ? 0 : SWT.DEFAULT;
    }
    link.getParent().layout(true, true);
  }

  /**
   * The text fields end where the provider field ends, before its buttons, so their variable icons
   * line up with the provider's in one column.
   */
  static void alignWithProviderLine(GuiCompositeWidgets compositeWidgets) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER)
            instanceof MetaSelectionLine<?> line)
        || line.getwToolBar() == null) {
      return;
    }
    int inset = line.getwToolBar().computeSize(SWT.DEFAULT, SWT.DEFAULT).x + PropsUi.getMargin();
    Composite parent = null;
    for (String id : new String[] {WIDGET_EXTRA_CONTEXT, WIDGET_CONTEXT_FILES}) {
      Control control = compositeWidgets.getWidgetsMap().get(id);
      if (control != null && control.getLayoutData() instanceof FormData data) {
        data.right = new FormAttachment(100, -inset);
        parent = control.getParent();
      }
    }
    if (parent != null) {
      parent.layout(true, true);
    }
  }

  /**
   * An example of what Context files takes, in the empty field. A multi-line field on macOS shows
   * no message of its own, so it is painted.
   */
  static void addContextFilesHint(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(WIDGET_CONTEXT_FILES);
    Text text = control instanceof TextVar textVar ? textVar.getTextWidget() : null;
    if (text == null || text.isDisposed() || text.getData(HINT_DATA_KEY) != null) {
      return;
    }
    text.setData(HINT_DATA_KEY, Boolean.TRUE);
    String hint = BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.ExtraContextFiles.Hint");
    if (EnvironmentUtils.getInstance().isWeb()) {
      text.setMessage(hint);
      return;
    }
    text.addPaintListener(
        e -> {
          if (text.getCharCount() > 0) {
            return;
          }
          e.gc.setForeground(GuiResource.getInstance().getColorDarkGray());
          e.gc.drawText(hint, PropsUi.getMargin(), PropsUi.getMargin() / 2, true);
        });
    text.addListener(SWT.Modify, e -> text.redraw());
  }
}
