/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use it except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.AbstractOperationHandler;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;
import org.jspecify.annotations.Nullable;

/**
 * Hop Web (RAP) implementation of the content editor. Uses Monaco Editor when available
 * (client-side JavaScript), with fallback to a plain Text widget.
 *
 * <p>Builds the shared {@link IContentEditorWidget#GUI_PLUGIN_TOOLBAR_PARENT_ID} toolbar so
 * plugin-contributed actions (e.g. Markdown preview) appear in hop-web as well as desktop.
 */
public class ContentEditorFacadeImpl extends ContentEditorFacade {

  private static final String MONACO_REMOTE_TYPE = "hop.MonacoEditor";

  @Override
  protected IContentEditorWidget createContentEditorInternal(Composite parent, String languageId) {
    try {
      Composite root = createRootComposite(parent);

      // Editor host (Monaco parent) fills the area below the toolbar.
      Composite host = new Composite(root, SWT.NONE);
      PropsUi.setLook(host);
      // Avoid a light flash before Monaco mounts when Hop Web is in dark mode
      if (PropsUi.getInstance().isDarkMode()) {
        host.setBackground(host.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
      }

      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject(MONACO_REMOTE_TYPE);
      remoteObject.set("parent", WidgetUtil.getId(host));
      remoteObject.set("self", remoteObject.getId());
      remoteObject.set("content", "");
      remoteObject.set("language", languageId != null ? languageId : "plaintext");
      // Match Hop Web theme (/ui-dark → Monaco vs-dark), same idea as canvas themeId
      remoteObject.set("theme", PropsUi.getInstance().isDarkMode() ? "vs-dark" : "vs");

      RapMonacoEditorWidget widget =
          new RapMonacoEditorWidget(root, host, remoteObject, languageId);
      Control toolbar = addToolbar(root, widget);
      host.setLayoutData(FormDataBuilder.builder().top(toolbar).bottom().fullWidth().build());

      remoteObject.setHandler(widget.getOperationHandler());
      remoteObject.listen("contentChanged", true);
      host.addListener(
          SWT.Dispose,
          event -> {
            try {
              remoteObject.destroy();
            } catch (Exception ignored) {
              // ignore
            }
          });
      return widget;
    } catch (Exception e) {
      LogChannel.UI.logDebug("Monaco editor not available, using plain Text: " + e.getMessage());
      return createFallbackTextWidget(parent, languageId);
    }
  }

  private static IContentEditorWidget createFallbackTextWidget(
      Composite parent, String languageId) {
    Composite root = createRootComposite(parent);

    Text text = new Text(root, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
    PropsUi.setLook(text, Props.WIDGET_STYLE_FIXED);

    RapContentEditorWidget widget = new RapContentEditorWidget(root, text, languageId);
    Control toolbar = addToolbar(root, widget);
    text.setLayoutData(FormDataBuilder.builder().top(toolbar).bottom().fullWidth().build());
    return widget;
  }

  private static Composite createRootComposite(Composite parent) {
    Composite root = new Composite(parent, SWT.NONE);
    root.setLayout(new FormLayout());
    root.setLayoutData(FormDataBuilder.builder().fullSize().build());
    PropsUi.setLook(root);
    return root;
  }

  /**
   * Create the shared content-editor toolbar on {@code root}, registering {@code widget} for
   * toolbar filters and static listeners.
   *
   * @return the toolbar control
   */
  private static Control addToolbar(Composite root, IContentEditorWidget widget) {
    IToolbarContainer toolbarContainer =
        ToolbarFacade.createToolbarContainer(root, SWT.WRAP | SWT.RIGHT | SWT.HORIZONTAL);
    Control toolbar = toolbarContainer.getControl();
    toolbar.setLayoutData(FormDataBuilder.builder().top().fullWidth().build());
    PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    GuiToolbarWidgets toolbarWidgets = new GuiToolbarWidgets();
    toolbarWidgets.registerGuiPluginObject(widget);
    toolbarWidgets.createToolbarWidgets(
        toolbarContainer, IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolbar.pack();
    return toolbar;
  }

  private static class RapMonacoEditorWidget implements IContentEditorWidget {

    private final Composite root;
    private final RemoteObject remoteObject;
    private volatile String cachedContent = "";
    private final java.util.List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private boolean suppressModify;
    private volatile String languageId;
    private final AbstractOperationHandler operationHandler;

    RapMonacoEditorWidget(
        Composite root, Composite host, RemoteObject remoteObject, String languageId) {
      this.root = root;
      this.remoteObject = remoteObject;
      this.languageId = languageId != null ? languageId : "";
      this.operationHandler =
          new AbstractOperationHandler() {
            @Override
            public void handleNotify(String event, JsonObject properties) {
              if (!"contentChanged".equals(event) || properties.get("content") == null) {
                return;
              }
              String newContent = properties.get("content").asString();
              cachedContent = newContent != null ? newContent : "";
              if (suppressModify) {
                return;
              }
              Display display = host.getDisplay();
              if (display == null || host.isDisposed()) {
                return;
              }
              Runnable run =
                  () -> {
                    if (host.isDisposed()) return;
                    for (ModifyListener listener : modifyListeners) {
                      try {
                        listener.modifyText(null);
                      } catch (Exception ignored) {
                        // ignore
                      }
                    }
                  };
              if (Display.getCurrent() == display) {
                run.run();
              } else {
                display.asyncExec(run);
              }
            }
          };
    }

    AbstractOperationHandler getOperationHandler() {
      return operationHandler;
    }

    @Override
    public Control getControl() {
      return root;
    }

    @Override
    public String getText() {
      return cachedContent;
    }

    @Override
    public void setText(String content) {
      String s = content != null ? content : "";
      cachedContent = s;
      remoteObject.set("content", s);
    }

    @Override
    public void setTextSuppressModify(String content) {
      suppressModify = true;
      try {
        setText(content);
      } finally {
        suppressModify = false;
      }
    }

    @Override
    public @Nullable String getLanguage() {
      return languageId;
    }

    @Override
    public void setLanguage(String languageId) {
      this.languageId = languageId != null ? languageId : "";
      remoteObject.set("language", languageId != null ? languageId : "plaintext");
    }

    @Override
    public void setReadOnly(boolean readOnly) {
      remoteObject.set("readOnly", readOnly);
    }

    @Override
    public void addModifyListener(ModifyListener listener) {
      if (listener != null) {
        modifyListeners.add(listener);
      }
    }

    @Override
    public void removeModifyListener(ModifyListener listener) {
      if (listener != null) {
        modifyListeners.remove(listener);
      }
    }

    @Override
    public void selectAll() {
      // Monaco handles selection on client; no-op until a remote method is added
    }

    @Override
    public void unselectAll() {
      // no-op for Monaco
    }

    @Override
    public void copy() {
      // Monaco handles copy on client
    }

    @Override
    public void cut() {
      // no-op for Monaco until clipboard remote ops exist
    }

    @Override
    public void paste() {
      // no-op for Monaco until clipboard remote ops exist
    }

    @Override
    public void undo() {
      // no-op for Monaco until undo remote ops exist
    }

    @Override
    public void redo() {
      // no-op for Monaco until redo remote ops exist
    }
  }

  private static class RapContentEditorWidget implements IContentEditorWidget {

    private final Composite root;
    private final Text text;
    private final java.util.List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private boolean suppressModify;
    private volatile String languageId;

    RapContentEditorWidget(Composite root, Text text, String languageId) {
      this.root = root;
      this.text = text;
      this.languageId = languageId != null ? languageId : "";
      text.addModifyListener(
          e -> {
            if (suppressModify) return;
            for (ModifyListener listener : modifyListeners) {
              try {
                listener.modifyText(e);
              } catch (Exception ignored) {
                // ignore
              }
            }
          });
    }

    @Override
    public Control getControl() {
      return root;
    }

    @Override
    public String getText() {
      return text.getText();
    }

    @Override
    public void setText(String content) {
      text.setText(content != null ? content : "");
    }

    @Override
    public void setTextSuppressModify(String content) {
      suppressModify = true;
      try {
        text.setText(content != null ? content : "");
      } finally {
        suppressModify = false;
      }
    }

    @Override
    public @Nullable String getLanguage() {
      return languageId;
    }

    @Override
    public void setLanguage(String languageId) {
      this.languageId = languageId != null ? languageId : "";
    }

    @Override
    public void setReadOnly(boolean readOnly) {
      text.setEditable(!readOnly);
    }

    @Override
    public void addModifyListener(ModifyListener listener) {
      if (listener != null) modifyListeners.add(listener);
    }

    @Override
    public void removeModifyListener(ModifyListener listener) {
      if (listener != null) modifyListeners.remove(listener);
    }

    @Override
    public void selectAll() {
      text.selectAll();
    }

    @Override
    public void unselectAll() {
      text.setSelection(0, 0);
    }

    @Override
    public void copy() {
      text.copy();
    }

    @Override
    public void cut() {
      text.cut();
    }

    @Override
    public void paste() {
      text.paste();
    }

    @Override
    public void undo() {
      // SWT Text has no standard undo API
    }

    @Override
    public void redo() {
      // SWT Text has no standard redo API
    }
  }
}
