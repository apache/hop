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
      remoteObject.listen("focusChanged", true);
      remoteObject.listen("selectionChanged", true);
      remoteObject.listen("findRequested", true);
      remoteObject.listen("executeRequested", true);
      host.addListener(
          SWT.Dispose,
          event -> {
            widget.clearFocus();
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
    text.addListener(
        SWT.KeyDown,
        event -> {
          if ((event.stateMask & SWT.MOD1) == 0 || (event.stateMask & SWT.MOD2) != 0) {
            return;
          }
          if (event.keyCode == 'f') {
            ContentEditorActions.find(widget);
            event.doit = false;
          } else if (event.keyCode == 'h') {
            if (widget.isEditable()) {
              ContentEditorActions.findAndReplace(widget);
            } else {
              ContentEditorActions.find(widget);
            }
            event.doit = false;
          }
        });
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
    if (widget instanceof RapToolbarAware toolbarAware) {
      toolbarAware.setToolbarWidgets(toolbarWidgets);
      toolbarAware.updateToolbar();
    }
    toolbar.pack();
    return toolbar;
  }

  private interface RapToolbarAware {
    void setToolbarWidgets(GuiToolbarWidgets toolbarWidgets);

    void updateToolbar();
  }

  private static class RapMonacoEditorWidget implements IContentEditorWidget, RapToolbarAware {

    private final Composite root;
    private final RemoteObject remoteObject;
    private final Display display;
    private volatile String cachedContent = "";
    private volatile int selectionStart;
    private volatile int selectionEnd;
    private volatile boolean readOnly;
    private GuiToolbarWidgets toolbarWidgets;
    private final java.util.List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private boolean suppressModify;
    private volatile String languageId;
    private final AbstractOperationHandler operationHandler;

    RapMonacoEditorWidget(
        Composite root, Composite host, RemoteObject remoteObject, String languageId) {
      this.root = root;
      this.remoteObject = remoteObject;
      this.display = host.getDisplay();
      this.languageId = languageId != null ? languageId : "";
      this.operationHandler =
          new AbstractOperationHandler() {
            @Override
            public void handleNotify(String event, JsonObject properties) {
              if ("focusChanged".equals(event) && properties.get("focused") != null) {
                setEditorFocused(properties.get("focused").asBoolean());
                return;
              }
              if ("selectionChanged".equals(event)) {
                if (properties.get("start") != null) {
                  selectionStart = properties.get("start").asInt();
                }
                if (properties.get("end") != null) {
                  selectionEnd = properties.get("end").asInt();
                }
                return;
              }
              if ("findRequested".equals(event)) {
                boolean replace =
                    properties.get("replace") != null && properties.get("replace").asBoolean();
                Display current = host.getDisplay();
                if (current == null || host.isDisposed()) {
                  return;
                }
                current.asyncExec(
                    () -> {
                      if (host.isDisposed()) {
                        return;
                      }
                      if (replace && isEditable()) {
                        ContentEditorActions.findAndReplace(RapMonacoEditorWidget.this);
                      } else {
                        ContentEditorActions.find(RapMonacoEditorWidget.this);
                      }
                    });
                return;
              }
              if ("executeRequested".equals(event)) {
                Display current = host.getDisplay();
                if (current == null || host.isDisposed()) {
                  return;
                }
                current.asyncExec(
                    () -> {
                      if (root.isDisposed()) {
                        return;
                      }
                      Runnable action = IContentEditorWidget.executeActionOf(root);
                      if (action != null) {
                        action.run();
                      }
                    });
                return;
              }
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

    private void setEditorFocused(boolean focused) {
      if (display == null || display.isDisposed()) {
        return;
      }
      Object focusedEditor = display.getData(HopGui.TEXT_EDITOR_FOCUS_DATA);
      if (focused) {
        display.setData(HopGui.TEXT_EDITOR_FOCUS_DATA, this);
      } else if (focusedEditor == this) {
        display.setData(HopGui.TEXT_EDITOR_FOCUS_DATA, null);
      }
    }

    void clearFocus() {
      setEditorFocused(false);
    }

    @Override
    public void setToolbarWidgets(GuiToolbarWidgets toolbarWidgets) {
      this.toolbarWidgets = toolbarWidgets;
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
      selectionStart = 0;
      selectionEnd = 0;
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
      this.readOnly = readOnly;
      remoteObject.set("readOnly", readOnly);
      updateToolbar();
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

    @Override
    public String getSelectionText() {
      int start = clampedOffset(Math.min(selectionStart, selectionEnd));
      int end = clampedOffset(Math.max(selectionStart, selectionEnd));
      if (end <= start) {
        return "";
      }
      return cachedContent.substring(start, end);
    }

    @Override
    public int getSelectionCount() {
      return Math.abs(selectionEnd - selectionStart);
    }

    @Override
    public void setSelection(int start, int end) {
      selectionStart = Math.max(0, start);
      selectionEnd = Math.max(selectionStart, end);
      JsonObject obj = new JsonObject();
      obj.add("start", selectionStart);
      obj.add("end", selectionEnd);
      remoteObject.call("setSelection", obj);
    }

    @Override
    public int getCaretPosition() {
      return Math.max(selectionStart, selectionEnd);
    }

    @Override
    public void setCaretPosition(int position) {
      setSelection(position, position);
    }

    @Override
    public void insert(String text) {
      String insertion = text != null ? text : "";
      int start = clampedOffset(Math.min(selectionStart, selectionEnd));
      int end = clampedOffset(Math.max(selectionStart, selectionEnd));
      cachedContent = cachedContent.substring(0, start) + insertion + cachedContent.substring(end);
      int caret = start + insertion.length();
      selectionStart = caret;
      selectionEnd = caret;
      JsonObject obj = new JsonObject();
      obj.add("text", insertion);
      obj.add("start", start);
      obj.add("end", end);
      remoteObject.call("insert", obj);
    }

    @Override
    public boolean isEditable() {
      return !readOnly;
    }

    @Override
    public void updateToolbar() {
      if (toolbarWidgets == null) {
        return;
      }
      toolbarWidgets.enableToolbarItem(ContentEditorActions.ID_TOOLBAR_FIND, true);
      toolbarWidgets.enableToolbarItem(ContentEditorActions.ID_TOOLBAR_FIND_REPLACE, isEditable());
    }

    private int clampedOffset(int offset) {
      if (offset < 0) {
        return 0;
      }
      return Math.min(offset, cachedContent.length());
    }
  }

  private static class RapContentEditorWidget implements IContentEditorWidget, RapToolbarAware {

    private final Composite root;
    private final Text text;
    private final java.util.List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private boolean suppressModify;
    private volatile String languageId;
    private GuiToolbarWidgets toolbarWidgets;

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
      updateToolbar();
    }

    @Override
    public void setToolbarWidgets(GuiToolbarWidgets toolbarWidgets) {
      this.toolbarWidgets = toolbarWidgets;
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

    @Override
    public String getSelectionText() {
      String selected = text.getSelectionText();
      return selected != null ? selected : "";
    }

    @Override
    public int getSelectionCount() {
      return text.getSelectionCount();
    }

    @Override
    public void setSelection(int start, int end) {
      text.setSelection(start, end);
    }

    @Override
    public int getCaretPosition() {
      return text.getCaretPosition();
    }

    @Override
    public void setCaretPosition(int position) {
      text.setSelection(position);
    }

    @Override
    public void insert(String content) {
      text.insert(content != null ? content : "");
    }

    @Override
    public boolean isEditable() {
      return text.getEditable();
    }

    @Override
    public boolean setFocus() {
      return !text.isDisposed() && text.setFocus();
    }

    @Override
    public void updateToolbar() {
      if (toolbarWidgets == null) {
        return;
      }
      toolbarWidgets.enableToolbarItem(ContentEditorActions.ID_TOOLBAR_FIND, true);
      toolbarWidgets.enableToolbarItem(ContentEditorActions.ID_TOOLBAR_FIND_REPLACE, isEditable());
    }
  }
}
