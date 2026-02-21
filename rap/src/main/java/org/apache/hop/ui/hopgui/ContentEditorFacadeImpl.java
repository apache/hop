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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.AbstractOperationHandler;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;

/**
 * Hop Web (RAP) implementation of the content editor. Uses Monaco Editor when available
 * (client-side JavaScript), with fallback to a plain Text widget.
 */
public class ContentEditorFacadeImpl extends ContentEditorFacade {

  private static final String MONACO_REMOTE_TYPE = "hop.MonacoEditor";

  @Override
  protected IContentEditorWidget createContentEditorInternal(Composite parent, String languageId) {
    try {
      Composite host = new Composite(parent, SWT.NONE);
      PropsUi.setLook(host);
      FormData fd = new FormData();
      fd.left = new FormAttachment(0, 0);
      fd.right = new FormAttachment(100, 0);
      fd.top = new FormAttachment(0, 0);
      fd.bottom = new FormAttachment(100, 0);
      host.setLayoutData(fd);

      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject(MONACO_REMOTE_TYPE);
      remoteObject.set("parent", WidgetUtil.getId(host));
      remoteObject.set("self", remoteObject.getId());
      remoteObject.set("content", "");
      remoteObject.set("language", languageId != null ? languageId : "plaintext");

      RapMonacoEditorWidget widget = new RapMonacoEditorWidget(host, remoteObject);
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
      return createFallbackTextWidget(parent);
    }
  }

  private static IContentEditorWidget createFallbackTextWidget(Composite parent) {
    Text text = new Text(parent, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(text, Props.WIDGET_STYLE_FIXED);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, 0);
    fd.bottom = new FormAttachment(100, 0);
    text.setLayoutData(fd);
    return new RapContentEditorWidget(text);
  }

  private static class RapMonacoEditorWidget implements IContentEditorWidget {

    private final Composite control;
    private final RemoteObject remoteObject;
    private volatile String cachedContent = "";
    private final java.util.List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private boolean suppressModify;
    private final AbstractOperationHandler operationHandler;

    RapMonacoEditorWidget(Composite control, RemoteObject remoteObject) {
      this.control = control;
      this.remoteObject = remoteObject;
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
              Display display = control.getDisplay();
              if (display == null || control.isDisposed()) {
                return;
              }
              Runnable run =
                  () -> {
                    if (control.isDisposed()) return;
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
      return control;
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
    public void setLanguage(String languageId) {
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
      // Monaco handles selection on client; no-op or could call a remote method if we add one
    }

    @Override
    public void unselectAll() {
      // no-op for Monaco
    }

    @Override
    public void copy() {
      // Monaco handles copy on client
    }
  }

  private static class RapContentEditorWidget implements IContentEditorWidget {

    private final Text text;
    private final java.util.List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private boolean suppressModify;

    RapContentEditorWidget(Text text) {
      this.text = text;
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
      return text;
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
    public void setLanguage(String languageId) {
      // no-op for plain text
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
  }
}
