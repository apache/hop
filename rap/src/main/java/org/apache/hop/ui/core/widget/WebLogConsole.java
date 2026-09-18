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

import java.util.ArrayDeque;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.rap.json.JsonArray;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.json.JsonValue;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.AbstractOperationHandler;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MenuDetectEvent;
import org.eclipse.swt.events.MenuDetectListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;

/**
 * The Hop Web log view: a browser-side console (log-console.js) that receives only the lines added
 * since the last request.
 *
 * <p>The plain Text this replaces was rewritten in full on every append, and RAP then re-sent the
 * whole log: with the default unbounded log size a pipeline that logged 30k rows pushed 4-17 MB per
 * second to the browser and every request took over a second (issue #8435). Here a request carries
 * the new lines only, and both ends keep no more than {@link #getMaxLines()} of them.
 *
 * <p>The text is also kept on the server so the log toolbar keeps working: copy all, show errors
 * and the filter rebuild read {@link #getText()}. Selection is the browser's own; what the user
 * selected is reported back so "copy selection" works, capped so a select-all does not upload the
 * whole log again.
 */
public class WebLogConsole extends TextComposite implements ILogConsole {

  /** Lines kept when {@code HOP_MAX_LOG_SIZE_IN_LINES} says "keep everything" (0). */
  public static final int DEFAULT_MAX_LINES = 20000;

  /** Longest selection the browser reports back, in characters. */
  static final int MAX_REPORTED_SELECTION = 64 * 1024;

  /**
   * Lines one refresh may add. At row level a pipeline logs tens of thousands of lines a second;
   * shipping them all cost a 2.8 s request of 18 MB per second and the browser dropped all but the
   * last {@link #DEFAULT_MAX_LINES} of them on arrival.
   */
  static final int MAX_LINES_PER_REFRESH = 5000;

  private static final String REMOTE_TYPE = "hop.LogConsole";

  /** The composite the browser-side console is mounted in; it gets the font, colours and menu. */
  private final Composite host;

  private final RemoteObject remoteObject;
  private final ArrayDeque<String> lines = new ArrayDeque<>();
  private final int maxLines;
  private int charCount;
  private String selectionText = "";
  private boolean editable;

  public WebLogConsole(Composite parent, int style) {
    super(parent, SWT.NONE, true, TextComposite.STYLE_TYPE_LOG);
    maxLines = configuredMaxLines();

    host = new Composite(this, style & SWT.BORDER);
    PropsUi.setLook(host);
    Control top = getTopControl();
    FormDataBuilder layout = FormDataBuilder.builder().fullWidth().bottom();
    host.setLayoutData((top != null ? layout.top(top) : layout.top()).build());

    Connection connection = RWT.getUISession().getConnection();
    remoteObject = connection.createRemoteObject(REMOTE_TYPE);
    remoteObject.set("parent", WidgetUtil.getId(host));
    remoteObject.set("maxLines", maxLines);
    remoteObject.set("maxSelection", MAX_REPORTED_SELECTION);
    // getText() joins lines with the platform separator, like the desktop widget; the browser
    // needs its length to map the offsets that setSelection() sends.
    remoteObject.set("separatorLength", Const.CR.length());
    remoteObject.setHandler(
        new AbstractOperationHandler() {
          @Override
          public void handleNotify(String event, JsonObject properties) {
            if ("selectionChanged".equals(event)) {
              JsonValue value = properties.get("text");
              selectionText = value != null && value.isString() ? value.asString() : "";
              updateToolbar();
            }
          }
        });
    remoteObject.listen("selectionChanged", true);
    remoteObject.set("color", toCss(host.getForeground()));
    host.addListener(SWT.Dispose, event -> remoteObject.destroy());
  }

  /** The configured maximum, or {@link #DEFAULT_MAX_LINES} when the log store is unbounded. */
  static int configuredMaxLines() {
    DescribedVariable variable =
        HopConfig.getInstance().findDescribedVariable(Const.HOP_MAX_LOG_SIZE_IN_LINES);
    int configured =
        variable == null
            ? Const.MAX_NR_LOG_LINES
            : Const.toInt(variable.getValue(), Const.MAX_NR_LOG_LINES);
    return configured > 0 ? configured : DEFAULT_MAX_LINES;
  }

  public int getMaxLines() {
    return maxLines;
  }

  @Override
  public int getMaxLinesPerRefresh() {
    return Math.min(maxLines, MAX_LINES_PER_REFRESH);
  }

  // ILogConsole

  @Override
  public void appendLines(List<Line> newLines) {
    if (newLines == null || newLines.isEmpty() || isDisposed()) {
      return;
    }
    JsonArray texts = new JsonArray();
    JsonArray errors = new JsonArray();
    for (Line line : newLines) {
      String text = Const.NVL(line.text(), "");
      lines.addLast(text);
      charCount += text.length() + Const.CR.length();
      texts.add(text);
      errors.add(line.error());
    }
    while (lines.size() > maxLines) {
      charCount -= lines.removeFirst().length() + Const.CR.length();
    }
    JsonObject parameters = new JsonObject();
    parameters.add("lines", texts);
    parameters.add("errors", errors);
    remoteObject.call("append", parameters);
  }

  @Override
  public void clear() {
    lines.clear();
    charCount = 0;
    selectionText = "";
    if (!isDisposed()) {
      remoteObject.call("clear", null);
    }
  }

  @Override
  public void setHighlight(String term, boolean caseSensitive) {
    if (isDisposed()) {
      return;
    }
    remoteObject.set("highlight", Const.NVL(term, ""));
    remoteObject.set("highlightCaseSensitive", caseSensitive);
  }

  // TextComposite

  /**
   * The lines joined with the platform separator, as the desktop widget holds them: the log
   * delegates split on {@code Const.CR} ("show error lines"), which is CRLF on a Windows server.
   */
  @Override
  public String getText() {
    if (lines.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder(charCount);
    for (String line : lines) {
      builder.append(line).append(Const.CR);
    }
    return builder.toString();
  }

  /** Replaces everything; the log browser uses this to empty the view before a rebuild. */
  @Override
  public void setText(String text) {
    clear();
    if (text == null || text.isBlank()) {
      return;
    }
    List<Line> newLines =
        text.lines().filter(line -> !line.isEmpty()).map(line -> new Line(line, false)).toList();
    appendLines(newLines);
  }

  @Override
  public void insert(String string) {
    if (string != null && !string.isEmpty()) {
      appendLines(
          string
              .lines()
              .filter(line -> !line.isEmpty())
              .map(line -> new Line(line, false))
              .toList());
    }
  }

  @Override
  public int getCharCount() {
    return charCount;
  }

  @Override
  public String getSelectionText() {
    return selectionText;
  }

  @Override
  public int getSelectionCount() {
    return selectionText.length();
  }

  @Override
  public void setSelection(int start) {
    setSelection(start, start);
  }

  @Override
  public void setSelection(int start, int end) {
    if (isDisposed()) {
      return;
    }
    JsonObject parameters = new JsonObject();
    parameters.add("start", Math.max(0, Math.min(start, end)));
    parameters.add("end", Math.max(0, Math.max(start, end)));
    remoteObject.call("select", parameters);
  }

  @Override
  public int getCaretPosition() {
    return charCount;
  }

  @Override
  public void setCaretPosition(int position) {
    setSelection(position);
  }

  @Override
  public void selectAll() {
    setSelection(0, charCount);
  }

  @Override
  public void copy() {
    String text = selectionText.isEmpty() ? getText() : selectionText;
    if (!text.isEmpty()) {
      GuiResource.getInstance().toClipboard(text);
    }
  }

  @Override
  public void cut() {
    // read-only
  }

  @Override
  public void paste() {
    // read-only
  }

  @Override
  public boolean isEditable() {
    return editable;
  }

  @Override
  public void setEditable(boolean editable) {
    this.editable = editable;
  }

  @Override
  public void addModifyListener(ModifyListener listener) {
    // nothing is typed here
  }

  @Override
  public void addLineStyleListener() {
    // the console colours error lines itself
  }

  @Override
  public void addLineStyleListener(List<String> keywords) {
    // the console colours error lines itself
  }

  @Override
  public void addMenuDetectListener(MenuDetectListener listener) {
    host.addListener(SWT.MenuDetect, event -> listener.menuDetected(new MenuDetectEvent(event)));
  }

  @Override
  public void setMenu(Menu menu) {
    host.setMenu(menu);
  }

  @Override
  public void addListener(int eventType, org.eclipse.swt.widgets.Listener listener) {
    host.addListener(eventType, listener);
  }

  @Override
  public void addMouseListener(org.eclipse.swt.events.MouseListener listener) {
    host.addMouseListener(listener);
  }

  @Override
  public boolean setFocus() {
    return host.setFocus();
  }

  /**
   * RAP does not put a composite's font on its element, so the console is told explicitly; this is
   * what makes the log toolbar's zoom buttons work.
   */
  @Override
  public void setFont(Font font) {
    super.setFont(font);
    host.setFont(font);
    if (font != null && !font.isDisposed() && !isDisposed()) {
      remoteObject.set("font", toCss(font.getFontData()[0]));
    }
  }

  /** A CSS font shorthand for a font description; RAP sizes are pixels. */
  static String toCss(FontData fontData) {
    StringBuilder css = new StringBuilder();
    if ((fontData.getStyle() & SWT.ITALIC) != 0) {
      css.append("italic ");
    }
    if ((fontData.getStyle() & SWT.BOLD) != 0) {
      css.append("bold ");
    }
    css.append(Math.max(1, fontData.getHeight())).append("px ");
    String family = Const.NVL(fontData.getName(), "").replace("\"", "");
    if (!family.isEmpty()) {
      css.append('"').append(family).append("\", ");
    }
    css.append("monospace");
    return css.toString();
  }

  @Override
  public void setBackground(Color color) {
    super.setBackground(color);
    host.setBackground(color);
  }

  /** Like the font, the text colour has to be told to the console (dark mode). */
  @Override
  public void setForeground(Color color) {
    super.setForeground(color);
    host.setForeground(color);
    if (color != null && !color.isDisposed() && !isDisposed()) {
      remoteObject.set("color", toCss(color));
    }
  }

  static String toCss(Color color) {
    if (color == null || color.isDisposed()) {
      return "";
    }
    return "rgb(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ")";
  }
}
