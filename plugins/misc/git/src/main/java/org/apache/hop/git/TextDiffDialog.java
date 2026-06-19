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
 *
 */
package org.apache.hop.git;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.git.diff.Diff;
import org.apache.hop.git.diff.TextDiff;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Path;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Sash;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Slider;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Widget;

@GuiPlugin(name = "i18n::TextDiffDialog.Name", description = "i18n::TextDiffDialog.Description")
public class TextDiffDialog extends Dialog {
  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "TextDiff-Toolbar";
  public static final String TOOLBAR_ITEM_NEXT_DIFF = "TextDiff-Toolbar-10100-NextDiff";
  public static final String TOOLBAR_ITEM_PREVIOUS_DIFF = "TextDiff-Toolbar-10200-PreviousDiff";
  public static final String TOOLBAR_ITEM_IGNORE_WHITESPACES =
      "TextDiff-Toolbar-10300-IgnoreWhiteSpaces";
  public static final String TOOLBAR_ITEM_IGNORE_EMPTY_LINES =
      "TextDiff-Toolbar-10310-IgnoreEmptyLines";

  public static final String OPTION_DIFF_IGNORE_WHITESPACES = "Git.Diff.IgnoreWhiteSpaces";

  private static final Class<?> PKG = GitGuiPlugin.class;
  private Shell shell;
  private Label wDiffLabel;
  private StyledText wLeftEditor;
  private StyledText wRightEditor;
  private Sash wSash;
  private Slider wSlider;

  @Getter @Setter private String title;

  @Getter @Setter private String leftTitle;
  @Getter @Setter private String rightTitle;

  @Getter @Setter private String leftContent;
  @Getter @Setter private String rightContent;

  @Getter @Setter private boolean leftEditable;
  @Getter @Setter private boolean rightEditable;

  private int leftTop;
  private int rightTop;

  private boolean modified;
  private boolean syncingVertical;
  private boolean syncingHorizontal;
  private boolean refreshing;
  private boolean ignoreWhiteSpaces;
  private List<Diff> diffs;
  private int currentDiffIndex = -1;
  private GuiToolbarWidgets toolBarWidgets;

  public TextDiffDialog(Shell parent, String leftContent, String rightContent) {
    super(parent, SWT.NONE);
    this.leftContent = leftContent;
    this.rightContent = rightContent;
    this.leftEditable = false;
    this.rightEditable = false;
    this.leftTop = 0;
    this.rightTop = 0;
    this.title = BaseMessages.getString(PKG, "TextDiffDialog.Name");
  }

  public boolean open() {
    shell =
        new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    shell.setText(title);
    shell.setImage(GitResource.getInstance().getDiffImage());
    shell.setMinimumSize(800, 400);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    PropsUi.setLook(shell);

    int middle = PropsUi.getInstance().getMiddlePct();

    // Create toolbar
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(shell, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);

    Control wToolBar = toolBarContainer.getControl();
    wToolBar.setLayoutData(FormDataBuilder.builder().left().right(middle, 0).top().result());
    wToolBar.pack();
    PropsUi.setLook(wToolBar, Props.WIDGET_STYLE_TOOLBAR);

    wDiffLabel = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wDiffLabel);
    wDiffLabel.setText(BaseMessages.getString(PKG, "TextDiffDialog.Toolbar.Diff.Label", 0));
    wDiffLabel.setForeground(GitResource.getInstance().getTextDifferenceColor());
    wDiffLabel.setLayoutData(FormDataBuilder.builder().left(middle, 0).right().top().result());

    CLabel leftLabel = new CLabel(shell, SWT.NONE);
    leftLabel.setText(Const.NVL(leftTitle, ""));
    leftLabel.setLayoutData(FormDataBuilder.builder().left().top(wToolBar).result());
    leftLabel.setImage(
        leftEditable
            ? GitResource.getInstance().getLockOpenImage()
            : GitResource.getInstance().getLockImage());
    PropsUi.setLook(leftLabel);

    CLabel rightLabel = new CLabel(shell, SWT.NONE);
    rightLabel.setText(Const.NVL(rightTitle, ""));
    rightLabel.setLayoutData(FormDataBuilder.builder().right().top(wToolBar).result());
    rightLabel.setImage(
        rightEditable
            ? GitResource.getInstance().getLockOpenImage()
            : GitResource.getInstance().getLockImage());
    PropsUi.setLook(rightLabel);

    List<Button> buttons = new ArrayList<>();

    Button ok = new Button(shell, SWT.PUSH);
    ok.setText(BaseMessages.getString("System.Button.OK"));
    ok.addListener(SWT.Selection, e -> ok());
    PropsUi.setLook(ok);
    buttons.add(ok);

    if (this.leftEditable || this.rightEditable) {
      Button cancel = new Button(shell, SWT.PUSH);
      cancel.setText(BaseMessages.getString("System.Button.Cancel"));
      cancel.addListener(SWT.Selection, e -> cancel());
      PropsUi.setLook(cancel);
      buttons.add(cancel);
    }

    BaseTransformDialog.positionBottomButtons(
        shell, buttons.toArray(new Button[0]), PropsUi.getMargin(), null);

    Composite composite = new Composite(shell, SWT.BORDER);
    FormLayout diffLayout = new FormLayout();
    diffLayout.marginWidth = 0;
    diffLayout.marginHeight = 0;
    composite.setLayout(diffLayout);
    composite.setLayoutData(
        FormDataBuilder.builder()
            .left()
            .right()
            .top(leftLabel, PropsUi.getMargin())
            .bottom(ok, -PropsUi.getMargin())
            .result());
    PropsUi.setLook(composite);

    wSlider = new Slider(composite, SWT.VERTICAL);
    wSlider.setLayoutData(FormDataBuilder.builder().top().bottom().right().result());
    PropsUi.setLook(wSlider);

    SashForm wSashForm = new SashForm(composite, SWT.HORIZONTAL);
    wSashForm.setLayoutData(
        FormDataBuilder.builder().left().right(wSlider, 0).top().bottom().result());

    wLeftEditor = createEditor(wSashForm, leftContent, leftEditable);
    wRightEditor = createEditor(wSashForm, rightContent, rightEditable);

    // If one content is null, hidde the side editor and the differences count
    boolean oneSideOnly = false;
    if (leftContent == null) {
      oneSideOnly = true;
      wSashForm.setWeights(0, 100);
    }
    if (rightContent == null) {
      oneSideOnly = true;
      wSashForm.setWeights(100, 0);
    }
    wSashForm.setSashWidth(oneSideOnly ? 0 : 100);
    wDiffLabel.setVisible(!oneSideOnly);

    BaseTransformDialog.setSize(shell);

    // Add paint listener to Sash separator
    for (Control child : wSashForm.getChildren()) {
      if (child instanceof Sash sash) {
        this.wSash = sash;
        sash.addListener(SWT.Paint, this::paintSash);
        break;
      }
    }

    // Repaint sash on shell resize
    shell.addListener(
        SWT.Resize,
        e -> {
          if (!wSash.isDisposed()) {
            wSash.redraw();
          }
        });

    installSliderAndMouseWheel();

    updateSlider();

    restoreDialogSettings();

    refresh();

    // Show the dialog now
    shell.open();

    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }

    return modified;
  }

  private void storeDialogSettings() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));

    try {
      HopConfig.setGuiProperty(OPTION_DIFF_IGNORE_WHITESPACES, ignoreWhiteSpaces ? "Y" : "N");
      HopConfig.getInstance().saveToFile();
    } catch (HopException e) {
      // Ignore, already logged by saveToFile function
    }
  }

  private void restoreDialogSettings() {
    ignoreWhiteSpaces = Const.toBoolean(HopConfig.getGuiProperty(OPTION_DIFF_IGNORE_WHITESPACES));

    Button ignoreWhiteSpacesCheckBox = getIgnoreWhiteSpacesCheckBox();
    if (ignoreWhiteSpacesCheckBox != null) {
      ignoreWhiteSpacesCheckBox.setSelection(ignoreWhiteSpaces);
    }
  }

  private Button getIgnoreWhiteSpacesCheckBox() {
    if (toolBarWidgets == null) {
      return null;
    }
    ToolItem checkboxItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_IGNORE_WHITESPACES);
    if (checkboxItem == null) {
      return null;
    }
    return (Button) checkboxItem.getControl();
  }

  private void mouseVerticalWheel(Event event) {
    if (syncingVertical || isDisposed(wLeftEditor) || isDisposed(wRightEditor)) {
      return;
    }
    syncingVertical = true;
    try {
      int lineHeight = wLeftEditor.getLineHeight();
      int delta = -event.count * lineHeight;
      int newTopPixel = Math.max(0, wLeftEditor.getTopPixel() + delta);

      this.leftTop = newTopPixel;
      this.rightTop = newTopPixel;

      wLeftEditor.setTopPixel(newTopPixel);
      wRightEditor.setTopPixel(newTopPixel);
      wSlider.setSelection(newTopPixel);
      wSash.redraw();
    } finally {
      syncingVertical = false;
    }
  }

  private void paintEditor(Event event) {
    if (diffs == null) {
      return;
    }
    GC gc = event.gc;
    Rectangle area = event.getBounds();
    // Draw line to left editor where rows are inserted
    if (event.widget == wLeftEditor) {

      //  Rectangle area = wLeftEditor.getBounds();
      gc.setForeground(GitResource.getInstance().getTextInsertBackgroundColor());
      gc.setLineWidth(2);
      for (Diff diff : diffs) {
        if (diff.type() == Diff.Type.INSERT) {
          int y = getVerticalLocationAtLine(wLeftEditor, diff.leftLineStart());
          gc.drawLine(area.x, y, area.x + area.width, y);
        }
      }
    }

    // Draw line to right editor where rows are deleted
    else if (event.widget == wRightEditor) {
      gc.setForeground(GitResource.getInstance().getTextDeleteBackgroundColor());
      gc.setLineWidth(2);
      for (Diff diff : diffs) {
        if (diff.type() == Diff.Type.DELETE) {
          int y = getVerticalLocationAtLine(wRightEditor, diff.rightLineStart());
          gc.drawLine(area.x, y, area.x + area.width, y);
        }
      }
    }
  }

  private void paintSash(Event event) {
    if (diffs == null) {
      return;
    }
    GC gc = event.gc;
    Rectangle area = event.getBounds();
    int lineHeight = wLeftEditor.getLineHeight();

    for (Diff diff : diffs) {
      Color color =
          switch (diff.type()) {
            case CHANGE -> GitResource.getInstance().getTextChangeBackgroundColor();
            case INSERT -> GitResource.getInstance().getTextInsertBackgroundColor();
            case DELETE -> GitResource.getInstance().getTextDeleteBackgroundColor();
          };
      gc.setBackground(color);

      int ly1 = getVerticalLocationAtLine(wLeftEditor, diff.leftLineStart());
      int ry1 = getVerticalLocationAtLine(wRightEditor, diff.rightLineStart());
      int ly2 = ly1 + diff.leftLineCount() * lineHeight;
      int ry2 = ry1 + diff.rightLineCount() * lineHeight;

      Path path = new Path(event.display);
      path.moveTo(0, ly1);
      path.cubicTo(40, ly1, area.width - 40, ry1, area.width, ry1);
      path.lineTo(area.width, ry2);
      path.cubicTo(area.width - 40, ry2, 40, ly2, 0, ly2);
      path.close();
      gc.fillPath(path);
      path.dispose();
    }

    gc.setForeground(Display.getDefault().getSystemColor(SWT.COLOR_WIDGET_BORDER));
    gc.drawLine(0, 0, 0, area.height);
    gc.drawLine(area.width - 1, 0, area.width - 1, area.height);
  }

  private int getVerticalLocationAtLine(StyledText editor, int line) {
    if (line < editor.getLineCount() - 1) {
      int offsetAtLine = editor.getOffsetAtLine(line);
      return editor.getLocationAtOffset(offsetAtLine).y;
    }

    // Special case for the last line
    int offsetAtLine = editor.getOffsetAtLine(line - 1);
    return editor.getLocationAtOffset(offsetAtLine).y + editor.getBaseline() + 2;
  }

  private StyledText createEditor(Composite parent, String content, boolean editable) {
    StyledText editor = new StyledText(parent, SWT.MULTI | SWT.H_SCROLL);
    editor.setEditable(editable);
    editor.setWordWrap(false);
    editor.setTabs(4);
    editor.setAlwaysShowScrollBars(false);
    editor.setFont(GuiResource.getInstance().getFontFixed());
    if (content != null) {
      editor.setText(content);
    }
    editor.addListener(
        SWT.Modify,
        event -> {
          refresh();
          updateSlider();
        });
    editor.addListener(SWT.Paint, this::paintEditor);
    editor.addListener(SWT.Resize, event -> updateSlider());
    editor.addListener(SWT.KeyUp, event -> detectScrolling());

    PropsUi.setLook(editor);

    return editor;
  }

  // Detect any changes to the viewport
  private void detectScrolling() {
    if (isDisposed(wLeftEditor) || isDisposed(wRightEditor) || isDisposed(wSash)) {
      return;
    }

    boolean change = false;

    // We detect left scrolling
    if (leftTop != wLeftEditor.getTopPixel()) {
      change = true;
      this.leftTop = wLeftEditor.getTopPixel();
    }

    // We detect right scrolling
    if (rightTop != wRightEditor.getTopPixel()) {
      change = true;
      rightTop = wRightEditor.getTopPixel();
    }

    if (change) {
      updateSlider();
      wSash.redraw();
    }
  }

  private void installSliderAndMouseWheel() {
    ScrollBar leftHorizontal = wLeftEditor.getHorizontalBar();
    ScrollBar rightHorizontal = wRightEditor.getHorizontalBar();

    wSlider.addListener(
        SWT.Selection,
        e -> {
          if (syncingVertical || isDisposed(wLeftEditor) || isDisposed(wRightEditor)) {
            return;
          }
          syncingVertical = true;
          try {
            int selection = wSlider.getSelection();
            wLeftEditor.setTopPixel(selection);
            wRightEditor.setTopPixel(selection);
            wSash.redraw();
          } finally {
            syncingVertical = false;
          }
        });

    // Listen to mouse wheel events on both editors and the sash
    wLeftEditor.addListener(SWT.MouseVerticalWheel, this::mouseVerticalWheel);
    wRightEditor.addListener(SWT.MouseVerticalWheel, this::mouseVerticalWheel);
    wSash.addListener(SWT.MouseVerticalWheel, this::mouseVerticalWheel);

    // Synchronize horizontal scrolling
    if (leftHorizontal != null && rightHorizontal != null) {
      leftHorizontal.addListener(
          SWT.Selection,
          e -> {
            if (syncingHorizontal || isDisposed(wLeftEditor) || isDisposed(wRightEditor)) {
              return;
            }
            syncingHorizontal = true;
            try {
              wRightEditor.setHorizontalPixel(wLeftEditor.getHorizontalPixel());
            } finally {
              syncingHorizontal = false;
            }
          });

      rightHorizontal.addListener(
          SWT.Selection,
          e -> {
            if (syncingHorizontal || isDisposed(wLeftEditor) || isDisposed(wRightEditor)) {
              return;
            }
            syncingHorizontal = true;
            try {
              wLeftEditor.setHorizontalPixel(wRightEditor.getHorizontalPixel());
            } finally {
              syncingHorizontal = false;
            }
          });
    }
  }

  private void updateSlider() {
    if (isDisposed(wLeftEditor) || isDisposed(wRightEditor) || isDisposed(wSlider)) {
      return;
    }

    int leftLineCount = wLeftEditor.getLineCount();
    int rightLineCount = wRightEditor.getLineCount();
    int maxLineCount = Math.max(leftLineCount, rightLineCount);
    int lineHeight = wLeftEditor.getLineHeight();
    int clientHeight = wLeftEditor.getClientArea().height;
    int maxPixel = maxLineCount * lineHeight;

    wSlider.setMinimum(0);
    wSlider.setMaximum(maxPixel);
    wSlider.setThumb(clientHeight);
    wSlider.setPageIncrement(clientHeight);
    wSlider.setIncrement(lineHeight);
    wSlider.setEnabled(maxPixel > clientHeight);
  }

  private void refresh() {
    if (refreshing || isDisposed(wLeftEditor) || isDisposed(wRightEditor) || isDisposed(wSash)) {
      return;
    }

    refreshing = true;
    try {

      String left = wLeftEditor.getText();
      String right = wRightEditor.getText();

      wLeftEditor.setRedraw(false);
      wRightEditor.setRedraw(false);

      // Reset style ranges and line background color
      wLeftEditor.setStyleRanges(new StyleRange[0]);
      wLeftEditor.setLineBackground(0, wLeftEditor.getLineCount(), null);
      wRightEditor.setStyleRanges(new StyleRange[0]);
      wRightEditor.setLineBackground(0, wRightEditor.getLineCount(), null);

      TextDiff textDiff = new TextDiff();
      textDiff.setIgnoreWhiteSpaces(ignoreWhiteSpaces);
      diffs = textDiff.diff(left, right);
      diffs.forEach(
          diff -> {
            switch (diff.type()) {
              case INSERT -> {
                wRightEditor.setLineBackground(
                    diff.rightLineStart(),
                    diff.rightLineCount(),
                    GitResource.getInstance().getTextInsertBackgroundColor());
                wRightEditor.setStyleRange(
                    new StyleRange(
                        diff.rightStart(),
                        diff.rightCount(),
                        GitResource.getInstance().getTextInsertForegroundColor(),
                        null));
              }

              case DELETE -> {
                wLeftEditor.setLineBackground(
                    diff.leftLineStart(),
                    diff.leftLineCount(),
                    GitResource.getInstance().getTextDeleteBackgroundColor());

                wLeftEditor.setStyleRange(
                    new StyleRange(
                        diff.leftStart(),
                        diff.leftCount(),
                        GitResource.getInstance().getTextDeleteForegroundColor(),
                        null));
              }

              case CHANGE -> {
                wLeftEditor.setLineBackground(
                    diff.leftLineStart(),
                    diff.leftLineCount(),
                    GitResource.getInstance().getTextChangeBackgroundColor());

                wRightEditor.setLineBackground(
                    diff.rightLineStart(),
                    diff.rightLineCount(),
                    GitResource.getInstance().getTextChangeBackgroundColor());
              }
            }
          });

      wLeftEditor.setRedraw(true);
      wRightEditor.setRedraw(true);
      wSash.redraw();

      // Update the label to display difference count
      wDiffLabel.setText(
          BaseMessages.getString(PKG, "TextDiffDialog.Toolbar.Diff.Label", diffs.size()));
    } catch (Exception e) {
      LogChannel.UI.logError("Error refresh text diff dialog", e);
    } finally {
      refreshing = false;
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEXT_DIFF,
      toolTip = "i18n::TextDiffDialog.Toolbar.GoToNextDiff.Tooltip",
      image = "ui/images/arrow-down.svg")
  public void goToNextDiff() {
    if (diffs.isEmpty() || isDisposed(wLeftEditor) || isDisposed(wRightEditor)) {
      return;
    }
    currentDiffIndex = (currentDiffIndex + 1) % diffs.size();
    select(diffs.get(currentDiffIndex));
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PREVIOUS_DIFF,
      toolTip = "i18n::TextDiffDialog.Toolbar.GoToPreviousDiff.Tooltip",
      image = "ui/images/arrow-up.svg")
  public void goToPreviousDiff() {
    if (diffs.isEmpty() || isDisposed(wLeftEditor) || isDisposed(wRightEditor)) {
      return;
    }
    currentDiffIndex = currentDiffIndex <= 0 ? diffs.size() - 1 : currentDiffIndex - 1;
    select(diffs.get(currentDiffIndex));
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_IGNORE_WHITESPACES,
      label = "i18n::TextDiffDialog.Toolbar.IgnoreWhiteSpaces.Label",
      toolTip = "i18n::TextDiffDialog.Toolbar.IgnoreWhiteSpaces.Tooltip",
      type = GuiToolbarElementType.CHECKBOX)
  public void enableIgnoreWhiteSpaces() {

    Button button = getIgnoreWhiteSpacesCheckBox();
    if (button == null) {
      ignoreWhiteSpaces = false;
    } else {
      ignoreWhiteSpaces = button.getSelection();
    }

    refresh();
  }

  /**
   * Selects and highlights the specified diff in the left and right editors.
   *
   * @param diff the diff object specifying the line and character differences to be highlighted.
   */
  private void select(Diff diff) {
    if (diff == null) {
      return;
    }

    int leftOffsetAtLine = wLeftEditor.getOffsetAtLine(diff.leftLineStart());
    Point leftLocation = wLeftEditor.getLocationAtOffset(leftOffsetAtLine);
    int rightOffsetAtLine = wRightEditor.getOffsetAtLine(diff.rightLineStart());
    Point rightLocation = wRightEditor.getLocationAtOffset(rightOffsetAtLine);

    wLeftEditor.setTopPixel(Math.max(0, leftLocation.y - 20));
    wRightEditor.setTopPixel(Math.max(0, rightLocation.y - 20));
    wSash.redraw();

    if (diff.leftStart() >= 0) {
      wLeftEditor.setSelection(diff.leftStart(), diff.leftStart() + diff.leftCount());
    }
    if (diff.rightStart() >= 0) {
      wRightEditor.setSelection(diff.rightStart(), diff.rightStart() + diff.rightCount());
    }
  }

  private void ok() {
    if (!isDisposed(wLeftEditor) && !isDisposed(wRightEditor)) {
      leftContent = wLeftEditor.getText();
      rightContent = wRightEditor.getText();
      modified = true;
    }
    dispose();
  }

  private void cancel() {
    modified = false;
    dispose();
  }

  private void dispose() {
    if (!isDisposed(shell)) {

      // Store the toolbar settings
      storeDialogSettings();

      shell.dispose();
    }
  }

  private boolean isDisposed(Widget widget) {
    return widget == null || widget.isDisposed();
  }
}
