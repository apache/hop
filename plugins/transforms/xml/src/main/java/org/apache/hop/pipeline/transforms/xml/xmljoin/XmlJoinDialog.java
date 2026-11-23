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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.xml.xmljoin.TargetXPathBuilder.KeyCandidate;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.w3c.dom.Document;

/**
 * The XML join options come from the annotated fields of {@link XmlJoinMeta}. This dialog adds a
 * line saying what the transform does, fills the combos (the input transforms, the encodings, the
 * fields of the selected target and source transform) and enables the key field when joining by
 * key. Under the XPath statement a line checks it against the target document. Beside the options
 * the first target document can be shown as a tree, to click the node to add the elements to.
 */
public class XmlJoinDialog extends BaseTransformDialog {
  private static final Class<?> PKG = XmlJoinMeta.class;

  /** The width the check line wraps at when the options are measured, before they are laid out. */
  private static final int CHECK_WRAP_WIDTH = 300;

  private final XmlJoinMeta input;

  private GuiCompositeWidgets widgets;

  /** True while the dialog fills its widgets, so that does not mark the transform changed. */
  private boolean loading;

  /** The transforms with a hop into this one. */
  private String[] inputTransforms = new String[0];

  /**
   * The line under the XPath statement: what the statement finds in the target document, or what is
   * still missing to check it. An icon and the text.
   */
  private Composite wXPathCheck;

  private Label wXPathCheckIcon;
  private Label wXPathCheckText;

  /** The colour of the check line when it reports no success or error. */
  private Color checkForeground;

  /** The first target document beside the options. */
  private TargetDocumentPane wDocument;

  /** The statement the document pane wrote last, to write it again when the join mode changes. */
  private String writtenXPath;

  /** True while a node picked in the document fills in the options. */
  private boolean picking;

  /**
   * The output fields of the transforms, by name, looked up once: an upstream transform may need a
   * database query to describe them.
   */
  private final Map<String, String[]> fieldsByTransform = new HashMap<>();

  /** The encodings, listed the first time the encoding combo gets focus: that takes a while. */
  private String[] encodings;

  public XmlJoinDialog(
      Shell parent, IVariables variables, XmlJoinMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "XmlJoin.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;

    Label wIntro = new Label(shell, SWT.WRAP | SWT.LEFT);
    wIntro.setText(BaseMessages.getString(PKG, "XmlJoin.Intro"));
    PropsUi.setLook(wIntro);
    FormData fdIntro = new FormData();
    fdIntro.left = new FormAttachment(0, 0);
    fdIntro.right = new FormAttachment(100, 0);
    fdIntro.top = new FormAttachment(wSpacer, margin);
    wIntro.setLayoutData(fdIntro);

    // The options on the left, the target document on the right
    SashForm sash = new SashForm(shell, SWT.HORIZONTAL);
    PropsUi.setLook(sash);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.top = new FormAttachment(wIntro, margin);
    fdSash.bottom = new FormAttachment(wOk, -2 * margin);
    sash.setLayoutData(fdSash);
    Composite options = new Composite(sash, SWT.NONE);
    PropsUi.setLook(options);
    options.setLayout(new FormLayout());
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            options, variables, null, null, XmlJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, input);
    wDocument = new TargetDocumentPane(sash, this::loadDocument);
    wDocument.setPickListener(this::picked);
    wDocument.setKeyNames(this::keyNames);
    sash.setWeights(new int[] {60, 40});
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (loading) {
              return;
            }
            input.setChanged();
            switch (widgetId) {
              case XmlJoinMeta.WIDGET_COMPLEX_JOIN -> joinModeChanged();
              case XmlJoinMeta.WIDGET_TARGET_XPATH -> wDocument.show(resolvedXPath());
              case XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD -> keyFieldChanged();
              case XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM -> {
                fillOtherInput(XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM);
                wDocument.setDocument(null, null);
              }
              case XmlJoinMeta.WIDGET_TARGET_XML_FIELD -> wDocument.setDocument(null, null);
              case XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM ->
                  fillOtherInput(XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM);
              default -> {
                // Nothing else depends on another option
              }
            }
            updateXPathCheck();
          }
        });

    inputTransforms =
        pipelineMeta
            .findPreviousTransforms(pipelineMeta.findTransform(transformName), true)
            .stream()
            .map(TransformMeta::getName)
            .toArray(String[]::new);
    setComboItems(XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM, inputTransforms);
    setComboItems(XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM, inputTransforms);
    listItemsOnFocus(XmlJoinMeta.WIDGET_ENCODING, this::encodings);
    listItemsOnFocus(
        XmlJoinMeta.WIDGET_TARGET_XML_FIELD,
        () -> fieldsOf(XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM));
    listItemsOnFocus(
        XmlJoinMeta.WIDGET_SOURCE_XML_FIELD,
        () -> fieldsOf(XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM));
    listItemsOnFocus(
        XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD,
        () -> fieldsOf(XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM));
    addXPathCheck();
    enableKeyField();
    wDocument.setByKey(byKey());
    wDocument.show(resolvedXPath());
    updateXPathCheck();

    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /**
   * Puts the check line under the XPath statement. Annotations can't: the line follows the other
   * options.
   */
  private void addXPathCheck() {
    Control xpath = widgets.getWidgetsMap().get(XmlJoinMeta.WIDGET_TARGET_XPATH);
    if (xpath == null) {
      return;
    }
    Composite content = xpath.getParent();

    wXPathCheck = new Composite(content, SWT.NONE);
    PropsUi.setLook(wXPathCheck);
    GridLayout layout = new GridLayout(2, false);
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    layout.horizontalSpacing = margin;
    wXPathCheck.setLayout(layout);
    wXPathCheckIcon = new Label(wXPathCheck, SWT.NONE);
    PropsUi.setLook(wXPathCheckIcon);
    wXPathCheckIcon.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, false, false));
    wXPathCheckText = new Label(wXPathCheck, SWT.WRAP | SWT.LEFT);
    PropsUi.setLook(wXPathCheckText);
    GridData gdText = new GridData(SWT.FILL, SWT.TOP, true, false);
    // Without a width the line would be measured on one line and widen the options
    gdText.widthHint = (int) (CHECK_WRAP_WIDTH * PropsUi.getInstance().getZoomFactor());
    wXPathCheckText.setLayoutData(gdText);
    checkForeground = wXPathCheckText.getForeground();
    FormData fdCheck = new FormData();
    fdCheck.left = new FormAttachment(xpath, 0, SWT.LEFT);
    fdCheck.right = new FormAttachment(100, 0);
    fdCheck.top = new FormAttachment(xpath, margin);
    wXPathCheck.setLayoutData(fdCheck);

    // Whatever was attached under the statement row, the statement or its label, goes under the
    // line now. What is attached beside them, centred or level with their top, stays.
    Control xpathLabel = widgets.getLabelsMap().get(XmlJoinMeta.WIDGET_TARGET_XPATH);
    Set<Control> xpathRow = xpathLabel == null ? Set.of(xpath) : Set.of(xpath, xpathLabel);
    for (Control control : content.getChildren()) {
      if (control != wXPathCheck
          && !xpathRow.contains(control)
          && control.getLayoutData() instanceof FormData fd
          && fd.top != null
          && fd.top.control != null
          && xpathRow.contains(fd.top.control)
          && (fd.top.alignment == SWT.DEFAULT || fd.top.alignment == SWT.BOTTOM)) {
        fd.top = new FormAttachment(wXPathCheck, fd.top.offset);
      }
    }
    GuiCompositeWidgets.updateScrolledMinSize(content);
  }

  /**
   * Says what the statement finds in the target document: the node the elements go under, no node,
   * or why it is not valid XPath. Without a document, or when something else is missing, it says
   * that instead.
   */
  private void updateXPathCheck() {
    if (wXPathCheck == null || wXPathCheck.isDisposed()) {
      return;
    }
    GuiResource resource = GuiResource.getInstance();
    String xpath = Const.NVL(resolvedXPath(), "").trim();
    String keyField = variables.resolve(textOf(XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD));
    boolean byKey = byKey();
    boolean hasPlaceholder = xpath.contains(TargetXPathBuilder.KEY_PLACEHOLDER);
    TargetDocumentPane.Check check = wDocument.check();

    Image icon = resource.getImageInfo();
    Color color = checkForeground;
    String text;
    String toolTip = null;
    if (xpath.isEmpty()) {
      text = message("XmlJoin.XPathHint.Empty");
    } else if (byKey && TargetXPathBuilder.KEY_PLACEHOLDER.equals(xpath)) {
      text = message("XmlJoin.XPathHint.FromKeyField");
    } else if (check.outcome() == TargetDocumentPane.Outcome.INVALID) {
      icon = resource.getImageError();
      color = resource.getColorRed();
      text = message("XmlJoin.XPathHint.Invalid", check.error());
    } else if (check.outcome() == TargetDocumentPane.Outcome.NOT_FOUND) {
      icon = resource.getImageError();
      color = resource.getColorRed();
      text =
          message(
              byKey && hasPlaceholder
                  ? "XmlJoin.XPathHint.NotFound.ByKey"
                  : "XmlJoin.XPathHint.NotFound");
    } else if (check.outcome() == TargetDocumentPane.Outcome.KEY_FINDS_NO_NODE) {
      icon = resource.getImageError();
      color = resource.getColorRed();
      text = message("XmlJoin.XPathHint.KeyFindsNoNode", check.keyValue());
    } else if (check.outcome() == TargetDocumentPane.Outcome.KEY_NOT_UNIQUE) {
      icon = resource.getImageWarning();
      text = message("XmlJoin.XPathHint.KeyNotUnique", check.keyValue(), location(check));
      toolTip =
          message(
              "XmlJoin.XPathHint.Example",
              check.keyValue(),
              TargetXPathBuilder.forKeyValue(xpath, check.keyValue()));
    } else if (check.outcome() == TargetDocumentPane.Outcome.NOT_FOLLOWED) {
      text = message("XmlJoin.XPathHint.NotFollowed");
    } else if (byKey && !hasPlaceholder) {
      icon = resource.getImageWarning();
      text = message("XmlJoin.XPathHint.NoPlaceholder");
    } else if (byKey && Utils.isEmpty(keyField)) {
      icon = resource.getImageWarning();
      text = message("XmlJoin.XPathHint.ByKeyNoField");
    } else if (check.outcome() == TargetDocumentPane.Outcome.FOUND
        || check.outcome() == TargetDocumentPane.Outcome.FOUND_NOT_LISTED) {
      icon = resource.getImageSuccess();
      color = resource.getColorDarkGreen();
      text = found(check, byKey);
      if (byKey && check.keyValue() != null) {
        toolTip =
            message(
                "XmlJoin.XPathHint.Example",
                check.keyValue(),
                TargetXPathBuilder.forKeyValue(xpath, check.keyValue()));
      }
    } else if (byKey) {
      text = message("XmlJoin.XPathHint.NotChecked.ByKey", keyField);
    } else {
      text = message("XmlJoin.XPathHint.NotChecked");
    }

    int heightBefore = wXPathCheck.getSize().y;
    wXPathCheckIcon.setImage(icon);
    wXPathCheckText.setText(text);
    wXPathCheckText.setForeground(color);
    wXPathCheckText.setToolTipText(toolTip == null ? text : text + Const.CR + toolTip);
    int width = wXPathCheck.getSize().x;
    if (width > 0 && wXPathCheck.computeSize(width, SWT.DEFAULT, true).y <= heightBefore) {
      // The text fits in the line as it is. Laying out the options again only when the line has to
      // grow keeps typing smooth: in Hop Web every layout is a round trip. A line that could be
      // shorter keeps its height until the options are laid out again.
      wXPathCheck.layout(true, true);
    } else {
      relayoutOptions();
    }
  }

  /**
   * The check line got more lines: its box grows, the options under it move, and the height they
   * scroll over changes. The width stays: measured again, the options would be as wide as the
   * statement typed in them.
   */
  private void relayoutOptions() {
    // The line sits in a scrolled box, and the boxes in the scrolled options: from the inside out,
    // each one gets the height of what it holds now
    for (Control control = wXPathCheck;
        control != null && !(control instanceof Shell);
        control = control.getParent()) {
      if (control instanceof ScrolledComposite scrolled
          && scrolled.getContent() instanceof Composite content) {
        content.layout(true, true);
        int width = Math.max(scrolled.getClientArea().width, scrolled.getMinWidth());
        scrolled.setMinHeight(content.computeSize(width, SWT.DEFAULT, true).y);
      }
    }
  }

  /**
   * The node the elements go under, highlighted in the target document or past the listed nodes.
   */
  private static String found(TargetDocumentPane.Check check, boolean byKey) {
    String node = location(check);
    String where;
    if (!byKey) {
      where = message("XmlJoin.XPathHint.Found", node);
    } else {
      where =
          (check.keyValue() != null
                  ? message("XmlJoin.XPathHint.Found.ByKey", check.keyValue(), node)
                  : message("XmlJoin.XPathHint.Found.ByKeyNoValue", node))
              + " "
              + (check.keyNodes() == 1
                  ? message("XmlJoin.XPathHint.KeyNodes.One")
                  : message("XmlJoin.XPathHint.KeyNodes.Many", check.keyNodes()));
    }
    String shown =
        check.outcome() == TargetDocumentPane.Outcome.FOUND
            ? message("XmlJoin.XPathHint.Highlighted")
            : message("XmlJoin.XPathHint.NotListed", TargetDocumentPane.MAX_CHILDREN);
    return where + " " + shown;
  }

  /** The node of a check and its parent, or that it is the root. */
  private static String location(TargetDocumentPane.Check check) {
    return check.parent() == null
        ? message("XmlJoin.XPathHint.Node.Root", check.node())
        : message("XmlJoin.XPathHint.Node.InParent", check.node(), check.parent());
  }

  private static String message(String key, Object... arguments) {
    return BaseMessages.getString(PKG, key, arguments);
  }

  /** Shows the first document of the target transform beside the options. */
  private void loadDocument() {
    String targetTransform = textOf(XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM);
    String targetField = variables.resolve(textOf(XmlJoinMeta.WIDGET_TARGET_XML_FIELD));
    if (Utils.isEmpty(targetTransform) || Utils.isEmpty(targetField)) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "XmlJoin.Document.Title"));
      box.setMessage(BaseMessages.getString(PKG, "XmlJoin.Document.ChooseTarget"));
      box.open();
      return;
    }
    Document document;
    try {
      document = firstTargetDocument(targetTransform, targetField);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "XmlJoin.Document.Title"),
          BaseMessages.getString(PKG, "XmlJoin.Document.Error", targetTransform),
          e);
      return;
    }
    if (document != null) {
      wDocument.setDocument(document, targetTransform);
      updateXPathCheck();
    }
  }

  /**
   * A node was clicked in the document: its statement goes in the XPath statement. When the key
   * field is empty and a source field has the name of the chosen key, that field becomes the key
   * field.
   */
  private void picked(String xpath, KeyCandidate key) {
    picking = true;
    try {
      if (key != null && Utils.isEmpty(textOf(XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD))) {
        String localName = key.name().substring(key.name().indexOf(':') + 1);
        for (String field : fieldsOf(XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM)) {
          if (field.equalsIgnoreCase(localName)) {
            setText(XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD, field);
            break;
          }
        }
      }
      writtenXPath = xpath;
      setText(XmlJoinMeta.WIDGET_TARGET_XPATH, xpath);
    } finally {
      picking = false;
    }
    updateXPathCheck();
  }

  /**
   * Another key field: when the statement was written by a click in the document, the key of the
   * node named like the field is chosen, and the statement written for it. A statement typed by
   * hand stays.
   */
  private void keyFieldChanged() {
    if (!picking && writtenXPath != null && writtenXPath.equals(resolvedXPath())) {
      wDocument.keyNamesChanged();
    }
  }

  /** The names to choose a key by: the key field, or else the fields of the source transform. */
  private List<String> keyNames() {
    String keyField = variables.resolve(textOf(XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD));
    return Utils.isEmpty(keyField)
        ? List.of(fieldsOf(XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM))
        : List.of(keyField);
  }

  /**
   * The key field and the key in the document only mean something when joining by key. A statement
   * the document wrote is written again when that is switched; one typed by hand stays.
   */
  private void joinModeChanged() {
    enableKeyField();
    boolean written = writtenXPath != null && writtenXPath.equals(resolvedXPath());
    wDocument.setByKey(byKey());
    if (written) {
      wDocument.pickAgain();
    } else {
      wDocument.show(resolvedXPath());
    }
  }

  private String resolvedXPath() {
    return variables.resolve(textOf(XmlJoinMeta.WIDGET_TARGET_XPATH));
  }

  /**
   * The XML document in the first row of the target transform. Only that transform and the
   * transforms that lead to it are run, in preview. When some of them may write, they are named and
   * the user is asked first. Null when that was declined, or the preview was cancelled or sent no
   * rows; the preview log is shown in that last case.
   */
  Document firstTargetDocument(String targetTransform, String targetField) throws HopException {
    PipelineMeta upstream = UpstreamPipeline.of(pipelineMeta, variables, targetTransform);
    List<String> mayWrite = UpstreamPipeline.mayWrite(upstream);
    if (!mayWrite.isEmpty() && !runAnyway(mayWrite)) {
      return null;
    }
    PipelinePreviewProgressDialog progress =
        new PipelinePreviewProgressDialog(
            shell, variables, upstream, new String[] {targetTransform}, new int[] {1});
    try {
      progress.open();
    } finally {
      // In Hop Web the preview runs without a progress window, puts a busy cursor on this shell
      // and leaves it there
      shell.setCursor(null);
    }
    if (progress.isCancelled() || progress.getLoggingText() == null) {
      // Cancelled, or the preview could not start: it has already said why
      return null;
    }
    List<Object[]> rows = progress.getPreviewRows(targetTransform);
    IRowMeta rowMeta = progress.getPreviewRowsMeta(targetTransform);
    if (rows == null || rows.isEmpty() || rowMeta == null) {
      EnterTextDialog log =
          new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "XmlJoin.Document.Title"),
              BaseMessages.getString(PKG, "XmlJoin.Document.NoRows", targetTransform),
              Const.NVL(progress.getLoggingText(), ""),
              true);
      log.setReadOnly();
      log.open();
      return null;
    }
    return UpstreamPipeline.documentIn(rowMeta, rows.get(0), targetTransform, targetField);
  }

  /** Names the transforms that may write when Load runs them, and asks whether to run them. */
  private boolean runAnyway(List<String> transformNames) {
    StringBuilder names = new StringBuilder();
    for (String name : transformNames) {
      names.append(names.isEmpty() ? "" : Const.CR).append("    ").append(name);
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_WARNING);
    box.setText(BaseMessages.getString(PKG, "XmlJoin.Document.Title"));
    box.setMessage(BaseMessages.getString(PKG, "XmlJoin.Document.MayWrite", names.toString()));
    return box.open() == SWT.YES;
  }

  /**
   * Lists the items of a combo when it gets focus. Looking them up when the dialog opens would make
   * it wait: for whatever the upstream transforms need to describe their fields, a database query
   * for instance, or for the list of encodings.
   */
  private void listItemsOnFocus(String widgetId, Supplier<String[]> items) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    Control focusControl =
        control instanceof ComboVar comboVar ? comboVar.getCComboWidget() : control;
    if (focusControl != null) {
      focusControl.addListener(SWT.FocusIn, e -> setComboItems(widgetId, items.get()));
    }
  }

  private String[] encodings() {
    if (encodings == null) {
      encodings = whileBusy(ConstUi::getEncodings);
    }
    return encodings;
  }

  /** Shows a busy cursor while a slow lookup runs. */
  private String[] whileBusy(Supplier<String[]> lookup) {
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    try {
      return lookup.get();
    } finally {
      shell.setCursor(null);
    }
  }

  /** The output fields of the transform selected in the given combo, none if it is unknown. */
  private String[] fieldsOf(String transformWidgetId) {
    String name = textOf(transformWidgetId);
    if (Utils.isEmpty(name)) {
      return new String[0];
    }
    return fieldsByTransform.computeIfAbsent(name, n -> whileBusy(() -> lookUpFields(n)));
  }

  private String[] lookUpFields(String transformName) {
    try {
      IRowMeta fields = pipelineMeta.getTransformFields(variables, transformName);
      return fields == null ? new String[0] : fields.getFieldNames();
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "XmlJoin.Log.UnableToGetFields", transformName), e);
      return new String[0];
    }
  }

  /** Whether each element goes under the node of its key: a complex join. */
  private boolean byKey() {
    return widgets.getWidgetsMap().get(XmlJoinMeta.WIDGET_COMPLEX_JOIN) instanceof Button button
        && button.getSelection();
  }

  /** The key field only means something when joining by key. */
  private void enableKeyField() {
    Control keyField = widgets.getWidgetsMap().get(XmlJoinMeta.WIDGET_JOIN_COMPARE_FIELD);
    if (keyField != null) {
      keyField.setEnabled(byKey());
    }
  }

  /**
   * With exactly two transforms sending rows to the join, picking one of them as the target (or
   * source) leaves only the other for the empty source (or target) box.
   */
  private void fillOtherInput(String changedWidgetId) {
    String otherWidgetId =
        XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM.equals(changedWidgetId)
            ? XmlJoinMeta.WIDGET_SOURCE_XML_TRANSFORM
            : XmlJoinMeta.WIDGET_TARGET_XML_TRANSFORM;
    if (inputTransforms.length != 2 || !Utils.isEmpty(textOf(otherWidgetId))) {
      return;
    }
    String chosen = textOf(changedWidgetId);
    String other = null;
    if (inputTransforms[0].equals(chosen)) {
      other = inputTransforms[1];
    } else if (inputTransforms[1].equals(chosen)) {
      other = inputTransforms[0];
    }
    if (other != null) {
      setText(otherWidgetId, other);
    }
  }

  private String textOf(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar) {
      return comboVar.getText();
    } else if (control instanceof Combo combo) {
      return combo.getText();
    } else if (control instanceof TextVar textVar) {
      return textVar.getText();
    } else if (control instanceof Text text) {
      return text.getText();
    }
    return null;
  }

  private void setText(String widgetId, String value) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar) {
      comboVar.setText(value);
    } else if (control instanceof Combo combo) {
      combo.setText(value);
    } else if (control instanceof TextVar textVar) {
      textVar.setText(value);
    } else if (control instanceof Text text) {
      text.setText(value);
    }
  }

  /** Replaces the items of a combo and keeps what it shows. */
  private void setComboItems(String widgetId, String[] items) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control == null || control.isDisposed() || Arrays.equals(itemsOf(control), items)) {
      return;
    }
    boolean wasLoading = loading;
    loading = true;
    try {
      if (control instanceof ComboVar comboVar) {
        String text = comboVar.getText();
        comboVar.setItems(items);
        comboVar.setText(text);
      } else if (control instanceof Combo combo) {
        String text = combo.getText();
        combo.setItems(items);
        combo.setText(text);
      }
    } finally {
      loading = wasLoading;
    }
  }

  private static String[] itemsOf(Control control) {
    if (control instanceof ComboVar comboVar) {
      return comboVar.getItems();
    } else if (control instanceof Combo combo) {
      return combo.getItems();
    }
    return null;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    widgets.getWidgetsContents(input, XmlJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    dispose();
  }
}
