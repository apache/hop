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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.DropTargetListener;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * SWT composite that lets the user design an {@link XmlNode} tree visually.
 *
 * <p>Layout: a horizontal sash with the input-fields list on the left (drag source) and a vertical
 * sash on the right hosting the target tree (drop target + toolbar / context menu) on top and a
 * properties form for the selected node on the bottom.
 *
 * <p>The composite owns the {@link XmlNode} model passed via {@link #setRootNode(XmlNode)} and
 * mutates it in place; the host dialog reads the (mutated) model back via {@link #getRootNode()}
 * when the user clicks OK.
 */
public class XmlTreeDesigner extends Composite {

  private static final Class<?> PKG = AdvancedXmlOutputMeta.class;

  /** Listener that observers can register to be notified that the tree was modified. */
  public interface ChangeListener {
    void onChanged();
  }

  private final IVariables variables;

  // Left side: input fields
  private org.eclipse.swt.widgets.List wInputFields;
  private Button wGetFields;

  // Right top: tree + toolbar
  private Tree wTree;

  // Right bottom: properties form
  private Text wpName;
  private Text wpNamespace;
  private CCombo wpKind;
  private CCombo wpMappedField;
  private Text wpDefaultValue;
  private Text wpFormat;
  private Text wpLength;
  private Text wpPrecision;
  private Text wpCurrency;
  private Text wpDecimal;
  private Text wpGroup;
  private Button wpLoop;
  private Button wpGroupBy;
  private Button wpForceCreate;
  private Button wpStripOuterFragment;

  /** Root of the tree being designed; never {@code null} after {@link #setRootNode(XmlNode)}. */
  private XmlNode rootNode;

  /** Available input field names, used for the field combo and the drag-source list. */
  private final List<String> inputFields = new ArrayList<>();

  /** Avoid feedback loops when populating the properties form for the current selection. */
  private boolean updatingProperties = false;

  private ChangeListener changeListener;

  public XmlTreeDesigner(Composite parent, int style, IVariables variables) {
    super(parent, style);
    this.variables = variables;
    setLayout(new FormLayout());
    build();
  }

  // ---------------------------------------------------------------------------
  // Public API used by the host dialog
  // ---------------------------------------------------------------------------

  public void setRootNode(XmlNode root) {
    this.rootNode = root == null ? defaultRoot() : root;
    refreshTree();
    if (wTree.getItemCount() > 0) {
      wTree.setSelection(wTree.getItem(0));
      handleSelectionChanged();
    }
  }

  public XmlNode getRootNode() {
    return rootNode;
  }

  public void setInputFields(List<String> fields) {
    this.inputFields.clear();
    if (fields != null) {
      this.inputFields.addAll(fields);
    }
    refreshInputFieldsList();
    refreshMappedFieldCombo();
  }

  public void setChangeListener(ChangeListener listener) {
    this.changeListener = listener;
  }

  public void setGetFieldsListener(Listener listener) {
    if (wGetFields != null && listener != null) {
      wGetFields.addListener(SWT.Selection, listener);
    }
  }

  // ---------------------------------------------------------------------------
  // UI build
  // ---------------------------------------------------------------------------

  private void build() {
    SashForm hSash = new SashForm(this, SWT.HORIZONTAL);
    PropsUi.setLook(hSash);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    hSash.setLayoutData(fdSash);

    buildLeftPane(hSash);
    buildRightPane(hSash);
    hSash.setWeights(new int[] {25, 75});
  }

  private void buildLeftPane(Composite parent) {
    Composite left = new Composite(parent, SWT.NONE);
    PropsUi.setLook(left);
    FormLayout fl = new FormLayout();
    fl.marginWidth = 3;
    fl.marginHeight = 3;
    left.setLayout(fl);

    Label lbl = new Label(left, SWT.NONE);
    lbl.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.InputFields.Label"));
    PropsUi.setLook(lbl);
    FormData fdLbl = new FormData();
    fdLbl.left = new FormAttachment(0, 0);
    fdLbl.top = new FormAttachment(0, 0);
    fdLbl.right = new FormAttachment(100, 0);
    lbl.setLayoutData(fdLbl);

    wGetFields = new Button(left, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.GetFields.Button"));
    PropsUi.setLook(wGetFields);
    FormData fdGet = new FormData();
    fdGet.left = new FormAttachment(0, 0);
    fdGet.right = new FormAttachment(100, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGet);

    wInputFields =
        new org.eclipse.swt.widgets.List(
            left, SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
    PropsUi.setLook(wInputFields);
    FormData fdList = new FormData();
    fdList.left = new FormAttachment(0, 0);
    fdList.right = new FormAttachment(100, 0);
    fdList.top = new FormAttachment(lbl, 3);
    fdList.bottom = new FormAttachment(wGetFields, -3);
    wInputFields.setLayoutData(fdList);

    setupFieldsDragSource();
  }

  private void buildRightPane(Composite parent) {
    SashForm vSash = new SashForm(parent, SWT.VERTICAL);
    PropsUi.setLook(vSash);
    buildTreeWithToolbar(vSash);
    buildPropertiesPane(vSash);
    vSash.setWeights(new int[] {65, 35});
  }

  private void buildTreeWithToolbar(Composite parent) {
    Composite top = new Composite(parent, SWT.NONE);
    PropsUi.setLook(top);
    FormLayout fl = new FormLayout();
    fl.marginWidth = 3;
    fl.marginHeight = 3;
    top.setLayout(fl);

    ToolBar tb = new ToolBar(top, SWT.FLAT | SWT.HORIZONTAL);
    PropsUi.setLook(tb);
    FormData fdTb = new FormData();
    fdTb.left = new FormAttachment(0, 0);
    fdTb.top = new FormAttachment(0, 0);
    fdTb.right = new FormAttachment(100, 0);
    tb.setLayoutData(fdTb);

    addToolItem(tb, "AddElement", e -> addChild(XmlNode.NodeKind.Element));
    addToolItem(tb, "AddAttribute", e -> addChild(XmlNode.NodeKind.Attribute));
    addToolItem(tb, "AddFragment", e -> addChild(XmlNode.NodeKind.DocumentFragment));
    new ToolItem(tb, SWT.SEPARATOR);
    addToolItem(tb, "Delete", e -> deleteSelected());
    addToolItem(tb, "MoveUp", e -> moveSelected(-1));
    addToolItem(tb, "MoveDown", e -> moveSelected(+1));
    new ToolItem(tb, SWT.SEPARATOR);
    addToolItem(tb, "ToggleLoop", e -> toggleSelectedFlag(true, false));
    addToolItem(tb, "ToggleGroupBy", e -> toggleSelectedFlag(false, true));

    wTree = new Tree(top, SWT.BORDER | SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wTree);
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment(0, 0);
    fdTree.top = new FormAttachment(tb, 3);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.bottom = new FormAttachment(100, 0);
    wTree.setLayoutData(fdTree);

    wTree.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            handleSelectionChanged();
          }
        });

    setupTreeDropTarget();
    setupTreeContextMenu();
  }

  private void addToolItem(ToolBar tb, String key, Listener selectionListener) {
    ToolItem ti = new ToolItem(tb, SWT.PUSH);
    ti.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Toolbar." + key));
    ti.setToolTipText(
        BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Toolbar." + key + ".Tooltip"));
    ti.addListener(SWT.Selection, selectionListener);
  }

  private void buildPropertiesPane(Composite parent) {
    Composite props = new Composite(parent, SWT.NONE);
    PropsUi.setLook(props);
    FormLayout fl = new FormLayout();
    fl.marginWidth = 6;
    fl.marginHeight = 6;
    props.setLayout(fl);

    int labelWidth = 110;

    wpName = addLabeledText(props, null, "Name", labelWidth);
    wpNamespace = addLabeledText(props, wpName, "Namespace", labelWidth);

    Label lblKind = addLabel(props, wpNamespace, "Kind", labelWidth);
    wpKind = new CCombo(props, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wpKind);
    for (XmlNode.NodeKind k : XmlNode.NodeKind.values()) {
      wpKind.add(k.name());
    }
    layoutValueRight(lblKind, wpKind, labelWidth);
    wpKind.addModifyListener(e -> applyPropertiesToModel());

    // Attach each row below the previous row's value control (not the label): CCombo is taller than
    // a Label, so attaching the next label under the previous label caused vertical overlap.
    Label lblField = addLabel(props, wpKind, "MappedField", labelWidth);
    wpMappedField = new CCombo(props, SWT.BORDER);
    PropsUi.setLook(wpMappedField);
    layoutValueRight(lblField, wpMappedField, labelWidth);
    wpMappedField.addModifyListener(e -> applyPropertiesToModel());

    wpDefaultValue = addLabeledText(props, wpMappedField, "DefaultValue", labelWidth);
    wpFormat = addLabeledText(props, wpDefaultValue, "Format", labelWidth);

    Composite numericRow = new Composite(props, SWT.NONE);
    PropsUi.setLook(numericRow);
    FormLayout nrL = new FormLayout();
    nrL.marginWidth = 0;
    nrL.marginHeight = 0;
    numericRow.setLayout(nrL);
    FormData fdNr = new FormData();
    fdNr.left = new FormAttachment(0, labelWidth);
    fdNr.right = new FormAttachment(100, 0);
    fdNr.top = new FormAttachment(wpFormat, 3);
    numericRow.setLayoutData(fdNr);

    Label lblL = new Label(numericRow, SWT.NONE);
    lblL.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Properties.Length"));
    PropsUi.setLook(lblL);
    FormData fdLblL = new FormData();
    fdLblL.left = new FormAttachment(0, 0);
    fdLblL.top = new FormAttachment(0, 0);
    lblL.setLayoutData(fdLblL);

    wpLength = new Text(numericRow, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(wpLength);
    FormData fdLen = new FormData();
    fdLen.left = new FormAttachment(lblL, 3);
    fdLen.top = new FormAttachment(0, 0);
    fdLen.width = 60;
    wpLength.setLayoutData(fdLen);
    wpLength.addModifyListener(e -> applyPropertiesToModel());

    Label lblP = new Label(numericRow, SWT.NONE);
    lblP.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Properties.Precision"));
    PropsUi.setLook(lblP);
    FormData fdLblP = new FormData();
    fdLblP.left = new FormAttachment(wpLength, 12);
    fdLblP.top = new FormAttachment(0, 0);
    lblP.setLayoutData(fdLblP);

    wpPrecision = new Text(numericRow, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(wpPrecision);
    FormData fdPrec = new FormData();
    fdPrec.left = new FormAttachment(lblP, 3);
    fdPrec.top = new FormAttachment(0, 0);
    fdPrec.width = 60;
    wpPrecision.setLayoutData(fdPrec);
    wpPrecision.addModifyListener(e -> applyPropertiesToModel());

    wpCurrency = addLabeledText(props, numericRow, "Currency", labelWidth);
    wpDecimal = addLabeledText(props, wpCurrency, "Decimal", labelWidth);
    wpGroup = addLabeledText(props, wpDecimal, "Grouping", labelWidth);

    Composite flagsRow = new Composite(props, SWT.NONE);
    PropsUi.setLook(flagsRow);
    FormLayout flR = new FormLayout();
    flR.marginWidth = 0;
    flR.marginHeight = 0;
    flagsRow.setLayout(flR);
    FormData fdFlR = new FormData();
    fdFlR.left = new FormAttachment(0, labelWidth);
    fdFlR.right = new FormAttachment(100, 0);
    fdFlR.top = new FormAttachment(wpGroup, 6);
    flagsRow.setLayoutData(fdFlR);

    wpLoop = newFlagButton(flagsRow, null, "Loop");
    wpGroupBy = newFlagButton(flagsRow, wpLoop, "GroupBy");
    wpForceCreate = newFlagButton(flagsRow, wpGroupBy, "ForceCreate");
    wpStripOuterFragment = newFlagButton(flagsRow, wpForceCreate, "StripOuterFragment");
  }

  private Button newFlagButton(Composite parent, Button leftOf, String key) {
    Button b = new Button(parent, SWT.CHECK);
    b.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Properties." + key));
    PropsUi.setLook(b);
    FormData fd = new FormData();
    fd.top = new FormAttachment(0, 0);
    if (leftOf == null) {
      fd.left = new FormAttachment(0, 0);
    } else {
      fd.left = new FormAttachment(leftOf, 12);
    }
    b.setLayoutData(fd);
    b.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            applyPropertiesToModel();
          }
        });
    return b;
  }

  private Label addLabel(Composite parent, Control below, String key, int labelWidth) {
    Label lbl = new Label(parent, SWT.NONE);
    lbl.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Properties." + key));
    PropsUi.setLook(lbl);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.width = labelWidth - 3;
    fd.top = below == null ? new FormAttachment(0, 0) : new FormAttachment(below, 6);
    lbl.setLayoutData(fd);
    return lbl;
  }

  private void layoutValueRight(Control labelControl, Control valueControl, int labelWidth) {
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, labelWidth);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(labelControl, 0, SWT.TOP);
    valueControl.setLayoutData(fd);
  }

  private Text addLabeledText(Composite parent, Control below, String key, int labelWidth) {
    Label lbl = addLabel(parent, below, key, labelWidth);
    Text t = new Text(parent, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(t);
    layoutValueRight(lbl, t, labelWidth);
    t.addModifyListener(e -> applyPropertiesToModel());
    return t;
  }

  // ---------------------------------------------------------------------------
  // DnD: dragging fields from the input list to the tree
  // ---------------------------------------------------------------------------

  private void setupFieldsDragSource() {
    Transfer[] transfers = new Transfer[] {TextTransfer.getInstance()};
    DragSource source = new DragSource(wInputFields, DND.DROP_COPY | DND.DROP_MOVE);
    source.setTransfer(transfers);
    source.addDragListener(
        new DragSourceListener() {
          @Override
          public void dragStart(DragSourceEvent event) {
            event.doit = wInputFields.getSelectionCount() > 0;
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            String[] sel = wInputFields.getSelection();
            StringBuilder sb = new StringBuilder();
            for (String s : sel) {
              sb.append(s).append(Const.CR);
            }
            event.data = sb.toString();
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            // nothing
          }
        });
  }

  private void setupTreeDropTarget() {
    Transfer[] transfers = new Transfer[] {TextTransfer.getInstance()};
    DropTarget target = new DropTarget(wTree, DND.DROP_COPY | DND.DROP_MOVE);
    target.setTransfer(transfers);
    target.addDropListener(
        new DropTargetListener() {
          @Override
          public void dragEnter(DropTargetEvent event) {
            // nothing
          }

          @Override
          public void dragLeave(DropTargetEvent event) {
            // nothing
          }

          @Override
          public void dragOperationChanged(DropTargetEvent event) {
            // nothing
          }

          @Override
          public void dragOver(DropTargetEvent event) {
            event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_EXPAND | DND.FEEDBACK_SCROLL;
          }

          @Override
          public void drop(DropTargetEvent event) {
            if (event.data == null) {
              event.detail = DND.DROP_NONE;
              return;
            }
            XmlNode targetNode =
                event.item instanceof TreeItem ti ? (XmlNode) ti.getData() : rootNode;
            if (targetNode == null) {
              return;
            }
            // Adding under an Attribute makes no sense - redirect to its parent.
            if (targetNode.getKind() == XmlNode.NodeKind.Attribute) {
              XmlNode parent = findParent(targetNode);
              targetNode = parent != null ? parent : rootNode;
            }
            StringTokenizer tok = new StringTokenizer((String) event.data, Const.CR);
            XmlNode lastAdded = null;
            while (tok.hasMoreTokens()) {
              String fieldName = tok.nextToken();
              if (Utils.isEmpty(fieldName)) {
                continue;
              }
              XmlNode added = new XmlNode(fieldName, XmlNode.NodeKind.Element);
              added.setMappedField(fieldName);
              targetNode.addChild(added);
              lastAdded = added;
            }
            refreshTree();
            if (lastAdded != null) {
              selectNode(lastAdded);
            }
            fireChanged();
          }

          @Override
          public void dropAccept(DropTargetEvent event) {
            // nothing
          }
        });
  }

  // ---------------------------------------------------------------------------
  // Context menu mirroring the toolbar
  // ---------------------------------------------------------------------------

  private void setupTreeContextMenu() {
    Menu menu = new Menu(wTree);

    menu.addListener(
        SWT.Show,
        e -> {
          XmlNode sel = currentSelection();
          for (MenuItem mi : menu.getItems()) {
            mi.dispose();
          }
          buildMenu(menu, sel);
        });

    wTree.setMenu(menu);
  }

  private void buildMenu(Menu menu, XmlNode sel) {
    addMenu(menu, "AddElement", () -> addChild(XmlNode.NodeKind.Element));
    addMenu(menu, "AddAttribute", () -> addChild(XmlNode.NodeKind.Attribute));
    addMenu(menu, "AddFragment", () -> addChild(XmlNode.NodeKind.DocumentFragment));
    new MenuItem(menu, SWT.SEPARATOR);
    addMenu(menu, "Delete", this::deleteSelected, sel != null && sel != rootNode);
    addMenu(menu, "MoveUp", () -> moveSelected(-1), sel != null && sel != rootNode);
    addMenu(menu, "MoveDown", () -> moveSelected(+1), sel != null && sel != rootNode);
    new MenuItem(menu, SWT.SEPARATOR);
    addCheckMenu(
        menu, "ToggleLoop", sel != null && sel.isLoop(), () -> toggleSelectedFlag(true, false));
    addCheckMenu(
        menu,
        "ToggleGroupBy",
        sel != null && sel.isGroupBy(),
        () -> toggleSelectedFlag(false, true));
  }

  private void addMenu(Menu parent, String key, Runnable action) {
    addMenu(parent, key, action, true);
  }

  private void addMenu(Menu parent, String key, Runnable action, boolean enabled) {
    MenuItem mi = new MenuItem(parent, SWT.PUSH);
    mi.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Toolbar." + key));
    mi.setEnabled(enabled);
    mi.addListener(SWT.Selection, ignore -> action.run());
  }

  private void addCheckMenu(Menu parent, String key, boolean selected, Runnable action) {
    MenuItem mi = new MenuItem(parent, SWT.CHECK);
    mi.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Toolbar." + key));
    mi.setSelection(selected);
    mi.addListener(SWT.Selection, ignore -> action.run());
  }

  // ---------------------------------------------------------------------------
  // Tree mutations
  // ---------------------------------------------------------------------------

  private void addChild(XmlNode.NodeKind kind) {
    XmlNode parent = currentSelection();
    if (parent == null) {
      parent = rootNode;
    }
    if (parent.getKind() == XmlNode.NodeKind.Attribute
        || parent.getKind() == XmlNode.NodeKind.DocumentFragment) {
      // Re-parent to the actual parent element.
      XmlNode p = findParent(parent);
      parent = p != null ? p : rootNode;
    }
    XmlNode child =
        new XmlNode(kind == XmlNode.NodeKind.Attribute ? "newAttr" : "newElement", kind);
    parent.addChild(child);
    refreshTree();
    selectNode(child);
    fireChanged();
  }

  private void deleteSelected() {
    XmlNode sel = currentSelection();
    if (sel == null || sel == rootNode) {
      return;
    }
    XmlNode parent = findParent(sel);
    if (parent != null && parent.getChildren() != null) {
      parent.getChildren().remove(sel);
      refreshTree();
      selectNode(parent);
      fireChanged();
    }
  }

  private void moveSelected(int delta) {
    XmlNode sel = currentSelection();
    if (sel == null || sel == rootNode) {
      return;
    }
    XmlNode parent = findParent(sel);
    if (parent == null || parent.getChildren() == null) {
      return;
    }
    List<XmlNode> sibs = parent.getChildren();
    int idx = sibs.indexOf(sel);
    int newIdx = idx + delta;
    if (idx < 0 || newIdx < 0 || newIdx >= sibs.size()) {
      return;
    }
    sibs.remove(idx);
    sibs.add(newIdx, sel);
    refreshTree();
    selectNode(sel);
    fireChanged();
  }

  /**
   * Toggle the loop / group-by flag of the current selection.
   *
   * <p>Loop is mutually exclusive across the entire tree; setting one node as loop clears the flag
   * elsewhere first. Group-by may co-exist on multiple ancestors of the loop.
   */
  private void toggleSelectedFlag(boolean toggleLoop, boolean toggleGroupBy) {
    XmlNode sel = currentSelection();
    if (sel == null) {
      return;
    }
    if (toggleLoop) {
      boolean newVal = !sel.isLoop();
      if (newVal) {
        clearLoopFlag(rootNode);
      }
      sel.setLoop(newVal);
    }
    if (toggleGroupBy) {
      sel.setGroupBy(!sel.isGroupBy());
    }
    refreshTree();
    selectNode(sel);
    fireChanged();
  }

  private void clearLoopFlag(XmlNode n) {
    n.setLoop(false);
    if (n.getChildren() != null) {
      for (XmlNode c : n.getChildren()) {
        clearLoopFlag(c);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Properties form ↔ model sync
  // ---------------------------------------------------------------------------

  private void handleSelectionChanged() {
    XmlNode n = currentSelection();
    populateProperties(n);
  }

  private void populateProperties(XmlNode n) {
    updatingProperties = true;
    try {
      boolean enabled = n != null;
      setPropertiesEnabled(enabled);
      if (n == null) {
        wpName.setText("");
        wpNamespace.setText("");
        wpKind.select(0);
        wpMappedField.setText("");
        wpDefaultValue.setText("");
        wpFormat.setText("");
        wpLength.setText("");
        wpPrecision.setText("");
        wpCurrency.setText("");
        wpDecimal.setText("");
        wpGroup.setText("");
        wpLoop.setSelection(false);
        wpGroupBy.setSelection(false);
        wpForceCreate.setSelection(false);
        wpStripOuterFragment.setSelection(false);
        return;
      }
      wpName.setText(Const.NVL(n.getName(), ""));
      wpNamespace.setText(Const.NVL(n.getNamespace(), ""));
      wpKind.setText(n.getKind() == null ? XmlNode.NodeKind.Element.name() : n.getKind().name());
      wpMappedField.setText(Const.NVL(n.getMappedField(), ""));
      wpDefaultValue.setText(Const.NVL(n.getDefaultValue(), ""));
      wpFormat.setText(Const.NVL(n.getFormat(), ""));
      wpLength.setText(n.getLength() > 0 ? Integer.toString(n.getLength()) : "");
      wpPrecision.setText(n.getPrecision() > 0 ? Integer.toString(n.getPrecision()) : "");
      wpCurrency.setText(Const.NVL(n.getCurrencySymbol(), ""));
      wpDecimal.setText(Const.NVL(n.getDecimalSymbol(), ""));
      wpGroup.setText(Const.NVL(n.getGroupingSymbol(), ""));
      wpLoop.setSelection(n.isLoop());
      wpGroupBy.setSelection(n.isGroupBy());
      wpForceCreate.setSelection(n.isForceCreate());
      wpStripOuterFragment.setSelection(n.isStripOuterFragmentElement());
      boolean frag = n.getKind() == XmlNode.NodeKind.DocumentFragment;
      wpStripOuterFragment.setEnabled(frag);
    } finally {
      updatingProperties = false;
    }
  }

  private void setPropertiesEnabled(boolean enabled) {
    wpName.setEnabled(enabled);
    wpNamespace.setEnabled(enabled);
    wpKind.setEnabled(enabled);
    wpMappedField.setEnabled(enabled);
    wpDefaultValue.setEnabled(enabled);
    wpFormat.setEnabled(enabled);
    wpLength.setEnabled(enabled);
    wpPrecision.setEnabled(enabled);
    wpCurrency.setEnabled(enabled);
    wpDecimal.setEnabled(enabled);
    wpGroup.setEnabled(enabled);
    wpLoop.setEnabled(enabled);
    wpGroupBy.setEnabled(enabled);
    wpForceCreate.setEnabled(enabled);
    wpStripOuterFragment.setEnabled(enabled);
  }

  private void applyPropertiesToModel() {
    if (updatingProperties) {
      return;
    }
    XmlNode n = currentSelection();
    if (n == null) {
      return;
    }
    n.setName(wpName.getText());
    n.setNamespace(wpNamespace.getText());
    n.setKind(XmlNode.NodeKind.getIfPresent(wpKind.getText()));
    n.setMappedField(wpMappedField.getText());
    n.setDefaultValue(wpDefaultValue.getText());
    n.setFormat(wpFormat.getText());
    n.setLength(parseInt(wpLength.getText(), -1));
    n.setPrecision(parseInt(wpPrecision.getText(), -1));
    n.setCurrencySymbol(wpCurrency.getText());
    n.setDecimalSymbol(wpDecimal.getText());
    n.setGroupingSymbol(wpGroup.getText());

    // Loop flag is mutually exclusive
    boolean wantLoop = wpLoop.getSelection();
    if (wantLoop && !n.isLoop()) {
      clearLoopFlag(rootNode);
    }
    n.setLoop(wantLoop);
    n.setGroupBy(wpGroupBy.getSelection());
    n.setForceCreate(wpForceCreate.getSelection());
    if (n.getKind() == XmlNode.NodeKind.DocumentFragment) {
      n.setStripOuterFragmentElement(wpStripOuterFragment.getSelection());
    } else {
      n.setStripOuterFragmentElement(false);
    }
    wpStripOuterFragment.setEnabled(n.getKind() == XmlNode.NodeKind.DocumentFragment);

    refreshSelectedTreeItemLabel();
    fireChanged();
  }

  private static int parseInt(String s, int dflt) {
    if (s == null || s.isBlank()) {
      return dflt;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return dflt;
    }
  }

  // ---------------------------------------------------------------------------
  // Tree refresh / lookup
  // ---------------------------------------------------------------------------

  /** Rebuilds the tree from the model. Preserves expansion where possible. */
  private void refreshTree() {
    wTree.removeAll();
    if (rootNode == null) {
      return;
    }
    TreeItem rootItem = new TreeItem(wTree, SWT.NONE);
    populateItem(rootItem, rootNode);
    rootItem.setExpanded(true);
  }

  private void populateItem(TreeItem item, XmlNode node) {
    item.setData(node);
    item.setText(formatNodeLabel(node));
    if (node.getChildren() != null) {
      for (XmlNode c : node.getChildren()) {
        TreeItem child = new TreeItem(item, SWT.NONE);
        populateItem(child, c);
      }
    }
    item.setExpanded(true);
  }

  private static String formatNodeLabel(XmlNode n) {
    StringBuilder sb = new StringBuilder();
    sb.append(n.getKind() == XmlNode.NodeKind.Attribute ? "@" : "");
    sb.append(Utils.isEmpty(n.getName()) ? "(unnamed)" : n.getName());
    if (n.isLoop()) {
      sb.append("  [loop]");
    }
    if (n.isGroupBy()) {
      sb.append("  [group-by]");
    }
    if (n.isStripOuterFragmentElement()) {
      sb.append("  [strip]");
    }
    if (!Utils.isEmpty(n.getMappedField())) {
      sb.append("  ← ").append(n.getMappedField());
    } else if (!Utils.isEmpty(n.getDefaultValue())) {
      sb.append("  = ").append(n.getDefaultValue());
    }
    return sb.toString();
  }

  private void refreshSelectedTreeItemLabel() {
    TreeItem[] sel = wTree.getSelection();
    if (sel.length == 0) {
      return;
    }
    XmlNode n = (XmlNode) sel[0].getData();
    if (n != null) {
      sel[0].setText(formatNodeLabel(n));
    }
  }

  private XmlNode currentSelection() {
    TreeItem[] sel = wTree.getSelection();
    return sel.length == 0 ? null : (XmlNode) sel[0].getData();
  }

  /** Selects the tree item that holds the given node. */
  private void selectNode(XmlNode target) {
    TreeItem found = findTreeItem(wTree.getItems(), target);
    if (found != null) {
      wTree.setSelection(found);
      handleSelectionChanged();
    }
  }

  private TreeItem findTreeItem(TreeItem[] items, XmlNode target) {
    for (TreeItem ti : items) {
      if (ti.getData() == target) {
        return ti;
      }
      TreeItem inner = findTreeItem(ti.getItems(), target);
      if (inner != null) {
        return inner;
      }
    }
    return null;
  }

  /** Finds the parent node of {@code target} in the model, or null for root. */
  private XmlNode findParent(XmlNode target) {
    return findParentRecursive(rootNode, target);
  }

  private XmlNode findParentRecursive(XmlNode current, XmlNode target) {
    if (current == null || current.getChildren() == null) {
      return null;
    }
    for (XmlNode c : current.getChildren()) {
      if (c == target) {
        return current;
      }
      XmlNode deeper = findParentRecursive(c, target);
      if (deeper != null) {
        return deeper;
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Misc
  // ---------------------------------------------------------------------------

  private void refreshInputFieldsList() {
    if (wInputFields == null) {
      return;
    }
    wInputFields.removeAll();
    for (String f : inputFields) {
      wInputFields.add(f);
    }
  }

  private void refreshMappedFieldCombo() {
    if (wpMappedField == null) {
      return;
    }
    String current = wpMappedField.getText();
    wpMappedField.removeAll();
    wpMappedField.add("");
    for (String f : inputFields) {
      wpMappedField.add(f);
    }
    wpMappedField.setText(current == null ? "" : current);
  }

  private void fireChanged() {
    if (changeListener != null) {
      changeListener.onChanged();
    }
  }

  private static XmlNode defaultRoot() {
    XmlNode root = new XmlNode("Rows", XmlNode.NodeKind.Element);
    XmlNode loop = new XmlNode("Row", XmlNode.NodeKind.Element);
    loop.setLoop(true);
    root.addChild(loop);
    return root;
  }

  /** Returns the variables resolver, currently unused but kept for future preview features. */
  IVariables getVariables() {
    return variables;
  }
}
