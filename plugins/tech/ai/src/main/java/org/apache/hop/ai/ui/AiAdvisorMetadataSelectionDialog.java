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

package org.apache.hop.ai.ui;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.engine.AiAdvisorMetadataContext;
import org.apache.hop.ai.engine.AiAdvisorMetadataContext.TypeCatalog;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/** Checked tree of project metadata types and objects to include in an advisory prompt. */
public class AiAdvisorMetadataSelectionDialog {

  private static final Class<?> PKG = AiAdvisorPerspective.class;
  private static final String DATA_SELECTION = "selection";
  private static final String DATA_TYPE_KEY = "typeKey";

  private final Shell parent;
  private final List<TypeCatalog> catalog;
  private final Set<AiAdvisorMetadataSelection> checked = new LinkedHashSet<>();
  private Shell shell;
  private Text wFilter;
  private Tree wTree;
  private boolean accepted;
  private List<AiAdvisorMetadataSelection> result = List.of();

  public AiAdvisorMetadataSelectionDialog(
      Shell parent,
      IHopMetadataProvider metadataProvider,
      List<AiAdvisorMetadataSelection> current) {
    this.parent = parent;
    this.catalog = AiAdvisorMetadataContext.listTypes(metadataProvider);
    if (current != null) {
      checked.addAll(current);
    }
  }

  public List<AiAdvisorMetadataSelection> open() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "AiAdvisorMetadataSelectionDialog.Title"));
    shell.setLayout(PropsUi.getInstance().createFormLayout());
    int margin = PropsUi.getMargin();

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "AiAdvisorMetadataSelectionDialog.Ok.Label"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "AiAdvisorMetadataSelectionDialog.Cancel.Label"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    Button wNone = new Button(shell, SWT.PUSH);
    wNone.setText(BaseMessages.getString(PKG, "AiAdvisorMetadataSelectionDialog.None.Label"));
    wNone.addListener(SWT.Selection, e -> clearAll());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel, wNone}, margin, null);

    Label wlFilter = new Label(shell, SWT.LEFT);
    wlFilter.setText(BaseMessages.getString(PKG, "AiAdvisorMetadataSelectionDialog.Filter.Label"));
    PropsUi.setLook(wlFilter);
    wlFilter.setLayoutData(new FormDataBuilder().left(0, margin).top(0, margin).result());

    wFilter = new Text(shell, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(wFilter);
    wFilter.setLayoutData(
        new FormDataBuilder().left(wlFilter, margin).top(0, margin).right(100, -margin).result());
    wFilter.addListener(SWT.Modify, e -> rebuildTree());

    Label wlTree = new Label(shell, SWT.LEFT | SWT.WRAP);
    wlTree.setText(BaseMessages.getString(PKG, "AiAdvisorMetadataSelectionDialog.Tree.Label"));
    PropsUi.setLook(wlTree);
    wlTree.setLayoutData(
        new FormDataBuilder().left(0, margin).top(wFilter, margin).right(100, -margin).result());

    wTree = new Tree(shell, SWT.BORDER | SWT.CHECK | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wTree);
    wTree.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wlTree, margin)
            .right(100, -margin)
            .bottom(wOk, -margin)
            .result());
    wTree.addListener(SWT.Selection, this::treeChecked);

    rebuildTree();
    BaseTransformDialog.setSize(shell);
    shell.open();
    while (!shell.isDisposed()) {
      if (!parent.getDisplay().readAndDispatch()) {
        parent.getDisplay().sleep();
      }
    }
    return accepted ? result : null;
  }

  private void rebuildTree() {
    if (wTree == null || wTree.isDisposed()) {
      return;
    }
    String filter = Const.NVL(wFilter != null ? wFilter.getText() : "", "").trim().toLowerCase();
    wTree.removeAll();
    for (TypeCatalog type : catalog) {
      List<String> names = matchingNames(type, filter);
      if (names.isEmpty()) {
        continue;
      }
      TreeItem typeItem = new TreeItem(wTree, SWT.NONE);
      typeItem.setText(type.getTypeName() + " (" + names.size() + ")");
      typeItem.setData(DATA_TYPE_KEY, type.getTypeKey());
      int checkedCount = 0;
      for (String name : names) {
        TreeItem child = new TreeItem(typeItem, SWT.NONE);
        child.setText(name);
        AiAdvisorMetadataSelection selection =
            new AiAdvisorMetadataSelection(type.getTypeKey(), name);
        child.setData(DATA_SELECTION, selection);
        boolean isChecked = checked.contains(selection);
        child.setChecked(isChecked);
        if (isChecked) {
          checkedCount++;
        }
      }
      typeItem.setChecked(checkedCount == names.size());
      typeItem.setGrayed(checkedCount > 0 && checkedCount < names.size());
      typeItem.setExpanded(true);
    }
  }

  private List<String> matchingNames(TypeCatalog type, String filter) {
    if (Utils.isEmpty(filter)) {
      return type.getNames();
    }
    List<String> names = new ArrayList<>();
    for (String name : type.getNames()) {
      if (name.toLowerCase().contains(filter)
          || type.getTypeName().toLowerCase().contains(filter)) {
        names.add(name);
      }
    }
    return names;
  }

  private void treeChecked(Event event) {
    if (event.detail != SWT.CHECK || !(event.item instanceof TreeItem item)) {
      return;
    }
    boolean on = item.getChecked();
    item.setGrayed(false);
    Object selectionData = item.getData(DATA_SELECTION);
    if (selectionData instanceof AiAdvisorMetadataSelection selection) {
      if (on) {
        checked.add(selection);
      } else {
        checked.remove(selection);
      }
      syncParent(item.getParentItem());
      return;
    }
    if (item.getData(DATA_TYPE_KEY) instanceof String) {
      for (TreeItem child : item.getItems()) {
        child.setChecked(on);
        Object childData = child.getData(DATA_SELECTION);
        if (childData instanceof AiAdvisorMetadataSelection selection) {
          if (on) {
            checked.add(selection);
          } else {
            checked.remove(selection);
          }
        }
      }
    }
  }

  private void syncParent(TreeItem parentItem) {
    if (parentItem == null) {
      return;
    }
    int checkedCount = 0;
    TreeItem[] children = parentItem.getItems();
    for (TreeItem child : children) {
      if (child.getChecked()) {
        checkedCount++;
      }
    }
    parentItem.setChecked(checkedCount == children.length && children.length > 0);
    parentItem.setGrayed(checkedCount > 0 && checkedCount < children.length);
  }

  private void clearAll() {
    checked.clear();
    rebuildTree();
  }

  private void ok() {
    accepted = true;
    result = new ArrayList<>(checked);
    close();
  }

  private void cancel() {
    accepted = false;
    close();
  }

  private void close() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
