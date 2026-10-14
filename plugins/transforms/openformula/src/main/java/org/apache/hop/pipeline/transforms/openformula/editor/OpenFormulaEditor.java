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

package org.apache.hop.pipeline.transforms.openformula.editor;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.pipeline.transforms.openformula.OpenFormulaMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/** OpenFormula editor: field and function tree, expression, and function help. */
public class OpenFormulaEditor extends Dialog {
  public static final Class<?> PKG = OpenFormulaMeta.class;
  private static final int DEFAULT_WIDTH = 900;
  private static final int DEFAULT_HEIGHT = 700;
  private static final String FIELDS_NODE = "Fields";

  private final IVariables variables;
  private final String[] inputFields;
  private Shell shell;
  private StyledTextComp expressionEditor;
  private Browser message;
  private String formula;

  public OpenFormulaEditor(
      IVariables variables, Shell parent, int style, String formula, String[] inputFields) {
    super(parent, style);
    this.variables = variables;
    this.formula = formula;
    this.inputFields = inputFields;
  }

  public String open() {
    Shell parent = getParent();
    shell =
        new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX | SWT.APPLICATION_MODAL);
    PropsUi.setLook(shell);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "OpenFormulaEditor.Shell.Title"));

    Composite buttonsComposite = new Composite(shell, SWT.NONE);
    FillLayout bcLayout = new FillLayout();
    bcLayout.spacing = 5;
    buttonsComposite.setLayout(bcLayout);
    Button ok = new Button(buttonsComposite, SWT.PUSH);
    ok.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button cancel = new Button(buttonsComposite, SWT.PUSH);
    cancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    ok.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            ok();
          }
        });
    cancel.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cancel();
          }
        });

    SashForm sashForm = new SashForm(shell, SWT.HORIZONTAL);
    sashForm.setLayout(new FillLayout());
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment(0, 0);
    fdSashForm.right = new FormAttachment(100, 0);
    fdSashForm.top = new FormAttachment(0, 10);
    fdSashForm.bottom = new FormAttachment(buttonsComposite, -10);
    sashForm.setLayoutData(fdSashForm);

    FormData fdBC = new FormData();
    fdBC.left = new FormAttachment(sashForm, 0, SWT.CENTER);
    fdBC.bottom = new FormAttachment(100, 0);
    buttonsComposite.setLayoutData(fdBC);

    Tree tree = new Tree(sashForm, SWT.SINGLE);
    TreeItem fieldsItem = new TreeItem(tree, SWT.NONE);
    fieldsItem.setText(FIELDS_NODE);
    for (String inputField : inputFields) {
      TreeItem fieldItem = new TreeItem(fieldsItem, SWT.NONE);
      fieldItem.setText(inputField);
    }

    OpenFormulaFunctionCatalog catalog = new OpenFormulaFunctionCatalog();
    for (String category : catalog.getCategories()) {
      TreeItem item = new TreeItem(tree, SWT.NONE);
      item.setText(category);
      for (FunctionDescription function : catalog.getFunctions(category)) {
        TreeItem fitem = new TreeItem(item, SWT.NONE);
        fitem.setText(function.getCanonicalName());
        fitem.setData(function);
      }
    }

    SashForm rightSash = new SashForm(sashForm, SWT.VERTICAL);
    expressionEditor =
        new StyledTextComp(
            variables, rightSash, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    expressionEditor.setText(this.formula == null ? "" : this.formula);
    message = new Browser(rightSash, SWT.MULTI | SWT.READ_ONLY | SWT.V_SCROLL | SWT.H_SCROLL);

    tree.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            if (tree.getSelectionCount() != 1) {
              return;
            }
            TreeItem item = tree.getSelection()[0];
            if (item.getData() instanceof FunctionDescription description) {
              message.setText(OpenFormulaFunctionCatalog.htmlReport(description));
            }
          }
        });
    tree.addListener(
        SWT.MouseDoubleClick,
        event -> {
          if (tree.getSelectionCount() != 1) {
            return;
          }
          TreeItem item = tree.getSelection()[0];
          if (item.getParentItem() == null) {
            return;
          }
          String partToInsert;
          if (FIELDS_NODE.equals(item.getParentItem().getText())) {
            partToInsert = "[" + item.getText() + "]";
          } else if (item.getData() instanceof FunctionDescription description) {
            partToInsert = OpenFormulaFunctionCatalog.syntax(description);
          } else {
            partToInsert = item.getText();
          }
          expressionEditor.insert(partToInsert);
        });

    rightSash.setWeights(new int[] {40, 60});
    sashForm.setWeights(new int[] {15, 85});

    shell.setSize(DEFAULT_WIDTH, DEFAULT_HEIGHT);
    BaseTransformDialog.setSize(shell, -1, -1);
    shell.open();
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }
    return formula;
  }

  public void ok() {
    formula = expressionEditor.getText();
    dispose();
  }

  public void cancel() {
    formula = null;
    dispose();
  }

  private void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    PropsUi props = PropsUi.getInstance();
    props.setScreen(winprop);
    shell.dispose();
  }
}
