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

package org.apache.hop.pipeline.transforms.janino.editor;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.janino.JaninoMeta;
import org.apache.hop.pipeline.transforms.janino.function.FunctionDescription;
import org.apache.hop.pipeline.transforms.janino.function.FunctionLib;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

public class FormulaEditor extends Dialog implements KeyListener {
  public static final Class<?> PKG = JaninoMeta.class;

  private Shell shell;
  private Tree tree;
  private SashForm sashForm;
  private StyledTextComp expressionEditor;
  private String formula;
  private Browser message;
  private FunctionLib functionLib;

  private Button ok;
  private Button cancel;

  private Color blue;
  private Color red;
  private Color green;
  private Color white;
  private Color gray;
  private Color black;

  Menu helperMenu;

  private String[] categories;
  private SashForm rightSash;

  public FormulaEditor(
      IVariables variables, Shell parent, int style, String formula, List<String> inputFields)
      throws HopException {
    super(parent, style);
    this.formula = formula;

    // Run it in a new shell:
    //
    shell =
        new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX | SWT.APPLICATION_MODAL);

    // The layout...
    //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    shell.setLayout(formLayout);

    // At the bottom we have a few buttons...
    //
    Composite buttonsComposite = new Composite(shell, SWT.NONE);
    FillLayout bcLayout = new FillLayout();
    bcLayout.spacing = 5;
    buttonsComposite.setLayout(bcLayout);
    ok = new Button(buttonsComposite, SWT.PUSH);
    ok.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    cancel = new Button(buttonsComposite, SWT.PUSH);
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

    // A tree on the left, an editor on the right: put them in a sash form...
    // Right below the editor we display the error messages...
    //
    sashForm = new SashForm(shell, SWT.HORIZONTAL);
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

    // Read the function descriptions...
    //
    readFunctions();

    // A tree on the left:
    //
    tree = new Tree(sashForm, SWT.SINGLE);
    TreeItem fieldsItem = new TreeItem(tree, SWT.NONE);
    fieldsItem.setText("Fields");

    for (String inputField : inputFields) {
      TreeItem fieldItem = new TreeItem(fieldsItem, SWT.NONE);
      fieldItem.setText(inputField);
    }

    for (String category : categories) {
      String i18nCategory = category;
      // Look up the category in i18n if needed.
      if (category.startsWith("%")) {
        i18nCategory = BaseMessages.getString(PKG, category.substring(1)); // skip the %
      }

      TreeItem item = new TreeItem(tree, SWT.NONE);
      item.setText(i18nCategory);

      String[] fnames = functionLib.getFunctionsForACategory(category);
      for (String fname : fnames) {
        TreeItem fitem = new TreeItem(item, SWT.NONE);
        fitem.setText(fname);
      }
    }
    /*
     * If someone clicks on a function, we display the description of the function in the message
     * box...
     */
    tree.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            if (tree.getSelectionCount() == 1) {
              TreeItem item = tree.getSelection()[0];
              if (item.getParentItem() != null) { // has a category above it
                String functionName = item.getText();
                FunctionDescription functionDescription =
                    functionLib.getFunctionDescription(functionName);
                if (functionDescription != null) {
                  String report = functionDescription.getHtmlReport();
                  message.setText(report);
                }
              }
            }
          }
        });

    /*
     * DoubleClick Listener
     */
    tree.addListener(
        SWT.MouseDoubleClick,
        event -> {
          if (tree.getSelectionCount() == 1) {
            TreeItem item = tree.getSelection()[0];
            if (item.getParentItem() != null) {
              String partToInsert = "";

              // If it's a field we need the fieldname if it's a formula we take the syntax for that
              // formula
              if (item.getParentItem().getText().equals("Fields")) {
                partToInsert = item.getText();
              } else {
                partToInsert =
                    (functionLib.getFunctionDescription(item.getText()).getSyntax() == null)
                        ? item.getText()
                        : functionLib.getFunctionDescription(item.getText()).getSyntax();
              }

              String oldFormula = expressionEditor.getText();
              int caretPosition = expressionEditor.getCaretPosition();
              int textLength = expressionEditor.getText().length();
              int selectionsize = expressionEditor.getSelectionCount();

              // No text in editor yet, just add text
              if (textLength == 0) {
                expressionEditor.setText(partToInsert);
              } else if (textLength == caretPosition) {
                // we are at the end of the text, append new text
                expressionEditor.setText(oldFormula + partToInsert);
              } else {
                // Adding text somewhere between other text
                // if selectionsize is > 0 then we are writing over other text
                expressionEditor.setText(
                    oldFormula.substring(0, caretPosition)
                        + partToInsert
                        + oldFormula.substring(caretPosition + selectionsize));
              }
            }
          }
        });

    rightSash = new SashForm(sashForm, SWT.VERTICAL);

    // An expression editor on the right
    //
    expressionEditor =
        new StyledTextComp(
            variables, rightSash, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    expressionEditor.setText(this.formula);
    expressionEditor.addModifyListener(event -> setStyles());
    expressionEditor.addKeyListener(this);

    // Some information concerning the validity of the formula expression
    //
    message = new Browser(rightSash, SWT.MULTI | SWT.READ_ONLY | SWT.V_SCROLL | SWT.H_SCROLL);
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(0, 0);
    fdMessage.right = new FormAttachment(100, 0);
    fdMessage.top = new FormAttachment(0, 0);
    fdMessage.bottom = new FormAttachment(0, 100);
    message.setLayoutData(fdMessage);

    rightSash.setWeights(new int[] {10, 80});
    sashForm.setWeights(new int[] {15, 85});

    red = new Color(shell.getDisplay(), 255, 0, 0);
    green = new Color(shell.getDisplay(), 0, 220, 0);
    blue = new Color(shell.getDisplay(), 0, 0, 255);
    white = new Color(shell.getDisplay(), 255, 255, 255);
    gray = new Color(shell.getDisplay(), 150, 150, 150);
    black = new Color(shell.getDisplay(), 0, 0, 0);

    setStyles();

    shell.addDisposeListener(
        arg0 -> {
          red.dispose();
          green.dispose();
          blue.dispose();
          white.dispose();
          gray.dispose();
          black.dispose();
        });
  }

  public String open() {
    shell.layout();
    shell.open();

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }
    return formula;
  }

  public void ok() {
    formula = expressionEditor.getText();
    shell.dispose();
  }

  public void cancel() {
    formula = null;
    shell.dispose();
  }

  public void readFunctions() throws HopException {
    functionLib = new FunctionLib();
    categories = functionLib.getFunctionCategories();
  }

  public void setStyles() {
    // implement later
  }

  public void keyPressed(KeyEvent e) {
    // implement later
  }

  @Override
  public void keyReleased(KeyEvent keyEvent) {
    // implement later
  }
}
