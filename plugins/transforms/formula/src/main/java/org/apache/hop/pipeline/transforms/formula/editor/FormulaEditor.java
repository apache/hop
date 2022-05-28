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

package org.apache.hop.pipeline.transforms.formula.editor;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.formula.FormulaMeta;
import org.apache.hop.pipeline.transforms.formula.function.FunctionDescription;
import org.apache.hop.pipeline.transforms.formula.function.FunctionLib;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class FormulaEditor extends Dialog implements KeyListener {
  public static final Class<?> PKG = FormulaMeta.class; // For Translator
  public static final String FUNCTIONS_FILE = "functions.xml";

  private Shell shell;
  private Tree tree;
  private SashForm sashForm;
  private StyledTextComp expressionEditor;
  private String formula;
  private Browser message;

  private Button ok;
  private Button cancel;
  private String[] inputFields;

  private Color blue;
  private Color red;
  private Color green;
  private Color white;
  private Color gray;
  private Color black;

  Menu helperMenu;

  private FunctionLib functionLib;
  private String[] functions;
  private String[] categories;

  private SashForm rightSash;
  private IVariables variables;

  public FormulaEditor(IVariables variables, Shell parent, int style, String formula, String[] inputFields)
      throws Exception {
    super(parent, style);
    this.variables = variables;
    this.formula = formula;
    this.inputFields = inputFields;

    // Run it in a new shell:
    //
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);

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
    ok.setText("  OK  "); // TODO i18n
    cancel = new Button(buttonsComposite, SWT.PUSH);
    cancel.setText(" Cancel "); // TODO i18n

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
    for (int i = 0; i < categories.length; i++) {
      String category = categories[i];
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
    /**
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

    rightSash = new SashForm(sashForm, SWT.VERTICAL);

    // An expression editor on the right
    //
    expressionEditor = new StyledTextComp(variables, rightSash, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
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

    rightSash.setWeights(
        new int[] {
          10, 80,
        });

    sashForm.setWeights(
        new int[] {
          15, 85,
        });

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

  public void readFunctions() throws Exception {
    functionLib = new FunctionLib(FUNCTIONS_FILE);
    functions = functionLib.getFunctionNames();
    categories = functionLib.getFunctionCategories();
  }

  // TODO
  public void setStyles() {
    // implement later
  }

  public static void main(String[] args) throws Exception {
    Display display = new Display();
    String[] inputFields = {
      "firstname", "name",
    };
    FormulaEditor lbe =
        new FormulaEditor(
                null,
            new Shell(display, SWT.NONE),
            SWT.NONE,
            "MID(UPPER([name] & \" \" & [firstname]);5;10)",
            inputFields);
    lbe.open();
  }

  public void keyPressed(KeyEvent e) {
    // implement later
  }

  @Override
  public void keyReleased(KeyEvent keyEvent) {
    // implement later
  }
}
