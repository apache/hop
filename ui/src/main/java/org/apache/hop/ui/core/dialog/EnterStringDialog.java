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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to enter a (single line) String.
 *
 * @author Matt
 * @since 21-11-2004
 */
public class EnterStringDialog extends Dialog {
  private static final Class<?> PKG = EnterStringDialog.class; // For Translator

  private Text wString;

  private TextVar wStringVar;

  private final IVariables variables;

  private boolean allowVariables;

  private Button wOk;

  private Shell shell;

  private String string;

  private String shellText;

  private String lineText;

  private PropsUi props;

  private boolean mandatory;

  private char echoChar = 0;

  /**
   * This constructs without allowing for variable substitution. This constructor allows for
   * backwards compatibility for objects that wish to create this object without variable
   * substitution.
   *
   * @param parent Parent gui object
   * @param string The string to display in the dialog
   * @param shellText
   * @param lineText
   */
  public EnterStringDialog(Shell parent, String string, String shellText, String lineText) {
    this(parent, string, shellText, lineText, false, null);
  }

  /**
   * Constructs with the ability to use environmental variable substitution.
   *
   * @param parent Parent gui object
   * @param string The string to display in the dialog
   * @param shellText
   * @param lineText
   * @param allowVariables Indicates to allow environmental substitution
   * @param variables This object has the has the environmental variables
   */
  public EnterStringDialog(
      Shell parent,
      String string,
      String shellText,
      String lineText,
      boolean allowVariables,
      IVariables variables) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.string = string;
    this.shellText = shellText;
    this.lineText = lineText;
    this.allowVariables = allowVariables;
    this.variables = variables;
  }

  public String open() {
    Shell parent = getParent();
    Control lastControl;

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(shellText);

    int margin = props.getMargin();

    // The String line...
    Label wlString = new Label(shell, SWT.NONE);
    wlString.setText(lineText);
    props.setLook(wlString);
    FormData fdlString = new FormData();
    fdlString.left = new FormAttachment(0, 0);
    fdlString.top = new FormAttachment(0, margin);
    wlString.setLayoutData(fdlString);
    if (allowVariables) {
      wStringVar = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      wStringVar.setText(string);
      props.setLook(wStringVar);
      lastControl = wStringVar;
    } else {
      wString = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      wString.setText(string);
      props.setLook(wString);
      lastControl = wString;
    }

    if (echoChar != 0) {
      wString.setEchoChar(echoChar);
    }

    FormData fdString = new FormData();
    fdString.left = new FormAttachment(0, 0);
    fdString.top = new FormAttachment(wlString, margin);
    fdString.right = new FormAttachment(100, -margin);

    if (allowVariables) {
      wStringVar.setLayoutData(fdString);
      wStringVar.addModifyListener(arg0 -> setFlags());
    } else {
      wString.setLayoutData(fdString);
      wString.addModifyListener(arg0 -> setFlags());
    }

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, lastControl);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return string;
  }

  protected void setFlags() {
    String string = (allowVariables ? wStringVar.getText() : wString.getText());
    boolean enabled = !mandatory || !Utils.isEmpty(string);
    wOk.setEnabled(enabled);
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    if (allowVariables) {
      wStringVar.setText(Const.NVL(string, ""));
      wStringVar.selectAll();
    } else {
      wString.setText(Const.NVL(string, ""));
      wString.selectAll();
    }

    setFlags();
  }

  private void cancel() {
    string = null;
    dispose();
  }

  private void ok() {
    string = (allowVariables ? wStringVar.getText() : wString.getText());
    dispose();
  }

  /** @return the manditory */
  @Deprecated
  public boolean isManditory() {
    return this.isMandatory();
  }

  /** @return the mandatory */
  public boolean isMandatory() {
    return mandatory;
  }

  /** @param manditory the manditory to set */
  @Deprecated
  public void setManditory(boolean manditory) {
    this.setMandatory(manditory);
  }

  /** @param mandatory the manditory to set */
  public void setMandatory(boolean mandatory) {
    this.mandatory = mandatory;
  }

  /**
   * Gets echoChar
   *
   * @return value of echoChar
   */
  public char getEchoChar() {
    return echoChar;
  }

  /** @param echoChar The echoChar to set */
  public void setEchoChar(char echoChar) {
    this.echoChar = echoChar;
  }
}
