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

package org.apache.hop.pipeline.transforms.regexeval;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog to test a regular expression */
public class RegexEvalHelperDialog extends Dialog {
  private static final Class<?> PKG = RegexEvalMeta.class;

  private final IVariables variables;
  private Shell shell;
  private String regexScript;
  private final String regexOptions;
  private final boolean canonicalEqualityFlagSet;

  private StyledTextComp wRegExScript;

  private Label wIconValue1;
  private Text wValue1;

  private Label wIconValue2;
  private Text wValue2;

  private Label wIconValue3;
  private Text wValue3;

  private Text wRegExScriptCompile;

  private List wGroups;
  private Label wlGroups;

  private Label wIconValueGroup;
  private Text wValueGroup;

  private boolean errorDisplayed;

  /**
   * Dialog to allow someone to test regular expression
   *
   * @param parent The parent shell to use
   * @param regexScript The expression to test
   * @param regexOptions Any extended options for the regular expression
   * @param canonicalEqualityFlagSet canonical equality choice
   */
  public RegexEvalHelperDialog(
      Shell parent,
      IVariables variables,
      String regexScript,
      String regexOptions,
      boolean canonicalEqualityFlagSet) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.regexScript = regexScript;
    this.regexOptions = regexOptions;
    this.errorDisplayed = false;
    this.canonicalEqualityFlagSet = canonicalEqualityFlagSet;
  }

  private boolean isCanonicalEqualityFlagSet() {
    return this.canonicalEqualityFlagSet;
  }

  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Shell.Label"));

    int margin = PropsUi.getMargin();
    int middle = 30;

    // Some buttons at the bottom

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // RegEx Script
    Label wlRegExScript = new Label(shell, SWT.LEFT);
    wlRegExScript.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Script.Label"));
    PropsUi.setLook(wlRegExScript);
    FormData fdlRegExScript = new FormData();
    fdlRegExScript.left = new FormAttachment(0, 0);
    fdlRegExScript.right = new FormAttachment(100, 0);
    fdlRegExScript.top = new FormAttachment(0, margin);
    wlRegExScript.setLayoutData(fdlRegExScript);

    wRegExScript =
        new StyledTextComp(
            variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wRegExScript);
    FormData fdRegExScript = new FormData();
    fdRegExScript.left = new FormAttachment(0, 0);
    fdRegExScript.top = new FormAttachment(wlRegExScript, margin);
    fdRegExScript.right = new FormAttachment(100, -margin);
    fdRegExScript.bottom = new FormAttachment(40, -margin);
    wRegExScript.setLayoutData(fdRegExScript);
    wRegExScript.setFont(GuiResource.getInstance().getFontFixed());

    wRegExScriptCompile = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
    PropsUi.setLook(wRegExScriptCompile, Props.WIDGET_STYLE_FIXED);
    FormData fdRegExScriptCompile = new FormData();
    fdRegExScriptCompile.left = new FormAttachment(0, 0);
    fdRegExScriptCompile.top = new FormAttachment(wRegExScript, margin);
    fdRegExScriptCompile.right = new FormAttachment(100, 0);
    wRegExScriptCompile.setLayoutData(fdRegExScriptCompile);
    wRegExScriptCompile.setEditable(false);
    wRegExScriptCompile.setFont(GuiResource.getInstance().getFontNote());

    // ////////////////////////
    // START OF Values GROUP
    //

    Group wValuesGroup = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wValuesGroup);
    wValuesGroup.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.TestValues.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wValuesGroup.setLayout(groupLayout);

    // Value1
    Label wlValue1 = new Label(wValuesGroup, SWT.RIGHT);
    wlValue1.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Value1.Label"));
    PropsUi.setLook(wlValue1);
    FormData fdlValue1 = new FormData();
    fdlValue1.left = new FormAttachment(0, 0);
    fdlValue1.top = new FormAttachment(wRegExScriptCompile, margin);
    fdlValue1.right = new FormAttachment(middle, -margin);
    wlValue1.setLayoutData(fdlValue1);

    wIconValue1 = new Label(wValuesGroup, SWT.RIGHT);
    wIconValue1.setImage(GuiResource.getInstance().getImageEdit());
    PropsUi.setLook(wIconValue1);
    FormData fdlIconValue1 = new FormData();
    fdlIconValue1.top = new FormAttachment(wRegExScriptCompile, margin);
    fdlIconValue1.right = new FormAttachment(100, 0);
    wIconValue1.setLayoutData(fdlIconValue1);

    wValue1 = new Text(wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wValue1);
    FormData fdValue1 = new FormData();
    fdValue1.left = new FormAttachment(middle, margin);
    fdValue1.top = new FormAttachment(wRegExScriptCompile, margin);
    fdValue1.right = new FormAttachment(wIconValue1, -margin);
    wValue1.setLayoutData(fdValue1);

    // Value2
    Label wlValue2 = new Label(wValuesGroup, SWT.RIGHT);
    wlValue2.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Value2.Label"));
    PropsUi.setLook(wlValue2);
    FormData fdlValue2 = new FormData();
    fdlValue2.left = new FormAttachment(0, 0);
    fdlValue2.top = new FormAttachment(wValue1, margin);
    fdlValue2.right = new FormAttachment(middle, -margin);
    wlValue2.setLayoutData(fdlValue2);

    wIconValue2 = new Label(wValuesGroup, SWT.RIGHT);
    wIconValue2.setImage(GuiResource.getInstance().getImageEdit());
    PropsUi.setLook(wIconValue2);
    FormData fdlIconValue2 = new FormData();
    fdlIconValue2.top = new FormAttachment(wValue1, margin);
    fdlIconValue2.right = new FormAttachment(100, 0);
    wIconValue2.setLayoutData(fdlIconValue2);

    wValue2 = new Text(wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wValue2);
    FormData fdValue2 = new FormData();
    fdValue2.left = new FormAttachment(middle, margin);
    fdValue2.top = new FormAttachment(wValue1, margin);
    fdValue2.right = new FormAttachment(wIconValue2, -margin);
    wValue2.setLayoutData(fdValue2);

    // Value3
    Label wlValue3 = new Label(wValuesGroup, SWT.RIGHT);
    wlValue3.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Value3.Label"));
    PropsUi.setLook(wlValue3);
    FormData fdlValue3 = new FormData();
    fdlValue3.left = new FormAttachment(0, 0);
    fdlValue3.top = new FormAttachment(wValue2, margin);
    fdlValue3.right = new FormAttachment(middle, -margin);
    wlValue3.setLayoutData(fdlValue3);

    wIconValue3 = new Label(wValuesGroup, SWT.RIGHT);
    wIconValue3.setImage(GuiResource.getInstance().getImageEdit());
    PropsUi.setLook(wIconValue3);
    FormData fdlIconValue3 = new FormData();
    fdlIconValue3.top = new FormAttachment(wValue2, margin);
    fdlIconValue3.right = new FormAttachment(100, 0);
    wIconValue3.setLayoutData(fdlIconValue3);

    wValue3 = new Text(wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wValue3);
    FormData fdValue3 = new FormData();
    fdValue3.left = new FormAttachment(middle, margin);
    fdValue3.top = new FormAttachment(wValue2, margin);
    fdValue3.right = new FormAttachment(wIconValue3, -margin);
    wValue3.setLayoutData(fdValue3);

    FormData fdValuesGroup = new FormData();
    fdValuesGroup.left = new FormAttachment(0, 0);
    fdValuesGroup.top = new FormAttachment(wRegExScriptCompile, margin);
    fdValuesGroup.right = new FormAttachment(100, 0);
    wValuesGroup.setLayoutData(fdValuesGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF VALUES GROUP

    // ////////////////////////
    // START OF Values GROUP
    //

    Group wCaptureGroups = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wCaptureGroups);
    wCaptureGroups.setText("Capture");
    FormLayout captureLayout = new FormLayout();
    captureLayout.marginWidth = 10;
    captureLayout.marginHeight = 10;
    wCaptureGroups.setLayout(captureLayout);

    // ValueGroup
    Label wlValueGroup = new Label(wCaptureGroups, SWT.RIGHT);
    wlValueGroup.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.ValueGroup.Label"));
    PropsUi.setLook(wlValueGroup);
    FormData fdlValueGroup = new FormData();
    fdlValueGroup.left = new FormAttachment(0, 0);
    fdlValueGroup.top = new FormAttachment(wValuesGroup, margin);
    fdlValueGroup.right = new FormAttachment(middle, -margin);
    wlValueGroup.setLayoutData(fdlValueGroup);

    wIconValueGroup = new Label(wCaptureGroups, SWT.RIGHT);
    wIconValueGroup.setImage(GuiResource.getInstance().getImageEdit());
    PropsUi.setLook(wIconValueGroup);
    FormData fdlIconValueGroup = new FormData();
    fdlIconValueGroup.top = new FormAttachment(wValuesGroup, margin);
    fdlIconValueGroup.right = new FormAttachment(100, 0);
    wIconValueGroup.setLayoutData(fdlIconValueGroup);

    wValueGroup = new Text(wCaptureGroups, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wValueGroup);
    FormData fdValueGroup = new FormData();
    fdValueGroup.left = new FormAttachment(middle, margin);
    fdValueGroup.top = new FormAttachment(wValuesGroup, margin);
    fdValueGroup.right = new FormAttachment(wIconValueGroup, -margin);
    wValueGroup.setLayoutData(fdValueGroup);

    wlGroups = new Label(wCaptureGroups, SWT.RIGHT);
    wlGroups.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.GroupFields.Label"));
    PropsUi.setLook(wlGroups);
    FormData fdlGroups = new FormData();
    fdlGroups.left = new FormAttachment(0, 0);
    fdlGroups.top = new FormAttachment(wValueGroup, margin);
    fdlGroups.right = new FormAttachment(middle, -margin);
    wlGroups.setLayoutData(fdlGroups);
    wGroups =
        new List(wCaptureGroups, SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL | SWT.SINGLE);
    PropsUi.setLook(wGroups);
    FormData fdGroups = new FormData();
    fdGroups.left = new FormAttachment(middle, margin);
    fdGroups.top = new FormAttachment(wValueGroup, margin);
    fdGroups.right = new FormAttachment(100, -margin - ConstUi.SMALL_ICON_SIZE);
    fdGroups.bottom = new FormAttachment(100, -margin);
    wGroups.setLayoutData(fdGroups);

    FormData fdCaptureGroups = new FormData();
    fdCaptureGroups.left = new FormAttachment(0, 0);
    fdCaptureGroups.top = new FormAttachment(wValuesGroup, margin);
    fdCaptureGroups.right = new FormAttachment(100, 0);
    fdCaptureGroups.bottom = new FormAttachment(wOk, -margin);
    wCaptureGroups.setLayoutData(fdCaptureGroups);

    // ///////////////////////////////////////////////////////////
    // / END OF VALUES GROUP
    // ///////////////////////////////////////////////////////////

    // Add listeners
    wValue1.addModifyListener(e -> testValue(1, true, null));
    wValue2.addModifyListener(e -> testValue(2, true, null));
    wValue3.addModifyListener(e -> testValue(3, true, null));
    wValueGroup.addModifyListener(e -> testValue(4, true, null));
    wRegExScript.addModifyListener(
        e -> {
          errorDisplayed = false;
          testValues();
        });

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return regexScript;
  }

  private void testValues() {
    String realScript = variables.resolve(wRegExScript.getText());

    for (int i = 1; i < 5; i++) {
      testValue(i, false, realScript);
    }
  }

  private void testValue(int index, boolean testRegEx, String regExString) {
    String realScript = regExString;
    if (realScript == null) {
      realScript = variables.resolve(wRegExScript.getText());
    }

    if (Utils.isEmpty(realScript)) {
      if (testRegEx) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(BaseMessages.getString(PKG, "RegexEvalHelperDialog.EnterScript.Message"));
        mb.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.EnterScript.Title"));
        mb.open();
      }
      return;
    }

    String realValue;
    Label control;
    switch (index) {
      case 1:
        realValue = Const.NVL(variables.resolve(wValue1.getText()), "");
        control = wIconValue1;
        break;
      case 2:
        realValue = Const.NVL(variables.resolve(wValue2.getText()), "");
        control = wIconValue2;
        break;
      case 3:
        realValue = Const.NVL(variables.resolve(wValue3.getText()), "");
        control = wIconValue3;
        break;
      case 4:
        realValue = Const.NVL(variables.resolve(wValueGroup.getText()), "");
        control = wIconValueGroup;
        break;
      default:
        return;
    }

    try {
      Pattern p;
      if (isCanonicalEqualityFlagSet()) {
        p = Pattern.compile(regexOptions + realScript, Pattern.CANON_EQ);
      } else {
        p = Pattern.compile(regexOptions + realScript);
      }

      Matcher m = p.matcher(Const.NVL(realValue, ""));
      boolean isMatch = m.matches();

      if (Utils.isEmpty(realValue)) {
        control.setImage(GuiResource.getInstance().getImageEdit());
      } else if (isMatch) {
        control.setImage(GuiResource.getInstance().getImageTrue());
      } else {
        control.setImage(GuiResource.getInstance().getImageFalse());
      }

      if (index == 4) {
        wGroups.removeAll();
        int nrFields = m.groupCount();
        int nr = 0;
        for (int i = 1; i <= nrFields; i++) {
          if (m.group(i) == null) {
            wGroups.add("");
          } else {
            wGroups.add(m.group(i));
          }
          nr++;
        }
        wlGroups.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.FieldsGroup", nr));
      }
      wRegExScriptCompile.setForeground(GuiResource.getInstance().getColorDarkGreen());
      wRegExScriptCompile.setText(
          BaseMessages.getString(PKG, "RegexEvalHelperDialog.ScriptSuccessfullyCompiled"));
      wRegExScriptCompile.setToolTipText("");
    } catch (Exception e) {
      if (!errorDisplayed) {
        wRegExScriptCompile.setForeground(GuiResource.getInstance().getColorDarkRed());
        wRegExScriptCompile.setText(e.getMessage());
        wRegExScriptCompile.setToolTipText(
            BaseMessages.getString(PKG, "RegexEvalHelperDialog.ErrorCompiling.Message")
                + Const.CR
                + e);
        this.errorDisplayed = true;
      }
    }
  }

  public void dispose() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    if (regexScript != null) {
      wRegExScript.setText(regexScript);
    }
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    if (wRegExScript.getText() != null) {
      regexScript = wRegExScript.getText();
    }

    dispose();
  }
}
