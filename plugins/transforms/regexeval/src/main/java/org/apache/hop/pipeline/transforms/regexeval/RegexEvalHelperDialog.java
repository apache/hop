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

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Dialog to test a regular expression */
public class RegexEvalHelperDialog extends Dialog {
  private static final Class<?> PKG = RegexEvalMeta.class; // For Translator

  private final IVariables variables;
  private Shell shell;
  private final PropsUi props;
  private String regexScript;
  private final String regexOptions;
  private final boolean canonicalEqualityFlagSet;

  private StyledTextComp wRegExScript;

  private Text wValue1;

  private Text wValue2;

  private Text wValue3;

  private Text wRegExScriptCompile;

  private List wGroups;
  private Label wlGroups;

  private Text wValueGroup;

  GuiResource guiresource = GuiResource.getInstance();

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
    props = PropsUi.getInstance();
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

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.NONE);
    props.setLook(shell);
    shell.setImage(guiresource.getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Shell.Label"));

    int margin = props.getMargin();
    int middle = 30;

    // Some buttons at the bottom

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    CTabFolder wNoteFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wNoteFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF NOTE CONTENT TAB///
    // /
    CTabItem wNoteContentTab = new CTabItem(wNoteFolder, SWT.NONE);
    wNoteContentTab.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.RegExTab.Label"));
    Composite wNoteContentComp = new Composite(wNoteFolder, SWT.NONE);
    props.setLook(wNoteContentComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wNoteContentComp.setLayout(fileLayout);

    // RegEx Script
    Label wlRegExScript = new Label(wNoteContentComp, SWT.RIGHT);
    wlRegExScript.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Script.Label"));
    props.setLook(wlRegExScript);
    FormData fdlRegExScript = new FormData();
    fdlRegExScript.left = new FormAttachment(0, 0);
    fdlRegExScript.top = new FormAttachment(0, 2 * margin);
    wlRegExScript.setLayoutData(fdlRegExScript);

    wRegExScript =
        new StyledTextComp(
            variables,
            wNoteContentComp,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wRegExScript.setText("");
    props.setLook(wRegExScript, Props.WIDGET_STYLE_FIXED);
    props.setLook(wRegExScript);
    FormData fdRegExScript = new FormData();
    fdRegExScript.left = new FormAttachment(0, 0);
    fdRegExScript.top = new FormAttachment(wlRegExScript, 2 * margin);
    fdRegExScript.right = new FormAttachment(100, -2 * margin);
    fdRegExScript.bottom = new FormAttachment(40, -margin);
    wRegExScript.setLayoutData(fdRegExScript);

    wRegExScriptCompile =
        new Text(wNoteContentComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
    wRegExScriptCompile.setText("");
    props.setLook(wRegExScriptCompile, Props.WIDGET_STYLE_FIXED);
    FormData fdRegExScriptCompile = new FormData();
    fdRegExScriptCompile.left = new FormAttachment(0, 0);
    fdRegExScriptCompile.top = new FormAttachment(wRegExScript, margin);
    fdRegExScriptCompile.right = new FormAttachment(100, 0);
    wRegExScriptCompile.setLayoutData(fdRegExScriptCompile);
    wRegExScriptCompile.setEditable(false);
    wRegExScriptCompile.setFont(guiresource.getFontNote());

    // ////////////////////////
    // START OF Values GROUP
    //

    Group wValuesGroup = new Group(wNoteContentComp, SWT.SHADOW_NONE);
    props.setLook(wValuesGroup);
    wValuesGroup.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.TestValues.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wValuesGroup.setLayout(groupLayout);

    // Value1
    Label wlValue1 = new Label(wValuesGroup, SWT.RIGHT);
    wlValue1.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Value1.Label"));
    props.setLook(wlValue1);
    FormData fdlValue1 = new FormData();
    fdlValue1.left = new FormAttachment(0, 0);
    fdlValue1.top = new FormAttachment(wRegExScriptCompile, margin);
    fdlValue1.right = new FormAttachment(middle, -margin);
    wlValue1.setLayoutData(fdlValue1);
    wValue1 = new Text(wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wValue1);
    FormData fdValue1 = new FormData();
    fdValue1.left = new FormAttachment(middle, margin);
    fdValue1.top = new FormAttachment(wRegExScriptCompile, margin);
    fdValue1.right = new FormAttachment(100, -margin);
    wValue1.setLayoutData(fdValue1);

    // Value2
    Label wlValue2 = new Label(wValuesGroup, SWT.RIGHT);
    wlValue2.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Value2.Label"));
    props.setLook(wlValue2);
    FormData fdlValue2 = new FormData();
    fdlValue2.left = new FormAttachment(0, 0);
    fdlValue2.top = new FormAttachment(wValue1, margin);
    fdlValue2.right = new FormAttachment(middle, -margin);
    wlValue2.setLayoutData(fdlValue2);
    wValue2 = new Text(wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wValue2);
    FormData fdValue2 = new FormData();
    fdValue2.left = new FormAttachment(middle, margin);
    fdValue2.top = new FormAttachment(wValue1, margin);
    fdValue2.right = new FormAttachment(100, -margin);
    wValue2.setLayoutData(fdValue2);

    // Value3
    Label wlValue3 = new Label(wValuesGroup, SWT.RIGHT);
    wlValue3.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.Value3.Label"));
    props.setLook(wlValue3);
    FormData fdlValue3 = new FormData();
    fdlValue3.left = new FormAttachment(0, 0);
    fdlValue3.top = new FormAttachment(wValue2, margin);
    fdlValue3.right = new FormAttachment(middle, -margin);
    wlValue3.setLayoutData(fdlValue3);
    wValue3 = new Text(wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wValue3);
    FormData fdValue3 = new FormData();
    fdValue3.left = new FormAttachment(middle, margin);
    fdValue3.top = new FormAttachment(wValue2, margin);
    fdValue3.right = new FormAttachment(100, -margin);
    wValue3.setLayoutData(fdValue3);

    FormData fdValuesGroup = new FormData();
    fdValuesGroup.left = new FormAttachment(0, margin);
    fdValuesGroup.top = new FormAttachment(wRegExScriptCompile, margin);
    fdValuesGroup.right = new FormAttachment(100, -margin);
    wValuesGroup.setLayoutData(fdValuesGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF VALUES GROUP

    // ////////////////////////
    // START OF Values GROUP
    //

    Group wCaptureGroups = new Group(wNoteContentComp, SWT.SHADOW_NONE);
    props.setLook(wCaptureGroups);
    wCaptureGroups.setText("Capture");
    FormLayout captureLayout = new FormLayout();
    captureLayout.marginWidth = 10;
    captureLayout.marginHeight = 10;
    wCaptureGroups.setLayout(captureLayout);

    // ValueGroup
    Label wlValueGroup = new Label(wCaptureGroups, SWT.RIGHT);
    wlValueGroup.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.ValueGroup.Label"));
    props.setLook(wlValueGroup);
    FormData fdlValueGroup = new FormData();
    fdlValueGroup.left = new FormAttachment(0, 0);
    fdlValueGroup.top = new FormAttachment(wValuesGroup, margin);
    fdlValueGroup.right = new FormAttachment(middle, -margin);
    wlValueGroup.setLayoutData(fdlValueGroup);
    wValueGroup = new Text(wCaptureGroups, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wValueGroup);
    FormData fdValueGroup = new FormData();
    fdValueGroup.left = new FormAttachment(middle, margin);
    fdValueGroup.top = new FormAttachment(wValuesGroup, margin);
    fdValueGroup.right = new FormAttachment(100, -margin);
    wValueGroup.setLayoutData(fdValueGroup);

    wlGroups = new Label(wCaptureGroups, SWT.RIGHT);
    wlGroups.setText(BaseMessages.getString(PKG, "RegexEvalHelperDialog.GroupFields.Label"));
    props.setLook(wlGroups);
    FormData fdlGroups = new FormData();
    fdlGroups.left = new FormAttachment(0, 0);
    fdlGroups.top = new FormAttachment(wValueGroup, margin);
    fdlGroups.right = new FormAttachment(middle, -margin);
    wlGroups.setLayoutData(fdlGroups);
    wGroups =
        new List(wCaptureGroups, SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL | SWT.SINGLE);
    props.setLook(wValue3);
    FormData fdGroups = new FormData();
    fdGroups.left = new FormAttachment(middle, margin);
    fdGroups.top = new FormAttachment(wValueGroup, margin);
    fdGroups.right = new FormAttachment(100, -margin);
    fdGroups.bottom = new FormAttachment(100, -margin);
    wGroups.setLayoutData(fdGroups);

    FormData fdCaptureGroups = new FormData();
    fdCaptureGroups.left = new FormAttachment(0, margin);
    fdCaptureGroups.top = new FormAttachment(wValuesGroup, margin);
    fdCaptureGroups.right = new FormAttachment(100, -margin);
    fdCaptureGroups.bottom = new FormAttachment(100, -margin);
    wCaptureGroups.setLayoutData(fdCaptureGroups);

    // ///////////////////////////////////////////////////////////
    // / END OF VALUES GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdNoteContentComp = new FormData();
    fdNoteContentComp.left = new FormAttachment(0, 0);
    fdNoteContentComp.top = new FormAttachment(0, 0);
    fdNoteContentComp.right = new FormAttachment(100, 0);
    fdNoteContentComp.bottom = new FormAttachment(100, 0);
    wNoteContentComp.setLayoutData(fdNoteContentComp);
    wNoteContentComp.layout();
    wNoteContentTab.setControl(wNoteContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF NOTE CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////

    FormData fdNoteFolder = new FormData();
    fdNoteFolder.left = new FormAttachment(0, 0);
    fdNoteFolder.top = new FormAttachment(0, margin);
    fdNoteFolder.right = new FormAttachment(100, 0);
    fdNoteFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wNoteFolder.setLayoutData(fdNoteFolder);

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

    wNoteFolder.setSelection(0);

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
    Text control;
    switch (index) {
      case 1:
        realValue = Const.NVL(variables.resolve(wValue1.getText()), "");
        control = wValue1;
        break;
      case 2:
        realValue = Const.NVL(variables.resolve(wValue2.getText()), "");
        control = wValue2;
        break;
      case 3:
        realValue = Const.NVL(variables.resolve(wValue3.getText()), "");
        control = wValue3;
        break;
      case 4:
        realValue = Const.NVL(variables.resolve(wValueGroup.getText()), "");
        control = wValueGroup;
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
      if (isMatch) {
        control.setBackground(guiresource.getColorGreen());
      } else {
        control.setBackground(guiresource.getColorRed());
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
      wRegExScriptCompile.setForeground(guiresource.getColorBlue());
      wRegExScriptCompile.setText(
          BaseMessages.getString(PKG, "RegexEvalHelperDialog.ScriptSuccessfullyCompiled"));
      wRegExScriptCompile.setToolTipText("");
    } catch (Exception e) {
      if (!errorDisplayed) {
        wRegExScriptCompile.setForeground(guiresource.getColorRed());
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
    props.setScreen(new WindowProperty(shell));
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
