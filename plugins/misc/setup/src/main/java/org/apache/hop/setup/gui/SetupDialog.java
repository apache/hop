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

package org.apache.hop.setup.gui;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.setup.HopEnvironmentApplier;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopEnvironmentDefaults;
import org.apache.hop.setup.HopEnvironmentSnapshot;
import org.apache.hop.setup.HopEnvironmentSpec;
import org.apache.hop.setup.HopSetupVariables;
import org.apache.hop.setup.OsFamily;
import org.apache.hop.setup.UserPaths;
import org.apache.hop.setup.persist.HopInstallHome;
import org.apache.hop.setup.persist.HopVfsFiles;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Configure launcher environment variables for Hop. */
public class SetupDialog extends Dialog {

  private static final Class<?> PKG = SetupGuiPlugin.class;

  private final PropsUi props;
  private final IVariables variables;
  private final OsFamily os;
  private final UserPaths paths;
  private final boolean web;

  private HopEnvironmentSnapshot snapshot;
  private Shell shell;
  private Text wConfig;
  private Text wAudit;
  private Text wJavaHome;
  private Text wOptions;
  private Text wJdbc;
  private Button wCreateFolders;
  private Button wCopyExisting;
  private Button wUserEnv;
  private Button wShellRc;
  private Text wRcFile;
  private Button wScript;
  private Text wScriptFile;
  private Button wCreateDefaultProject;
  private Text wDefaultProjectHome;
  private Button wRegisterSamples;
  private Label wSamplesPath;

  public SetupDialog(Shell parent, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.variables = variables;
    this.props = PropsUi.getInstance();
    this.os = OsFamily.detect();
    this.paths = UserPaths.system();
    this.web = EnvironmentUtils.getInstance().isWeb();
  }

  public SetupDialog(Shell parent, IVariables variables, boolean ignoredPrefillDefaults) {
    this(parent, variables);
  }

  public void open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "SetupDialog.Shell.Title"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "setup.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    Button wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "SetupDialog.Preview"));
    wPreview.addListener(SWT.Selection, e -> preview());
    Button wApply = new Button(shell, SWT.PUSH);
    wApply.setText(BaseMessages.getString(PKG, "SetupDialog.Apply"));
    wApply.addListener(SWT.Selection, e -> apply(false));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wPreview, wApply, wCancel}, margin * 3, null);
    if (web) {
      wApply.setEnabled(false);
    }

    snapshot = HopEnvironmentSnapshot.capture(os, paths);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, margin);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wPreview, -margin * 2);
    wTabFolder.setLayoutData(fdTabs);

    addVariablesTab(wTabFolder, middle, margin);
    addLocationTab(wTabFolder, middle, margin);
    addProjectsTab(wTabFolder, middle, margin);
    wTabFolder.setSelection(0);

    populate(snapshot);

    BaseTransformDialog.setSize(shell);
    shell.open();
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }
  }

  private void addVariablesTab(CTabFolder folder, int middle, int margin) {
    Composite comp = addTab(folder, "SetupDialog.Tab.Variables");

    Label wIntro = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wIntro);
    wIntro.setText(
        BaseMessages.getString(PKG, web ? "SetupDialog.Web.ReadOnly" : "SetupDialog.Intro"));
    FormData fdIntro = new FormData();
    fdIntro.left = new FormAttachment(0, 0);
    fdIntro.right = new FormAttachment(100, 0);
    fdIntro.top = new FormAttachment(0, 0);
    wIntro.setLayoutData(fdIntro);

    Label wCurrent = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wCurrent);
    wCurrent.setText(
        BaseMessages.getString(
            PKG,
            snapshot.isConfigFolderFromEnv()
                ? "SetupDialog.Current.FromEnv"
                : "SetupDialog.Current.FromInstall"));
    FormData fdCurrent = new FormData();
    fdCurrent.left = new FormAttachment(0, 0);
    fdCurrent.right = new FormAttachment(100, 0);
    fdCurrent.top = new FormAttachment(wIntro, margin);
    wCurrent.setLayoutData(fdCurrent);
    Control last = wCurrent;

    wConfig = addPathField(comp, "SetupDialog.ConfigFolder.Label", last, middle, margin, true);
    last = wConfig;
    wAudit = addPathField(comp, "SetupDialog.AuditFolder.Label", last, middle, margin, true);
    last = wAudit;
    wJavaHome = addPathField(comp, "SetupDialog.JavaHome.Label", last, middle, margin, true);
    last = wJavaHome;
    wOptions = addPathField(comp, "SetupDialog.Options.Label", last, middle, margin, false);
    last = wOptions;
    wJdbc = addPathField(comp, "SetupDialog.JdbcFolders.Label", last, middle, margin, true);
    last = wJdbc;

    Button wRecommended = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wRecommended);
    wRecommended.setText(BaseMessages.getString(PKG, "SetupDialog.Recommended"));
    FormData fdRecommended = new FormData();
    fdRecommended.left = new FormAttachment(middle, margin);
    fdRecommended.top = new FormAttachment(last, margin);
    wRecommended.setLayoutData(fdRecommended);
    wRecommended.addListener(SWT.Selection, e -> fillRecommendedValues());

    Button wExisting = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wExisting);
    wExisting.setText(BaseMessages.getString(PKG, "SetupDialog.Existing"));
    FormData fdExisting = new FormData();
    fdExisting.left = new FormAttachment(wRecommended, margin);
    fdExisting.top = new FormAttachment(last, margin);
    wExisting.setLayoutData(fdExisting);
    wExisting.addListener(SWT.Selection, e -> fillExistingValues());
    last = wRecommended;

    wCreateFolders = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wCreateFolders);
    wCreateFolders.setText(BaseMessages.getString(PKG, "SetupDialog.CreateFolders"));
    wCreateFolders.setSelection(true);
    FormData fdCreate = new FormData();
    fdCreate.left = new FormAttachment(middle, margin);
    fdCreate.right = new FormAttachment(100, 0);
    fdCreate.top = new FormAttachment(last, margin);
    wCreateFolders.setLayoutData(fdCreate);
    last = wCreateFolders;

    wCopyExisting = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wCopyExisting);
    wCopyExisting.setText(BaseMessages.getString(PKG, "SetupDialog.CopyExisting"));
    wCopyExisting.setSelection(installConfigExists());
    FormData fdCopy = new FormData();
    fdCopy.left = new FormAttachment(middle, margin);
    fdCopy.right = new FormAttachment(100, 0);
    fdCopy.top = new FormAttachment(last, margin);
    wCopyExisting.setLayoutData(fdCopy);
  }

  private void addLocationTab(CTabFolder folder, int middle, int margin) {
    Composite comp = addTab(folder, "SetupDialog.Tab.Location");

    wUserEnv = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wUserEnv);
    wUserEnv.setText(BaseMessages.getString(PKG, "SetupDialog.UserEnv"));
    wUserEnv.setSelection(os.isWindows() && !web);
    wUserEnv.setEnabled(os.isWindows() && !web);
    FormData fdUserEnv = new FormData();
    fdUserEnv.left = new FormAttachment(middle, margin);
    fdUserEnv.right = new FormAttachment(100, 0);
    fdUserEnv.top = new FormAttachment(0, 0);
    wUserEnv.setLayoutData(fdUserEnv);
    Control last = wUserEnv;

    wShellRc = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wShellRc);
    wShellRc.setText(BaseMessages.getString(PKG, "SetupDialog.ShellRc"));
    boolean rcOk = !os.isWindows() && !web && HopEnvironmentDefaults.supportsShellRc(paths);
    wShellRc.setSelection(rcOk);
    wShellRc.setEnabled(!os.isWindows() && !web);
    FormData fdShellRc = new FormData();
    fdShellRc.left = new FormAttachment(middle, margin);
    fdShellRc.right = new FormAttachment(100, 0);
    fdShellRc.top = new FormAttachment(last, margin);
    wShellRc.setLayoutData(fdShellRc);
    last = wShellRc;

    wRcFile = addPathField(comp, "SetupDialog.RcFile.Label", last, middle, margin, true);
    last = wRcFile;
    wRcFile.setEnabled(!os.isWindows() && !web);

    wScript = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wScript);
    wScript.setText(BaseMessages.getString(PKG, "SetupDialog.Script"));
    wScript.setSelection(!web);
    wScript.setEnabled(!web);
    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment(middle, margin);
    fdScript.right = new FormAttachment(100, 0);
    fdScript.top = new FormAttachment(last, margin);
    wScript.setLayoutData(fdScript);
    last = wScript;

    wScriptFile = addPathField(comp, "SetupDialog.ScriptFile.Label", last, middle, margin, true);
  }

  private void addProjectsTab(CTabFolder folder, int middle, int margin) {
    Composite comp = addTab(folder, "SetupDialog.Tab.Projects");

    Label wIntro = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wIntro);
    wIntro.setText(BaseMessages.getString(PKG, "SetupDialog.Projects.Intro"));
    FormData fdIntro = new FormData();
    fdIntro.left = new FormAttachment(0, 0);
    fdIntro.right = new FormAttachment(100, 0);
    fdIntro.top = new FormAttachment(0, 0);
    wIntro.setLayoutData(fdIntro);

    wCreateDefaultProject = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wCreateDefaultProject);
    wCreateDefaultProject.setText(
        BaseMessages.getString(PKG, "SetupDialog.Projects.CreateDefault"));
    wCreateDefaultProject.setSelection(true);
    FormData fdCreate = new FormData();
    fdCreate.left = new FormAttachment(0, 0);
    fdCreate.right = new FormAttachment(100, 0);
    fdCreate.top = new FormAttachment(wIntro, margin * 2);
    wCreateDefaultProject.setLayoutData(fdCreate);

    wDefaultProjectHome =
        addPathField(
            comp,
            "SetupDialog.Projects.DefaultHome.Label",
            wCreateDefaultProject,
            middle,
            margin,
            true);
    wCreateDefaultProject.addListener(SWT.Selection, e -> updateProjectWidgets());

    Button wRecommendedProject = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wRecommendedProject);
    wRecommendedProject.setText(BaseMessages.getString(PKG, "SetupDialog.Projects.Recommended"));
    FormData fdRecommended = new FormData();
    fdRecommended.left = new FormAttachment(middle, margin);
    fdRecommended.top = new FormAttachment(wDefaultProjectHome, margin);
    wRecommendedProject.setLayoutData(fdRecommended);
    wRecommendedProject.addListener(
        SWT.Selection,
        e ->
            wDefaultProjectHome.setText(
                HopEnvironmentDefaults.recommendedDefaultProjectHome(os, paths)));

    wRegisterSamples = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wRegisterSamples);
    wRegisterSamples.setText(BaseMessages.getString(PKG, "SetupDialog.Projects.RegisterSamples"));
    wRegisterSamples.setSelection(true);
    FormData fdSamples = new FormData();
    fdSamples.left = new FormAttachment(0, 0);
    fdSamples.right = new FormAttachment(100, 0);
    fdSamples.top = new FormAttachment(wRecommendedProject, margin * 2);
    wRegisterSamples.setLayoutData(fdSamples);

    wSamplesPath = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wSamplesPath);
    FormData fdPath = new FormData();
    fdPath.left = new FormAttachment(middle, margin);
    fdPath.right = new FormAttachment(100, 0);
    fdPath.top = new FormAttachment(wRegisterSamples, margin);
    wSamplesPath.setLayoutData(fdPath);
  }

  private Composite addTab(CTabFolder folder, String titleKey) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    PropsUi.setLook(tab);
    tab.setText(BaseMessages.getString(PKG, titleKey));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);
    return comp;
  }

  private Text addPathField(
      Composite parent, String labelKey, Control last, int middle, int margin, boolean browse) {
    Label label = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, 0);
    fdl.top = new FormAttachment(last, margin);
    label.setLayoutData(fdl);

    Button browseButton = null;
    if (browse) {
      browseButton = new Button(parent, SWT.PUSH);
      PropsUi.setLook(browseButton);
      browseButton.setText(BaseMessages.getString(PKG, "SetupDialog.Browse"));
      FormData fdb = new FormData();
      fdb.right = new FormAttachment(100, 0);
      fdb.top = new FormAttachment(label, 0, SWT.CENTER);
      browseButton.setLayoutData(fdb);
    }

    Text text = new Text(parent, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(text);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, margin);
    fd.right =
        browseButton == null
            ? new FormAttachment(100, 0)
            : new FormAttachment(browseButton, -margin);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    text.setLayoutData(fd);
    if (browseButton != null) {
      browseButton.addListener(
          SWT.Selection,
          e -> {
            String dir = BaseDialog.presentDirectoryDialog(shell, text.getText(), null, variables);
            if (dir != null) {
              text.setText(dir);
            }
          });
    }
    return text;
  }

  private void populate(HopEnvironmentSnapshot current) {
    wJavaHome.setText(Const.NVL(current.getJavaHome(), ""));
    wOptions.setText(Const.NVL(current.getOptions(), ""));
    wJdbc.setText(Const.NVL(current.getJdbcFolders(), ""));
    wRcFile.setText(HopEnvironmentDefaults.recommendedShellRcFile(paths));
    wScriptFile.setText(HopEnvironmentDefaults.wellKnownEnvFile(os, paths));
    wDefaultProjectHome.setText(HopEnvironmentDefaults.recommendedDefaultProjectHome(os, paths));
    java.nio.file.Path hopHome = HopInstallHome.resolveOrNull();
    if (hopHome != null) {
      wSamplesPath.setText(hopHome.resolve("config/projects/samples").toString());
    } else {
      wSamplesPath.setText(BaseMessages.getString(PKG, "SetupDialog.Projects.SamplesMissing"));
      wRegisterSamples.setSelection(false);
      wRegisterSamples.setEnabled(false);
    }
    if (isFirstTime()) {
      fillRecommendedValues();
    } else {
      fillExistingValues();
    }
    updateProjectWidgets();
  }

  private void updateProjectWidgets() {
    boolean create = wCreateDefaultProject.getSelection();
    wDefaultProjectHome.setEnabled(create);
  }

  private static boolean isFirstTime() {
    return !HopConfig.readOptionBoolean(HopSetupVariables.CONFIG_OPTION_DO_NOT_SHOW, false);
  }

  private void fillRecommendedValues() {
    wConfig.setText(HopEnvironmentDefaults.recommendedConfigFolder(os, paths));
    wAudit.setText(HopEnvironmentDefaults.recommendedAuditFolder(os, paths));
  }

  private void fillExistingValues() {
    wConfig.setText(
        Const.NVL(snapshot.getConfigFolder(), HopEnvironmentDefaults.INSTALL_CONFIG_FOLDER));
    wAudit.setText(
        Const.NVL(snapshot.getAuditFolder(), HopEnvironmentDefaults.INSTALL_AUDIT_FOLDER));
    wJavaHome.setText(Const.NVL(snapshot.getJavaHome(), ""));
    wOptions.setText(Const.NVL(snapshot.getOptions(), ""));
    wJdbc.setText(Const.NVL(snapshot.getJdbcFolders(), ""));
  }

  private boolean installConfigExists() {
    try {
      java.nio.file.Path home = HopInstallHome.resolveOrNull();
      if (home == null) {
        return false;
      }
      return HopVfsFiles.exists(
          home.resolve("config").resolve(HopSetupVariables.HOP_CONFIG_JSON).toString());
    } catch (Exception e) {
      return false;
    }
  }

  private HopEnvironmentSpec toSpec(boolean dryRun) {
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(blankToEmpty(wConfig.getText()));
    spec.setAuditFolder(blankToEmpty(wAudit.getText()));
    spec.setJavaHome(blankToEmpty(wJavaHome.getText()));
    spec.setOptions(blankToEmpty(wOptions.getText()));
    spec.setJdbcFolders(blankToEmpty(wJdbc.getText()));
    spec.setWriteUserEnv(wUserEnv.getSelection());
    spec.setWriteShellRc(wShellRc.getSelection());
    spec.setShellRcFile(wRcFile.getText());
    spec.setWriteScript(wScript.getSelection());
    spec.setScriptFile(wScriptFile.getText());
    spec.setCreateFolders(wCreateFolders.getSelection());
    spec.setCopyExisting(wCopyExisting.getSelection());
    spec.setDryRun(dryRun);
    spec.setCreateDefaultProject(wCreateDefaultProject.getSelection());
    spec.setDefaultProjectHome(blankToEmpty(wDefaultProjectHome.getText()));
    spec.setRegisterSamples(wRegisterSamples.getSelection());
    return spec;
  }

  private static String blankToEmpty(String value) {
    return StringUtils.isBlank(value) ? "" : value.trim();
  }

  private void preview() {
    apply(true);
  }

  private void apply(boolean dryRun) {
    try {
      if (!os.isWindows()
          && wShellRc.getSelection()
          && !HopEnvironmentDefaults.supportsShellRc(paths)) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "SetupDialog.Error.Header"));
        box.setMessage(BaseMessages.getString(PKG, "SetupDialog.FishShell"));
        box.open();
        return;
      }
      HopEnvironmentSpec spec = toSpec(dryRun);
      if (spec.variables().values().stream().allMatch(StringUtils::isBlank)) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "SetupDialog.Error.Header"));
        box.setMessage(BaseMessages.getString(PKG, "SetupDialog.NoVariables"));
        box.open();
        return;
      }
      if (web) {
        spec.setWriteUserEnv(false);
        spec.setWriteShellRc(false);
        spec.setWriteScript(true);
        spec.setDryRun(true);
      }
      HopEnvironmentApplyResult result = new HopEnvironmentApplier().apply(spec);
      if (dryRun || web) {
        StringBuilder preview = new StringBuilder(result.describe());
        result
            .getPlannedFiles()
            .forEach(
                (path, content) ->
                    preview.append("\n\n--- ").append(path).append(" ---\n").append(content));
        EnterTextDialog dialog =
            new EnterTextDialog(
                shell,
                BaseMessages.getString(PKG, "SetupDialog.Preview.Header"),
                BaseMessages.getString(PKG, "SetupDialog.Preview.Message"),
                preview.toString(),
                true);
        dialog.setReadOnly();
        dialog.open();
        return;
      }
      HopConfig.getInstance().saveOption(HopSetupVariables.CONFIG_OPTION_DO_NOT_SHOW, true);
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "SetupDialog.Success.Header"));
      box.setMessage(result.describe());
      box.open();
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          e);
    }
  }

  private void cancel() {
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
