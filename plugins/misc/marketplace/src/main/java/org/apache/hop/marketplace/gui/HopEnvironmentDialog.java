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

package org.apache.hop.marketplace.gui;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.EnvironmentDrift;
import org.apache.hop.marketplace.env.HopEnvironmentLoader;
import org.apache.hop.marketplace.env.HopEnvironmentSpec;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Graphical editor for marketplace environment files ({@code hop-env.yaml} / {@code .json}).
 *
 * <p>Covers all {@link HopEnvironmentSpec} fields: general settings, repositories, plugins, and jar
 * dependencies.
 */
@Getter
@Setter
public class HopEnvironmentDialog extends Dialog {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  /** Audit list type for recently opened/saved marketplace environment files. */
  public static final String AUDIT_TYPE_ENV_FILES = "MarketplaceEnvFiles";

  private final PropsUi props;
  private final ILogChannel log = new LogChannel("MarketplaceEnvironment");
  private final Path hopHome;
  private final MarketplaceConfig config;

  private Shell shell;
  private Combo wFileCombo;
  private Text wVersion;
  private Text wHopVersion;
  private Button wEnforceOnRun;
  private Button wPrune;
  private Button wStrict;
  private TableView wRepos;
  private TableView wPlugins;
  private TableView wDependencies;

  private Path currentPath;
  private boolean dirty;
  private boolean ignoreDirtyEvents;
  private boolean saved;
  private Path resultPath;
  private boolean embedded;
  private java.util.function.Consumer<String> statusListener;

  public HopEnvironmentDialog(Shell parent, Path initialPath) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    this.props = PropsUi.getInstance();
    Path home;
    MarketplaceConfig cfg;
    try {
      home = HopHome.resolve();
      cfg = MarketplaceConfig.load();
    } catch (Exception e) {
      home = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
      cfg = new MarketplaceConfig();
    }
    this.hopHome = home;
    this.config = cfg;
    this.currentPath = initialPath;
  }

  /**
   * Embed the environment editor into an existing composite (e.g. Marketplace Environment tab).
   *
   * @param parent composite with FormLayout
   * @param initialPath optional hop-env path to load
   * @param statusListener optional status line callback (may be null)
   */
  public static void embed(Composite parent, Path initialPath, Consumer<String> statusListener) {
    HopEnvironmentDialog editor = new HopEnvironmentDialog(parent.getShell(), initialPath);
    editor.embedded = true;
    editor.statusListener = statusListener;
    editor.shell = parent.getShell();
    editor.createControls(parent, false);
    if (initialPath != null && Files.isRegularFile(initialPath)) {
      editor.loadFromPath(initialPath);
    } else {
      editor.loadSpec(newEmptySpec(), null);
    }
  }

  public Path open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    updateTitle();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    createControls(shell, true);

    shell.addListener(
        SWT.Close,
        e -> {
          e.doit = false;
          close();
        });

    BaseTransformDialog.setSize(shell);
    shell.setSize(Math.max(shell.getSize().x, 820), Math.max(shell.getSize().y, 560));
    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return resultPath;
  }

  private void createControls(Composite parent, boolean includeClose) {
    // File toolbar
    Button wNew = new Button(parent, SWT.PUSH);
    wNew.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.New"));
    wNew.addListener(SWT.Selection, e -> newFile());
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment(0, 0);
    fdNew.top = new FormAttachment(0, 0);
    wNew.setLayoutData(fdNew);

    Button wOpen = new Button(parent, SWT.PUSH);
    wOpen.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Open"));
    wOpen.addListener(SWT.Selection, e -> openFile());
    FormData fdOpen = new FormData();
    fdOpen.left = new FormAttachment(wNew, PropsUi.getMargin());
    fdOpen.top = new FormAttachment(0, 0);
    wOpen.setLayoutData(fdOpen);

    Button wSave = new Button(parent, SWT.PUSH);
    wSave.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Save"));
    wSave.addListener(SWT.Selection, e -> save());
    FormData fdSave = new FormData();
    fdSave.left = new FormAttachment(wOpen, PropsUi.getMargin());
    fdSave.top = new FormAttachment(0, 0);
    wSave.setLayoutData(fdSave);

    Button wSaveAs = new Button(parent, SWT.PUSH);
    wSaveAs.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.SaveAs"));
    wSaveAs.addListener(SWT.Selection, e -> saveAs());
    FormData fdSaveAs = new FormData();
    fdSaveAs.left = new FormAttachment(wSave, PropsUi.getMargin());
    fdSaveAs.top = new FormAttachment(0, 0);
    wSaveAs.setLayoutData(fdSaveAs);

    wFileCombo = new Combo(parent, SWT.DROP_DOWN | SWT.BORDER);
    PropsUi.setLook(wFileCombo);
    FormData fdFile = new FormData();
    fdFile.left = new FormAttachment(wSaveAs, PropsUi.getMargin() * 2);
    fdFile.right = new FormAttachment(100, 0);
    fdFile.top = new FormAttachment(wNew, 0, SWT.CENTER);
    wFileCombo.setLayoutData(fdFile);
    refreshFileComboItems(currentPath != null ? currentPath.toString() : "");
    wFileCombo.addListener(SWT.Selection, e -> onFileComboSelected());
    wFileCombo.addListener(SWT.DefaultSelection, e -> onFileComboSelected());

    Label wToolbarSep = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdToolbarSep = new FormData();
    fdToolbarSep.left = new FormAttachment(0, 0);
    fdToolbarSep.right = new FormAttachment(100, 0);
    fdToolbarSep.top = new FormAttachment(wNew, PropsUi.getMargin());
    wToolbarSep.setLayoutData(fdToolbarSep);

    // Bottom buttons
    Button wApply = new Button(parent, SWT.PUSH);
    wApply.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Apply"));
    wApply.setToolTipText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Apply.Tooltip"));
    wApply.addListener(SWT.Selection, e -> apply());

    Button wValidate = new Button(parent, SWT.PUSH);
    wValidate.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Validate"));
    wValidate.setToolTipText(
        BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Validate.Tooltip"));
    wValidate.addListener(SWT.Selection, e -> validate());

    Button wClose = null;
    if (includeClose) {
      wClose = new Button(parent, SWT.PUSH);
      wClose.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Close"));
      wClose.addListener(SWT.Selection, e -> close());
      BaseTransformDialog.positionBottomButtons(
          parent, new Button[] {wValidate, wApply, wClose}, PropsUi.getMargin(), null);
    } else {
      BaseTransformDialog.positionBottomButtons(
          parent, new Button[] {wValidate, wApply}, PropsUi.getMargin(), null);
    }

    wPrune = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wPrune);
    wPrune.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Prune"));
    wPrune.setToolTipText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Prune.Tooltip"));
    FormData fdPrune = new FormData();
    fdPrune.left = new FormAttachment(0, 0);
    fdPrune.bottom = new FormAttachment(wValidate, -PropsUi.getMargin());
    wPrune.setLayoutData(fdPrune);

    wStrict = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wStrict);
    wStrict.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Strict"));
    wStrict.setToolTipText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Strict.Tooltip"));
    FormData fdStrict = new FormData();
    fdStrict.left = new FormAttachment(wPrune, PropsUi.getMargin() * 3);
    fdStrict.bottom = new FormAttachment(wValidate, -PropsUi.getMargin());
    wStrict.setLayoutData(fdStrict);

    CTabFolder wTabFolder = new CTabFolder(parent, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(wToolbarSep, PropsUi.getMargin());
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wPrune, -PropsUi.getMargin());
    wTabFolder.setLayoutData(fdTabs);

    createGeneralTab(wTabFolder);
    createRepositoriesTab(wTabFolder);
    createPluginsTab(wTabFolder);
    createDependenciesTab(wTabFolder);
    wTabFolder.setSelection(0);

    // Load initial content (dialog mode only; embed() loads after createControls)
    if (!embedded) {
      if (currentPath != null && Files.isRegularFile(currentPath)) {
        loadFromPath(currentPath);
      } else {
        loadSpec(newEmptySpec(), null);
      }
    }
  }

  private void createGeneralTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Tab.General"));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.General.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    int middle = 30;
    Label wlVersion = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlVersion);
    wlVersion.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.General.Version"));
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment(0, 0);
    fdlVersion.top = new FormAttachment(wlHelp, PropsUi.getMargin() * 2);
    fdlVersion.right = new FormAttachment(middle, -PropsUi.getMargin());
    wlVersion.setLayoutData(fdlVersion);
    wVersion = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wVersion);
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment(middle, 0);
    fdVersion.top = new FormAttachment(wlVersion, 0, SWT.CENTER);
    fdVersion.right = new FormAttachment(100, 0);
    wVersion.setLayoutData(fdVersion);
    wVersion.addListener(SWT.Modify, e -> markDirty());

    Label wlHopVersion = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlHopVersion);
    wlHopVersion.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.General.HopVersion"));
    FormData fdlHopVersion = new FormData();
    fdlHopVersion.left = new FormAttachment(0, 0);
    fdlHopVersion.top = new FormAttachment(wVersion, PropsUi.getMargin());
    fdlHopVersion.right = new FormAttachment(middle, -PropsUi.getMargin());
    wlHopVersion.setLayoutData(fdlHopVersion);
    wHopVersion = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wHopVersion);
    FormData fdHopVersion = new FormData();
    fdHopVersion.left = new FormAttachment(middle, 0);
    fdHopVersion.top = new FormAttachment(wlHopVersion, 0, SWT.CENTER);
    fdHopVersion.right = new FormAttachment(100, 0);
    wHopVersion.setLayoutData(fdHopVersion);
    wHopVersion.addListener(SWT.Modify, e -> markDirty());

    wEnforceOnRun = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wEnforceOnRun);
    wEnforceOnRun.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.General.EnforceOnRun"));
    wEnforceOnRun.setToolTipText(
        BaseMessages.getString(PKG, "HopEnvironmentDialog.General.EnforceOnRun.Tooltip"));
    FormData fdEnforce = new FormData();
    fdEnforce.left = new FormAttachment(middle, 0);
    fdEnforce.top = new FormAttachment(wHopVersion, PropsUi.getMargin() * 2);
    wEnforceOnRun.setLayoutData(fdEnforce);
    wEnforceOnRun.addListener(SWT.Selection, e -> markDirty());
  }

  private void createRepositoriesTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Tab.Repositories"));
    tab.setImage(GuiResource.getInstance().getImageServer());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Repositories.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wAdd = sideButton(comp, "HopEnvironmentDialog.Button.Add", wlHelp);
    Button wEdit = sideButton(comp, "HopEnvironmentDialog.Button.Edit", wAdd);
    Button wRemove = sideButton(comp, "HopEnvironmentDialog.Button.Remove", wEdit);
    Button wUp = sideButton(comp, "HopEnvironmentDialog.Button.MoveUp", wRemove);
    Button wDown = sideButton(comp, "HopEnvironmentDialog.Button.MoveDown", wUp);
    Button wImport = sideButton(comp, "HopEnvironmentDialog.Button.ImportRepos", wDown);

    wAdd.addListener(SWT.Selection, e -> addRepository());
    wEdit.addListener(SWT.Selection, e -> editRepository());
    wRemove.addListener(SWT.Selection, e -> removeSelected(wRepos));
    wUp.addListener(SWT.Selection, e -> moveSelected(wRepos, -1));
    wDown.addListener(SWT.Selection, e -> moveSelected(wRepos, 1));
    wImport.addListener(SWT.Selection, e -> importReposFromConfig());

    wRepos =
        createTableView(
            comp,
            wlHelp,
            wAdd,
            new ColumnInfo[] {
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Repo.Column.Id"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Repo.Column.Url"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Repo.Column.Username"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Repo.Column.Auth"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
            });
    wRepos.table.addListener(SWT.DefaultSelection, e -> editRepository());
  }

  private void createPluginsTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Tab.Plugins"));
    tab.setImage(GuiResource.getInstance().getImagePlugin());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Plugins.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wCatalog = sideButton(comp, "HopEnvironmentDialog.Button.AddFromCatalog", wlHelp, null);
    Button wAdd = sideButton(comp, "HopEnvironmentDialog.Button.Add", wCatalog, wCatalog);
    Button wEdit = sideButton(comp, "HopEnvironmentDialog.Button.Edit", wAdd, wCatalog);
    Button wRemove = sideButton(comp, "HopEnvironmentDialog.Button.Remove", wEdit, wCatalog);
    Button wUp = sideButton(comp, "HopEnvironmentDialog.Button.MoveUp", wRemove, wCatalog);
    Button wDown = sideButton(comp, "HopEnvironmentDialog.Button.MoveDown", wUp, wCatalog);

    wCatalog.addListener(SWT.Selection, e -> addPluginsFromCatalog());
    wAdd.addListener(SWT.Selection, e -> addPlugin());
    wEdit.addListener(SWT.Selection, e -> editPlugin());
    wRemove.addListener(SWT.Selection, e -> removeSelected(wPlugins));
    wUp.addListener(SWT.Selection, e -> moveSelected(wPlugins, -1));
    wDown.addListener(SWT.Selection, e -> moveSelected(wPlugins, 1));

    wPlugins =
        createTableView(
            comp,
            wlHelp,
            wCatalog,
            new ColumnInfo[] {
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Plugin.Column.GroupId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Plugin.Column.ArtifactId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Plugin.Column.Version"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
            });
    wPlugins.table.addListener(SWT.DefaultSelection, e -> editPlugin());
  }

  private void createDependenciesTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Tab.Dependencies"));
    tab.setImage(GuiResource.getInstance().getImageLabel());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Dependencies.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wAdd = sideButton(comp, "HopEnvironmentDialog.Button.Add", wlHelp);
    Button wEdit = sideButton(comp, "HopEnvironmentDialog.Button.Edit", wAdd);
    Button wRemove = sideButton(comp, "HopEnvironmentDialog.Button.Remove", wEdit);
    Button wUp = sideButton(comp, "HopEnvironmentDialog.Button.MoveUp", wRemove);
    Button wDown = sideButton(comp, "HopEnvironmentDialog.Button.MoveDown", wUp);

    wAdd.addListener(SWT.Selection, e -> addDependency());
    wEdit.addListener(SWT.Selection, e -> editDependency());
    wRemove.addListener(SWT.Selection, e -> removeSelected(wDependencies));
    wUp.addListener(SWT.Selection, e -> moveSelected(wDependencies, -1));
    wDown.addListener(SWT.Selection, e -> moveSelected(wDependencies, 1));

    wDependencies =
        createTableView(
            comp,
            wlHelp,
            wAdd,
            new ColumnInfo[] {
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Dep.Column.GroupId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Dep.Column.ArtifactId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Dep.Column.Version"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopEnvironmentDialog.Dep.Column.Target"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
            });
    wDependencies.table.addListener(SWT.DefaultSelection, e -> editDependency());
  }

  private Button sideButton(Composite parent, String key, Control above) {
    return sideButton(parent, key, above, null);
  }

  private Button sideButton(Composite parent, String key, Control above, Control left) {
    Button b = new Button(parent, SWT.PUSH);
    b.setText(BaseMessages.getString(PKG, key));
    FormData fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(above, PropsUi.getMargin());
    if (left == null) {
      fd.left = new FormAttachment(100, (int) (-100 * PropsUi.getNativeZoomFactor()));
    } else {
      fd.left = new FormAttachment(left, 0, SWT.LEFT);
    }
    b.setLayoutData(fd);
    return b;
  }

  private TableView createTableView(
      Composite parent, Label help, Button sideTop, ColumnInfo[] columns) {
    TableView view =
        new TableView(
            Variables.getADefaultVariableSpace(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.V_SCROLL,
            columns,
            1,
            true,
            null,
            props,
            false);
    PropsUi.setLook(view);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(help, PropsUi.getMargin());
    fd.right = new FormAttachment(sideTop, -PropsUi.getMargin());
    fd.bottom = new FormAttachment(100, 0);
    view.setLayoutData(fd);
    return view;
  }

  // --- load / save / dirty ---

  private static HopEnvironmentSpec newEmptySpec() {
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setVersion("1.0");
    return spec;
  }

  private void loadFromPath(Path path) {
    try {
      HopEnvironmentSpec spec = HopEnvironmentLoader.load(path);
      loadSpec(spec, path);
      rememberEnvFile(path);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Header"),
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Load", path.toString()),
          e);
    }
  }

  private void loadSpec(HopEnvironmentSpec spec, Path path) {
    ignoreDirtyEvents = true;
    try {
      currentPath = path;
      wVersion.setText(Const.NVL(spec.getVersion(), "1.0"));
      wHopVersion.setText(Const.NVL(spec.getHopVersion(), ""));
      wEnforceOnRun.setSelection(spec.isEnforceOnRun());
      fillRepos(spec.getRepositories());
      fillPlugins(spec.getPlugins());
      fillDependencies(spec.getDependencies());
      wRepos.optimizeTableView();
      wPlugins.optimizeTableView();
      wDependencies.optimizeTableView();
      dirty = false;
      updateFileLabel();
      updateTitle();
    } finally {
      ignoreDirtyEvents = false;
    }
  }

  private void fillRepos(List<HopEnvironmentSpec.RepositoryRef> list) {
    wRepos.table.removeAll();
    if (list == null) {
      return;
    }
    for (HopEnvironmentSpec.RepositoryRef ref : list) {
      if (ref == null) {
        continue;
      }
      addRepoItem(ref);
    }
  }

  private void addRepoItem(HopEnvironmentSpec.RepositoryRef ref) {
    TableItem item = new TableItem(wRepos.table, SWT.NONE);
    item.setText(1, Const.NVL(ref.getId(), ""));
    item.setText(2, Const.NVL(ref.getUrl(), ""));
    item.setText(3, Const.NVL(ref.getUsername(), ""));
    item.setText(4, StringUtils.isNotBlank(ref.getPassword()) ? "Y" : "");
    item.setData(ref);
  }

  private void fillPlugins(List<HopEnvironmentSpec.PluginRef> list) {
    wPlugins.table.removeAll();
    if (list == null) {
      return;
    }
    for (HopEnvironmentSpec.PluginRef ref : list) {
      if (ref == null) {
        continue;
      }
      addPluginItem(ref);
    }
  }

  private void addPluginItem(HopEnvironmentSpec.PluginRef ref) {
    TableItem item = new TableItem(wPlugins.table, SWT.NONE);
    item.setText(1, Const.NVL(ref.getGroupId(), ""));
    item.setText(2, Const.NVL(ref.getArtifactId(), ""));
    item.setText(3, Const.NVL(ref.getVersion(), ""));
    item.setData(ref);
  }

  private void fillDependencies(List<HopEnvironmentSpec.DependencyRef> list) {
    wDependencies.table.removeAll();
    if (list == null) {
      return;
    }
    for (HopEnvironmentSpec.DependencyRef ref : list) {
      if (ref == null) {
        continue;
      }
      addDepItem(ref);
    }
  }

  private void addDepItem(HopEnvironmentSpec.DependencyRef ref) {
    TableItem item = new TableItem(wDependencies.table, SWT.NONE);
    item.setText(1, Const.NVL(ref.getGroupId(), ""));
    item.setText(2, Const.NVL(ref.getArtifactId(), ""));
    item.setText(3, Const.NVL(ref.getVersion(), ""));
    item.setText(4, Const.NVL(ref.getTarget(), "lib/jdbc"));
    item.setData(ref);
  }

  private HopEnvironmentSpec collectSpec() {
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setVersion(StringUtils.defaultIfBlank(wVersion.getText(), "1.0").trim());
    String hopVer = wHopVersion.getText();
    spec.setHopVersion(StringUtils.isBlank(hopVer) ? null : hopVer.trim());
    spec.setEnforceOnRun(wEnforceOnRun.getSelection());

    List<HopEnvironmentSpec.RepositoryRef> repos = new ArrayList<>();
    for (TableItem item : wRepos.table.getItems()) {
      if (item.getData() instanceof HopEnvironmentSpec.RepositoryRef ref) {
        repos.add(ref);
      }
    }
    spec.setRepositories(repos);

    List<HopEnvironmentSpec.PluginRef> plugins = new ArrayList<>();
    for (TableItem item : wPlugins.table.getItems()) {
      if (item.getData() instanceof HopEnvironmentSpec.PluginRef ref) {
        plugins.add(ref);
      }
    }
    spec.setPlugins(plugins);

    List<HopEnvironmentSpec.DependencyRef> deps = new ArrayList<>();
    for (TableItem item : wDependencies.table.getItems()) {
      if (item.getData() instanceof HopEnvironmentSpec.DependencyRef ref) {
        deps.add(ref);
      }
    }
    spec.setDependencies(deps);
    return spec;
  }

  private void markDirty() {
    if (ignoreDirtyEvents) {
      return;
    }
    if (!dirty) {
      dirty = true;
      updateTitle();
    }
  }

  private void updateTitle() {
    if (embedded) {
      return;
    }
    String name =
        currentPath != null
            ? currentPath.getFileName().toString()
            : BaseMessages.getString(PKG, "HopEnvironmentDialog.Untitled");
    String title =
        BaseMessages.getString(PKG, "HopEnvironmentDialog.Shell.Title", name) + (dirty ? " *" : "");
    if (shell != null && !shell.isDisposed()) {
      shell.setText(title);
    }
  }

  private void updateFileLabel() {
    if (wFileCombo == null || wFileCombo.isDisposed()) {
      return;
    }
    if (currentPath != null) {
      refreshFileComboItems(currentPath.toString());
    } else {
      refreshFileComboItems("");
      wFileCombo.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Untitled"));
    }
  }

  /**
   * Load recent filenames from {@link AuditManager#getActive()}{@code .retrieveList(...)} into the
   * combo and select {@code selectedPath} when non-blank.
   */
  private void refreshFileComboItems(String selectedPath) {
    if (wFileCombo == null || wFileCombo.isDisposed()) {
      return;
    }
    try {
      AuditList list =
          AuditManager.getActive().retrieveList(HopNamespace.getNamespace(), AUDIT_TYPE_ENV_FILES);
      if (list != null && list.getNames() != null && !list.getNames().isEmpty()) {
        wFileCombo.setItems(list.getNames().toArray(new String[0]));
      } else {
        wFileCombo.setItems(new String[0]);
      }
    } catch (Exception e) {
      log.logError("Unable to load marketplace environment file audit list", e);
      wFileCombo.setItems(AuditManagerGuiUtil.getLastUsedValues(AUDIT_TYPE_ENV_FILES));
    }
    if (StringUtils.isNotBlank(selectedPath)) {
      wFileCombo.setText(selectedPath);
    }
  }

  /** Remember path in the audit list (most-recent first) and refresh the combo. */
  private void rememberEnvFile(Path path) {
    if (path == null) {
      return;
    }
    String absolute = path.toAbsolutePath().normalize().toString();
    AuditManagerGuiUtil.addLastUsedValue(AUDIT_TYPE_ENV_FILES, absolute);
    refreshFileComboItems(absolute);
  }

  private void onFileComboSelected() {
    if (wFileCombo == null || wFileCombo.isDisposed()) {
      return;
    }
    String text = wFileCombo.getText();
    if (StringUtils.isBlank(text)) {
      return;
    }
    Path p = Path.of(text.trim()).toAbsolutePath().normalize();
    if (currentPath != null && currentPath.equals(p)) {
      return;
    }
    if (!Files.isRegularFile(p)) {
      return;
    }
    if (confirmDiscardIfDirty()) {
      updateFileLabel();
      return;
    }
    loadFromPath(p);
  }

  private boolean confirmDiscardIfDirty() {
    if (!dirty) {
      return false;
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_WARNING);
    box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Dirty.Header"));
    box.setMessage(BaseMessages.getString(PKG, "HopEnvironmentDialog.Dirty.Message"));
    int answer = box.open();
    if (answer == SWT.CANCEL) {
      return true;
    }
    if (answer == SWT.YES) {
      return !save();
    }
    return false;
  }

  private void newFile() {
    if (confirmDiscardIfDirty()) {
      return;
    }
    loadSpec(newEmptySpec(), null);
  }

  private void openFile() {
    if (confirmDiscardIfDirty()) {
      return;
    }
    String path =
        BaseDialog.presentFileDialog(
            false,
            shell,
            new String[] {"*.yaml;*.yml;*.json", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.Env"),
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
            },
            false);
    if (StringUtils.isBlank(path)) {
      return;
    }
    Path p = Path.of(path.trim()).toAbsolutePath().normalize();
    if (!Files.isRegularFile(p)) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.NotFound.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.NotFound.Message", p.toString()));
      box.open();
      return;
    }
    loadFromPath(p);
    rememberEnvFile(p);
  }

  private boolean save() {
    if (currentPath == null) {
      return saveAs();
    }
    return writeTo(currentPath);
  }

  private boolean saveAs() {
    String path =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.yaml;*.yml", "*.json", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "HopEnvironmentDialog.Filter.Yaml"),
              BaseMessages.getString(PKG, "HopEnvironmentDialog.Filter.Json"),
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
            },
            false);
    if (StringUtils.isBlank(path)) {
      return false;
    }
    Path p = Path.of(path.trim()).toAbsolutePath().normalize();
    String name = p.getFileName().toString().toLowerCase();
    if (!name.endsWith(".yaml") && !name.endsWith(".yml") && !name.endsWith(".json")) {
      p = p.resolveSibling(p.getFileName().toString() + ".yaml");
    }
    return writeTo(p);
  }

  private boolean writeTo(Path path) {
    try {
      HopEnvironmentSpec spec = collectSpec();
      String validationError = validateSpec(spec);
      if (validationError != null) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.Header"));
        box.setMessage(validationError);
        box.open();
        return false;
      }
      HopEnvironmentLoader.save(path, spec);
      currentPath = path;
      resultPath = path;
      saved = true;
      dirty = false;
      rememberEnvFile(path);
      updateFileLabel();
      updateTitle();
      return true;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Header"),
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Save", path.toString()),
          e);
      return false;
    }
  }

  private String validateSpec(HopEnvironmentSpec spec) {
    for (HopEnvironmentSpec.RepositoryRef ref : spec.getRepositories()) {
      if (StringUtils.isBlank(ref.getId()) || StringUtils.isBlank(ref.getUrl())) {
        return BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.RepoIdUrl");
      }
    }
    for (HopEnvironmentSpec.PluginRef ref : spec.getPlugins()) {
      if (StringUtils.isBlank(ref.getArtifactId())) {
        return BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.PluginArtifact");
      }
    }
    for (HopEnvironmentSpec.DependencyRef ref : spec.getDependencies()) {
      if (StringUtils.isAnyBlank(ref.getGroupId(), ref.getArtifactId(), ref.getVersion())) {
        return BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.DepGav");
      }
    }
    return null;
  }

  private boolean ensureSavedForAction() {
    if (dirty || currentPath == null) {
      MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.SaveFirst.Header"));
      box.setMessage(BaseMessages.getString(PKG, "HopEnvironmentDialog.SaveFirst.Message"));
      if (box.open() != SWT.YES) {
        return true;
      }
      if (!save()) {
        return true;
      }
    }
    return currentPath == null || !Files.isRegularFile(currentPath);
  }

  private void validate() {
    if (ensureSavedForAction()) {
      return;
    }
    try {
      HopEnvironmentSpec env = HopEnvironmentLoader.load(currentPath);
      EnvironmentApplier applier = new EnvironmentApplier(log, hopHome, MarketplaceConfig.load());
      EnvironmentDrift drift = applier.validate(env);
      if (wStrict.getSelection()) {
        populateExtraPlugins(env, drift);
      }
      boolean hard =
          !drift.getMissingPlugins().isEmpty()
              || !drift.getVersionMismatches().isEmpty()
              || !drift.getMissingDependencies().isEmpty()
              || (wStrict.getSelection() && !drift.getExtraMarketplacePlugins().isEmpty());
      if (!hard) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Ok.Header"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "MarketplaceDialog.Validate.Ok.Message", currentPath.toString()));
        box.open();
        notifyStatus(BaseMessages.getString(PKG, "MarketplaceDialog.Status.RestartHint"));
        return;
      }
      notifyStatus(BaseMessages.getString(PKG, "MarketplaceDialog.Status.Drift"));
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Drift.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Drift.Message")
              + "\n\n"
              + drift.formatReport()
              + "\n"
              + BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Drift.Hint"));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Error"),
          e);
    }
  }

  private void apply() {
    if (ensureSavedForAction()) {
      return;
    }
    boolean prune = wPrune.getSelection();
    if (prune) {
      MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      confirm.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.PruneConfirm.Header"));
      confirm.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Apply.PruneConfirm.Message"));
      if (confirm.open() != SWT.YES) {
        return;
      }
    }
    try {
      MarketplaceConfig live = MarketplaceConfig.load();
      if (!live.isEnabled()) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Header"));
        box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Disabled"));
        box.open();
        return;
      }
      HopEnvironmentSpec env = HopEnvironmentLoader.load(currentPath);
      new EnvironmentApplier(log, hopHome, live).apply(env, prune);
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Done.Header"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "MarketplaceDialog.Apply.Done.Message", currentPath.toString()));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Error"),
          e);
    }
  }

  private void populateExtraPlugins(HopEnvironmentSpec env, EnvironmentDrift drift)
      throws Exception {
    Set<String> desired = new HashSet<>();
    if (env.getPlugins() != null) {
      for (HopEnvironmentSpec.PluginRef ref : env.getPlugins()) {
        if (ref.getArtifactId() != null) {
          desired.add(ref.getArtifactId());
        }
      }
    }
    Path receipts = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
    if (!Files.isDirectory(receipts)) {
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(receipts, "*.json")) {
      for (Path f : stream) {
        String name = f.getFileName().toString();
        String id = name.substring(0, name.length() - ".json".length());
        if (!desired.contains(id)) {
          drift.getExtraMarketplacePlugins().add(id);
        }
      }
    }
  }

  private void close() {
    if (embedded) {
      return;
    }
    if (confirmDiscardIfDirty()) {
      return;
    }
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  // --- repository CRUD ---

  private void addRepository() {
    HopEnvironmentSpec.RepositoryRef ref = new HopEnvironmentSpec.RepositoryRef();
    if (editRepositoryDialog(ref, true)) {
      addRepoItem(ref);
      markDirty();
    }
  }

  private void editRepository() {
    TableItem[] sel = wRepos.table.getSelection();
    if (sel == null || sel.length == 0) {
      return;
    }
    HopEnvironmentSpec.RepositoryRef ref = (HopEnvironmentSpec.RepositoryRef) sel[0].getData();
    if (ref == null) {
      return;
    }
    if (editRepositoryDialog(ref, false)) {
      sel[0].setText(1, Const.NVL(ref.getId(), ""));
      sel[0].setText(2, Const.NVL(ref.getUrl(), ""));
      sel[0].setText(3, Const.NVL(ref.getUsername(), ""));
      sel[0].setText(4, StringUtils.isNotBlank(ref.getPassword()) ? "Y" : "");
      markDirty();
    }
  }

  private boolean editRepositoryDialog(HopEnvironmentSpec.RepositoryRef ref, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew ? "HopEnvironmentDialog.Repo.Edit.Add" : "HopEnvironmentDialog.Repo.Edit.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);
    int middle = 30;
    int margin = PropsUi.getMargin();

    Label wlId = labeled(dialog, "HopEnvironmentDialog.Repo.Column.Id", 0, middle, margin);
    Text wId = textField(dialog, wlId, middle, Const.NVL(ref.getId(), ""));
    Label wlUrl = labeled(dialog, "HopEnvironmentDialog.Repo.Column.Url", wId, middle, margin);
    Text wUrl = textField(dialog, wlUrl, middle, Const.NVL(ref.getUrl(), ""));
    Label wlUser =
        labeled(dialog, "HopEnvironmentDialog.Repo.Column.Username", wUrl, middle, margin);
    Text wUser = textField(dialog, wlUser, middle, Const.NVL(ref.getUsername(), ""));
    Label wlPass =
        labeled(dialog, "HopEnvironmentDialog.Repo.Column.Password", wUser, middle, margin);
    Text wPass = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPass);
    wPass.setText(Const.NVL(ref.getPassword(), ""));
    FormData fdPass = new FormData();
    fdPass.left = new FormAttachment(middle, 0);
    fdPass.top = new FormAttachment(wlPass, 0, SWT.CENTER);
    fdPass.right = new FormAttachment(100, 0);
    wPass.setLayoutData(fdPass);

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isBlank(wId.getText()) || StringUtils.isBlank(wUrl.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.Header"));
            box.setMessage(
                BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.RepoIdUrl"));
            box.open();
            return;
          }
          ref.setId(wId.getText().trim());
          ref.setUrl(wUrl.getText().trim());
          ref.setUsername(blankToNull(wUser.getText()));
          ref.setPassword(blankToNull(wPass.getText()));
          ok[0] = true;
          dialog.dispose();
        });
    BaseTransformDialog.positionBottomButtons(dialog, new Button[] {wOk, wCancel}, margin, wPass);
    BaseTransformDialog.setSize(dialog);
    dialog.open();
    Display display = shell.getDisplay();
    while (!dialog.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return ok[0];
  }

  private void importReposFromConfig() {
    try {
      MarketplaceConfig live = config != null ? config : MarketplaceConfig.load();
      if (live.getRepositories() == null || live.getRepositories().isEmpty()) {
        return;
      }
      MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      confirm.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.ImportRepos.Header"));
      confirm.setMessage(BaseMessages.getString(PKG, "HopEnvironmentDialog.ImportRepos.Message"));
      if (confirm.open() != SWT.YES) {
        return;
      }
      wRepos.table.removeAll();
      for (MarketplaceRepository repo : live.getRepositories()) {
        if (repo == null || !repo.isEnabled()) {
          continue;
        }
        HopEnvironmentSpec.RepositoryRef ref = new HopEnvironmentSpec.RepositoryRef();
        ref.setId(repo.getId());
        ref.setUrl(repo.getUrl());
        ref.setUsername(repo.getUsername());
        ref.setPassword(repo.getPassword());
        addRepoItem(ref);
      }
      markDirty();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.Header"),
          BaseMessages.getString(PKG, "HopEnvironmentDialog.Error.ImportRepos"),
          e);
    }
  }

  // --- plugin CRUD ---

  private void addPlugin() {
    HopEnvironmentSpec.PluginRef ref = new HopEnvironmentSpec.PluginRef();
    ref.setVersion(StringUtils.trimToNull(wHopVersion.getText()));
    if (editPluginDialog(ref, true)) {
      addPluginItem(ref);
      markDirty();
    }
  }

  private void editPlugin() {
    TableItem[] sel = wPlugins.table.getSelection();
    if (sel == null || sel.length == 0) {
      return;
    }
    HopEnvironmentSpec.PluginRef ref = (HopEnvironmentSpec.PluginRef) sel[0].getData();
    if (ref == null) {
      return;
    }
    if (editPluginDialog(ref, false)) {
      sel[0].setText(1, Const.NVL(ref.getGroupId(), ""));
      sel[0].setText(2, Const.NVL(ref.getArtifactId(), ""));
      sel[0].setText(3, Const.NVL(ref.getVersion(), ""));
      markDirty();
    }
  }

  private boolean editPluginDialog(HopEnvironmentSpec.PluginRef ref, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew
                ? "HopEnvironmentDialog.Plugin.Edit.Add"
                : "HopEnvironmentDialog.Plugin.Edit.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);
    int middle = 30;
    int margin = PropsUi.getMargin();

    Label wlGroup =
        labeled(dialog, "HopEnvironmentDialog.Plugin.Column.GroupId", 0, middle, margin);
    Text wGroup = textField(dialog, wlGroup, middle, Const.NVL(ref.getGroupId(), ""));
    Label wlArt =
        labeled(dialog, "HopEnvironmentDialog.Plugin.Column.ArtifactId", wGroup, middle, margin);
    Text wArt = textField(dialog, wlArt, middle, Const.NVL(ref.getArtifactId(), ""));
    Label wlVer =
        labeled(dialog, "HopEnvironmentDialog.Plugin.Column.Version", wArt, middle, margin);
    Text wVer = textField(dialog, wlVer, middle, Const.NVL(ref.getVersion(), ""));

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isBlank(wArt.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.Header"));
            box.setMessage(
                BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.PluginArtifact"));
            box.open();
            return;
          }
          ref.setGroupId(blankToNull(wGroup.getText()));
          ref.setArtifactId(wArt.getText().trim());
          ref.setVersion(blankToNull(wVer.getText()));
          ok[0] = true;
          dialog.dispose();
        });
    BaseTransformDialog.positionBottomButtons(dialog, new Button[] {wOk, wCancel}, margin, wVer);
    BaseTransformDialog.setSize(dialog);
    dialog.open();
    Display display = shell.getDisplay();
    while (!dialog.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return ok[0];
  }

  private void addPluginsFromCatalog() {
    Set<String> existing = new HashSet<>();
    for (TableItem item : wPlugins.table.getItems()) {
      if (item.getData() instanceof HopEnvironmentSpec.PluginRef ref
          && StringUtils.isNotBlank(ref.getArtifactId())) {
        existing.add(ref.getArtifactId());
      }
    }
    List<OptionalPluginInfo> chosen = new AddPluginsFromCatalogDialog(shell, existing).open();
    if (chosen == null || chosen.isEmpty()) {
      return;
    }
    String defaultVersion = StringUtils.trimToNull(wHopVersion.getText());
    for (OptionalPluginInfo info : chosen) {
      HopEnvironmentSpec.PluginRef ref = new HopEnvironmentSpec.PluginRef();
      ref.setArtifactId(info.getArtifactId());
      ref.setVersion(defaultVersion);
      addPluginItem(ref);
    }
    markDirty();
  }

  // --- dependency CRUD ---

  private void addDependency() {
    HopEnvironmentSpec.DependencyRef ref = new HopEnvironmentSpec.DependencyRef();
    ref.setTarget("lib/jdbc");
    if (editDependencyDialog(ref, true)) {
      addDepItem(ref);
      markDirty();
    }
  }

  private void editDependency() {
    TableItem[] sel = wDependencies.table.getSelection();
    if (sel == null || sel.length == 0) {
      return;
    }
    HopEnvironmentSpec.DependencyRef ref = (HopEnvironmentSpec.DependencyRef) sel[0].getData();
    if (ref == null) {
      return;
    }
    if (editDependencyDialog(ref, false)) {
      sel[0].setText(1, Const.NVL(ref.getGroupId(), ""));
      sel[0].setText(2, Const.NVL(ref.getArtifactId(), ""));
      sel[0].setText(3, Const.NVL(ref.getVersion(), ""));
      sel[0].setText(4, Const.NVL(ref.getTarget(), "lib/jdbc"));
      markDirty();
    }
  }

  private boolean editDependencyDialog(HopEnvironmentSpec.DependencyRef ref, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew ? "HopEnvironmentDialog.Dep.Edit.Add" : "HopEnvironmentDialog.Dep.Edit.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);
    int middle = 30;
    int margin = PropsUi.getMargin();

    Label wlGroup = labeled(dialog, "HopEnvironmentDialog.Dep.Column.GroupId", 0, middle, margin);
    Text wGroup = textField(dialog, wlGroup, middle, Const.NVL(ref.getGroupId(), ""));
    Label wlArt =
        labeled(dialog, "HopEnvironmentDialog.Dep.Column.ArtifactId", wGroup, middle, margin);
    Text wArt = textField(dialog, wlArt, middle, Const.NVL(ref.getArtifactId(), ""));
    Label wlVer = labeled(dialog, "HopEnvironmentDialog.Dep.Column.Version", wArt, middle, margin);
    Text wVer = textField(dialog, wlVer, middle, Const.NVL(ref.getVersion(), ""));
    Label wlTarget =
        labeled(dialog, "HopEnvironmentDialog.Dep.Column.Target", wVer, middle, margin);
    Text wTarget = textField(dialog, wlTarget, middle, Const.NVL(ref.getTarget(), "lib/jdbc"));

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isAnyBlank(wGroup.getText(), wArt.getText(), wVer.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setText(BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.Header"));
            box.setMessage(BaseMessages.getString(PKG, "HopEnvironmentDialog.Validation.DepGav"));
            box.open();
            return;
          }
          ref.setGroupId(wGroup.getText().trim());
          ref.setArtifactId(wArt.getText().trim());
          ref.setVersion(wVer.getText().trim());
          ref.setTarget(StringUtils.defaultIfBlank(wTarget.getText(), "lib/jdbc").trim());
          ok[0] = true;
          dialog.dispose();
        });
    BaseTransformDialog.positionBottomButtons(dialog, new Button[] {wOk, wCancel}, margin, wTarget);
    BaseTransformDialog.setSize(dialog);
    dialog.open();
    Display display = shell.getDisplay();
    while (!dialog.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return ok[0];
  }

  // --- table helpers ---

  private void removeSelected(TableView view) {
    int idx = view.table.getSelectionIndex();
    if (idx < 0) {
      return;
    }
    view.table.remove(idx);
    markDirty();
  }

  private void moveSelected(TableView view, int delta) {
    int idx = view.table.getSelectionIndex();
    if (idx < 0) {
      return;
    }
    int target = idx + delta;
    if (target < 0 || target >= view.table.getItemCount()) {
      return;
    }
    TableItem a = view.table.getItem(idx);
    TableItem b = view.table.getItem(target);
    Object dataA = a.getData();
    Object dataB = b.getData();
    int cols = view.table.getColumnCount();
    String[] textsA = new String[cols];
    String[] textsB = new String[cols];
    for (int c = 0; c < cols; c++) {
      textsA[c] = a.getText(c);
      textsB[c] = b.getText(c);
    }
    for (int c = 0; c < cols; c++) {
      a.setText(c, textsB[c]);
      b.setText(c, textsA[c]);
    }
    a.setData(dataB);
    b.setData(dataA);
    view.table.setSelection(target);
    markDirty();
  }

  private Label labeled(Shell dialog, String key, Object above, int middle, int margin) {
    Label label = new Label(dialog, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, key));
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    if (above instanceof Integer) {
      fd.top = new FormAttachment(0, margin);
    } else {
      fd.top = new FormAttachment((Control) above, margin);
    }
    label.setLayoutData(fd);
    return label;
  }

  private Text textField(Shell dialog, Label label, int middle, String value) {
    Text text = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.setText(value);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    text.setLayoutData(fd);
    return text;
  }

  private void notifyStatus(String message) {
    if (statusListener != null && message != null) {
      statusListener.accept(message);
    }
  }

  private static String blankToNull(String value) {
    return StringUtils.isBlank(value) ? null : value.trim();
  }

  public boolean wasSaved() {
    return saved;
  }
}
