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

import java.lang.reflect.InvocationTargetException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.EnvironmentDrift;
import org.apache.hop.marketplace.env.HopInstallSpec;
import org.apache.hop.marketplace.env.HopInstallSpecFiles;
import org.apache.hop.marketplace.env.HopInstallSpecLoader;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.yaml.YamlExplorerFileType;
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
 * Graphical editor for Hop install spec files ({@code hop-env.yaml} / {@code .json}).
 *
 * <p>Covers all {@link HopInstallSpec} fields: general settings, repositories, plugins, and jar
 * dependencies.
 */
@Getter
@Setter
public class HopInstallSpecEditor extends Dialog {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  /** Audit list type for recently opened/saved install spec files. */
  public static final String AUDIT_TYPE_ENV_FILES = "MarketplaceEnvFiles";

  private final PropsUi props;
  private final ILogChannel log = new LogChannel("MarketplaceInstallSpec");
  private final Path hopHome;
  private final MarketplaceConfig config;

  private Shell shell;
  private Button wNew;
  private Button wOpen;
  private Button wSave;
  private Button wSaveAs;
  private Button wEditYaml;
  private Button wReload;
  private Combo wFileCombo;
  private Label wFullClientBanner;
  private Text wVersion;
  private Text wHopVersion;
  private Button wEnforceOnRun;
  private Button wPrune;
  private Button wStrict;
  private TableView wRepos;
  private TableView wPlugins;
  private TableView wDependencies;

  /** Unresolved path as shown in the combo (may contain variable expressions). */
  private String currentFilename;

  private boolean dirty;
  private boolean ignoreDirtyEvents;
  private boolean saved;
  private String resultFilename;
  private boolean embedded;

  @Setter(lombok.AccessLevel.NONE)
  private boolean explorerMode;

  private Runnable dirtyListener;
  private java.util.function.Consumer<String> statusListener;

  public HopInstallSpecEditor(Shell parent, Path initialPath) {
    this(parent, initialPath == null ? null : initialPath.toString());
  }

  public HopInstallSpecEditor(Shell parent, String initialFilename) {
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
    this.currentFilename = initialFilename;
  }

  /**
   * Embed the install spec editor into an existing composite (e.g. Marketplace Install spec tab).
   *
   * @param parent composite with FormLayout
   * @param initialFilename optional hop-env path to load (variables allowed)
   * @param statusListener optional status line callback (may be null)
   */
  public static HopInstallSpecEditor embed(
      Composite parent, String initialFilename, Consumer<String> statusListener) {
    HopInstallSpecEditor editor = new HopInstallSpecEditor(parent.getShell(), initialFilename);
    editor.embedded = true;
    editor.statusListener = statusListener;
    editor.shell = parent.getShell();
    editor.createControls(parent, false);
    if (HopInstallSpecFiles.exists(initialFilename, editor.variables())) {
      editor.loadFromFilename(initialFilename);
    } else {
      editor.loadSpec(newEmptySpec(), null);
    }
    return editor;
  }

  public void setExplorerMode(boolean explorerMode) {
    this.explorerMode = explorerMode;
    applyExplorerModeVisibility();
  }

  public Path open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageMarketplace());
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
    return resultFilename == null ? null : Path.of(resultFilename);
  }

  private void createControls(Composite parent, boolean includeClose) {
    // File toolbar
    wNew = new Button(parent, SWT.PUSH);
    wNew.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.New"));
    wNew.addListener(SWT.Selection, e -> newFile());
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment(0, 0);
    fdNew.top = new FormAttachment(0, 0);
    wNew.setLayoutData(fdNew);
    PropsUi.setLook(wNew);

    wOpen = new Button(parent, SWT.PUSH);
    wOpen.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Open"));
    wOpen.addListener(SWT.Selection, e -> openFile());
    FormData fdOpen = new FormData();
    fdOpen.left = new FormAttachment(wNew, PropsUi.getMargin());
    fdOpen.top = new FormAttachment(0, 0);
    wOpen.setLayoutData(fdOpen);
    PropsUi.setLook(wOpen);

    wSave = new Button(parent, SWT.PUSH);
    wSave.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Save"));
    wSave.addListener(SWT.Selection, e -> save());
    FormData fdSave = new FormData();
    fdSave.left = new FormAttachment(wOpen, PropsUi.getMargin());
    fdSave.top = new FormAttachment(0, 0);
    wSave.setLayoutData(fdSave);
    PropsUi.setLook(wSave);

    wSaveAs = new Button(parent, SWT.PUSH);
    wSaveAs.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.SaveAs"));
    wSaveAs.addListener(SWT.Selection, e -> saveAs());
    FormData fdSaveAs = new FormData();
    fdSaveAs.left = new FormAttachment(wSave, PropsUi.getMargin());
    fdSaveAs.top = new FormAttachment(0, 0);
    wSaveAs.setLayoutData(fdSaveAs);
    PropsUi.setLook(wSaveAs);

    wEditYaml = new Button(parent, SWT.PUSH);
    wEditYaml.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.EditYaml"));
    wEditYaml.setToolTipText(
        BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.EditYaml.Tooltip"));
    wEditYaml.addListener(SWT.Selection, e -> editYamlInExplorer());
    FormData fdYaml = new FormData();
    fdYaml.left = new FormAttachment(wSaveAs, PropsUi.getMargin());
    fdYaml.top = new FormAttachment(0, 0);
    wEditYaml.setLayoutData(fdYaml);
    PropsUi.setLook(wEditYaml);

    wReload = new Button(parent, SWT.PUSH);
    wReload.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Reload"));
    wReload.setToolTipText(
        BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Reload.Tooltip"));
    wReload.addListener(SWT.Selection, e -> reloadFromDisk());
    FormData fdReload = new FormData();
    fdReload.left = new FormAttachment(wEditYaml, PropsUi.getMargin());
    fdReload.top = new FormAttachment(0, 0);
    wReload.setLayoutData(fdReload);
    PropsUi.setLook(wReload);

    wFileCombo = new Combo(parent, SWT.DROP_DOWN | SWT.BORDER);
    PropsUi.setLook(wFileCombo);
    FormData fdFile = new FormData();
    fdFile.left = new FormAttachment(wReload, PropsUi.getMargin() * 2);
    fdFile.right = new FormAttachment(100, 0);
    fdFile.top = new FormAttachment(wNew, 0, SWT.CENTER);
    wFileCombo.setLayoutData(fdFile);
    refreshFileComboItems(Const.NVL(currentFilename, ""));
    wFileCombo.addListener(SWT.Selection, e -> onFileComboSelected());
    wFileCombo.addListener(SWT.DefaultSelection, e -> onFileComboSelected());

    Label wToolbarSep = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdToolbarSep = new FormData();
    fdToolbarSep.left = new FormAttachment(0, 0);
    fdToolbarSep.right = new FormAttachment(100, 0);
    fdToolbarSep.top = new FormAttachment(wNew, PropsUi.getMargin());
    wToolbarSep.setLayoutData(fdToolbarSep);

    wFullClientBanner = new Label(parent, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wFullClientBanner);
    wFullClientBanner.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Guidance"));
    FormData fdBanner = new FormData();
    fdBanner.left = new FormAttachment(0, 0);
    fdBanner.right = new FormAttachment(100, 0);
    fdBanner.top = new FormAttachment(wToolbarSep, PropsUi.getMargin());
    wFullClientBanner.setLayoutData(fdBanner);

    // Bottom buttons
    Button wApply = new Button(parent, SWT.PUSH);
    wApply.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Apply"));
    boolean canManage = MarketplaceSecurity.canManagePlugins();
    wApply.setEnabled(canManage);
    wApply.setToolTipText(
        canManage
            ? BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Apply.Tooltip")
            : BaseMessages.getString(PKG, "MarketplaceDialog.Button.Install.RequiresAdmin"));
    wApply.addListener(SWT.Selection, e -> apply());

    Button wValidate = new Button(parent, SWT.PUSH);
    wValidate.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Validate"));
    wValidate.setToolTipText(
        BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Validate.Tooltip"));
    wValidate.addListener(SWT.Selection, e -> validate());

    Button wClose = null;
    if (includeClose) {
      wClose = new Button(parent, SWT.PUSH);
      wClose.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Close"));
      wClose.addListener(SWT.Selection, e -> close());
      BaseTransformDialog.positionBottomButtons(
          parent, new Button[] {wValidate, wApply, wClose}, PropsUi.getMargin(), null);
    } else {
      BaseTransformDialog.positionBottomButtons(
          parent, new Button[] {wValidate, wApply}, PropsUi.getMargin(), null);
    }

    wPrune = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wPrune);
    wPrune.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Prune"));
    wPrune.setToolTipText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Prune.Tooltip"));
    wPrune.setEnabled(canManage);
    FormData fdPrune = new FormData();
    fdPrune.left = new FormAttachment(0, 0);
    fdPrune.bottom = new FormAttachment(wValidate, -PropsUi.getMargin());
    wPrune.setLayoutData(fdPrune);

    wStrict = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wStrict);
    wStrict.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Strict"));
    wStrict.setToolTipText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Strict.Tooltip"));
    FormData fdStrict = new FormData();
    fdStrict.left = new FormAttachment(wPrune, PropsUi.getMargin() * 3);
    fdStrict.bottom = new FormAttachment(wValidate, -PropsUi.getMargin());
    wStrict.setLayoutData(fdStrict);

    CTabFolder wTabFolder = new CTabFolder(parent, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(wFullClientBanner, PropsUi.getMargin());
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wPrune, -PropsUi.getMargin());
    wTabFolder.setLayoutData(fdTabs);

    createGeneralTab(wTabFolder);
    createRepositoriesTab(wTabFolder);
    createPluginsTab(wTabFolder);
    createDependenciesTab(wTabFolder);
    wTabFolder.setSelection(0);

    updateYamlButtons();
    applyExplorerModeVisibility();

    // Load initial content (dialog mode only; embed() loads after createControls)
    if (!embedded) {
      if (HopInstallSpecFiles.exists(currentFilename, variables())) {
        loadFromFilename(currentFilename);
      } else {
        loadSpec(newEmptySpec(), StringUtils.isBlank(currentFilename) ? null : currentFilename);
      }
    }
  }

  private void createGeneralTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Tab.General"));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.General.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    int middle = 30;
    Label wlVersion = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlVersion);
    wlVersion.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.General.Version"));
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
    wlHopVersion.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.General.HopVersion"));
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
    wEnforceOnRun.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.General.EnforceOnRun"));
    wEnforceOnRun.setToolTipText(
        BaseMessages.getString(PKG, "HopInstallSpecEditor.General.EnforceOnRun.Tooltip"));
    FormData fdEnforce = new FormData();
    fdEnforce.left = new FormAttachment(middle, 0);
    fdEnforce.top = new FormAttachment(wHopVersion, PropsUi.getMargin() * 2);
    wEnforceOnRun.setLayoutData(fdEnforce);
    wEnforceOnRun.addListener(SWT.Selection, e -> markDirty());
  }

  private void createRepositoriesTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Tab.Repositories"));
    tab.setImage(GuiResource.getInstance().getImageRepository());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Repositories.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wAdd = sideButton(comp, "HopInstallSpecEditor.Button.Add", wlHelp);
    Button wEdit = sideButton(comp, "HopInstallSpecEditor.Button.Edit", wAdd);
    Button wRemove = sideButton(comp, "HopInstallSpecEditor.Button.Remove", wEdit);
    Button wUp = sideButton(comp, "HopInstallSpecEditor.Button.MoveUp", wRemove);
    Button wDown = sideButton(comp, "HopInstallSpecEditor.Button.MoveDown", wUp);
    Button wImport = sideButton(comp, "HopInstallSpecEditor.Button.ImportRepos", wDown);

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
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Repo.Column.Id"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Repo.Column.Url"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Repo.Column.Username"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Repo.Column.Auth"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
            });
    wRepos.table.addListener(SWT.DefaultSelection, e -> editRepository());
  }

  private void createPluginsTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Tab.Plugins"));
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
    wlHelp.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Plugins.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wCatalog = sideButton(comp, "HopInstallSpecEditor.Button.AddFromCatalog", wlHelp, null);
    Button wAdd = sideButton(comp, "HopInstallSpecEditor.Button.Add", wCatalog, wCatalog);
    Button wEdit = sideButton(comp, "HopInstallSpecEditor.Button.Edit", wAdd, wCatalog);
    Button wRemove = sideButton(comp, "HopInstallSpecEditor.Button.Remove", wEdit, wCatalog);
    Button wUp = sideButton(comp, "HopInstallSpecEditor.Button.MoveUp", wRemove, wCatalog);
    Button wDown = sideButton(comp, "HopInstallSpecEditor.Button.MoveDown", wUp, wCatalog);

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
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Plugin.Column.GroupId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Plugin.Column.ArtifactId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Plugin.Column.Version"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
            });
    wPlugins.table.addListener(SWT.DefaultSelection, e -> editPlugin());
  }

  private void createDependenciesTab(CTabFolder folder) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Tab.Dependencies"));
    tab.setImage(GuiResource.getInstance().getImageDependence());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Dependencies.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Button wAdd = sideButton(comp, "HopInstallSpecEditor.Button.Add", wlHelp);
    Button wEdit = sideButton(comp, "HopInstallSpecEditor.Button.Edit", wAdd);
    Button wRemove = sideButton(comp, "HopInstallSpecEditor.Button.Remove", wEdit);
    Button wUp = sideButton(comp, "HopInstallSpecEditor.Button.MoveUp", wRemove);
    Button wDown = sideButton(comp, "HopInstallSpecEditor.Button.MoveDown", wUp);

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
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Dep.Column.GroupId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Dep.Column.ArtifactId"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Dep.Column.Version"),
                  ColumnInfo.COLUMN_TYPE_TEXT,
                  false,
                  true),
              new ColumnInfo(
                  BaseMessages.getString(PKG, "HopInstallSpecEditor.Dep.Column.Target"),
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

  private static HopInstallSpec newEmptySpec() {
    HopInstallSpec spec = new HopInstallSpec();
    spec.setVersion("1.0");
    return spec;
  }

  private void loadFromFilename(String filename) {
    try {
      HopInstallSpec spec = HopInstallSpecLoader.load(filename, variables());
      loadSpec(spec, filename);
      rememberEnvFile(filename);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"),
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Load", filename),
          e);
    }
  }

  private void loadSpec(HopInstallSpec spec, String filename) {
    ignoreDirtyEvents = true;
    try {
      currentFilename = filename;
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
      updateGuidance();
      updateYamlButtons();
    } finally {
      ignoreDirtyEvents = false;
    }
  }

  private void fillRepos(List<HopInstallSpec.RepositoryRef> list) {
    wRepos.table.removeAll();
    if (list == null) {
      return;
    }
    for (HopInstallSpec.RepositoryRef ref : list) {
      if (ref == null) {
        continue;
      }
      addRepoItem(ref);
    }
  }

  private void addRepoItem(HopInstallSpec.RepositoryRef ref) {
    TableItem item = new TableItem(wRepos.table, SWT.NONE);
    item.setText(1, Const.NVL(ref.getId(), ""));
    item.setText(2, Const.NVL(ref.getUrl(), ""));
    item.setText(3, Const.NVL(ref.getUsername(), ""));
    item.setText(4, StringUtils.isNotBlank(ref.getPassword()) ? "Y" : "");
    item.setData(ref);
  }

  private void fillPlugins(List<HopInstallSpec.PluginRef> list) {
    wPlugins.table.removeAll();
    if (list == null) {
      return;
    }
    for (HopInstallSpec.PluginRef ref : list) {
      if (ref == null) {
        continue;
      }
      addPluginItem(ref);
    }
  }

  private void addPluginItem(HopInstallSpec.PluginRef ref) {
    TableItem item = new TableItem(wPlugins.table, SWT.NONE);
    item.setText(1, Const.NVL(ref.getGroupId(), ""));
    item.setText(2, Const.NVL(ref.getArtifactId(), ""));
    item.setText(3, Const.NVL(ref.getVersion(), ""));
    item.setData(ref);
  }

  private void fillDependencies(List<HopInstallSpec.DependencyRef> list) {
    wDependencies.table.removeAll();
    if (list == null) {
      return;
    }
    for (HopInstallSpec.DependencyRef ref : list) {
      if (ref == null) {
        continue;
      }
      addDepItem(ref);
    }
  }

  private void addDepItem(HopInstallSpec.DependencyRef ref) {
    TableItem item = new TableItem(wDependencies.table, SWT.NONE);
    item.setText(1, Const.NVL(ref.getGroupId(), ""));
    item.setText(2, Const.NVL(ref.getArtifactId(), ""));
    item.setText(3, Const.NVL(ref.getVersion(), ""));
    item.setText(4, Const.NVL(ref.getTarget(), "lib/jdbc"));
    item.setData(ref);
  }

  private HopInstallSpec collectSpec() {
    HopInstallSpec spec = new HopInstallSpec();
    spec.setVersion(StringUtils.defaultIfBlank(wVersion.getText(), "1.0").trim());
    String hopVer = wHopVersion.getText();
    spec.setHopVersion(StringUtils.isBlank(hopVer) ? null : hopVer.trim());
    spec.setEnforceOnRun(wEnforceOnRun.getSelection());

    List<HopInstallSpec.RepositoryRef> repos = new ArrayList<>();
    for (TableItem item : wRepos.table.getItems()) {
      if (item.getData() instanceof HopInstallSpec.RepositoryRef ref) {
        repos.add(ref);
      }
    }
    spec.setRepositories(repos);

    List<HopInstallSpec.PluginRef> plugins = new ArrayList<>();
    for (TableItem item : wPlugins.table.getItems()) {
      if (item.getData() instanceof HopInstallSpec.PluginRef ref) {
        plugins.add(ref);
      }
    }
    spec.setPlugins(plugins);

    List<HopInstallSpec.DependencyRef> deps = new ArrayList<>();
    for (TableItem item : wDependencies.table.getItems()) {
      if (item.getData() instanceof HopInstallSpec.DependencyRef ref) {
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
      if (dirtyListener != null) {
        dirtyListener.run();
      }
    }
  }

  private void updateTitle() {
    if (embedded) {
      return;
    }
    String name =
        StringUtils.isNotBlank(currentFilename)
            ? HopInstallSpecFiles.baseName(currentFilename)
            : BaseMessages.getString(PKG, "HopInstallSpecEditor.Untitled");
    String title =
        BaseMessages.getString(PKG, "HopInstallSpecEditor.Shell.Title", name) + (dirty ? " *" : "");
    if (shell != null && !shell.isDisposed()) {
      shell.setText(title);
    }
  }

  private void updateFileLabel() {
    if (wFileCombo == null || wFileCombo.isDisposed()) {
      return;
    }
    if (StringUtils.isNotBlank(currentFilename)) {
      refreshFileComboItems(currentFilename);
    } else {
      refreshFileComboItems("");
      wFileCombo.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Untitled"));
    }
  }

  private void updateGuidance() {
    if (wFullClientBanner == null || wFullClientBanner.isDisposed()) {
      return;
    }
    if (HopInstallSpecFiles.isFullClient(currentFilename)) {
      wFullClientBanner.setText(
          BaseMessages.getString(PKG, "HopInstallSpecEditor.FullClient.Banner"));
    } else if (StringUtils.isBlank(currentFilename)) {
      wFullClientBanner.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Guidance.New"));
    } else {
      wFullClientBanner.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Guidance"));
    }
  }

  private void updateYamlButtons() {
    boolean hasFile = StringUtils.isNotBlank(currentFilename);
    if (wEditYaml != null && !wEditYaml.isDisposed()) {
      wEditYaml.setEnabled(hasFile && !includeCloseIsModalOnly());
    }
    if (wReload != null && !wReload.isDisposed()) {
      wReload.setEnabled(hasFile);
    }
  }

  private boolean includeCloseIsModalOnly() {
    return !embedded;
  }

  private void applyExplorerModeVisibility() {
    boolean showFileButtons = !explorerMode;
    if (wNew != null && !wNew.isDisposed()) {
      wNew.setVisible(showFileButtons);
    }
    if (wOpen != null && !wOpen.isDisposed()) {
      wOpen.setVisible(showFileButtons);
    }
    if (wSave != null && !wSave.isDisposed()) {
      wSave.setVisible(showFileButtons);
    }
    if (wSaveAs != null && !wSaveAs.isDisposed()) {
      wSaveAs.setVisible(showFileButtons);
    }
    if (wEditYaml != null && !wEditYaml.isDisposed()) {
      wEditYaml.setVisible(embedded);
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
      log.logError("Unable to load marketplace install spec file audit list", e);
      wFileCombo.setItems(AuditManagerGuiUtil.getLastUsedValues(AUDIT_TYPE_ENV_FILES));
    }
    if (StringUtils.isNotBlank(selectedPath)) {
      wFileCombo.setText(selectedPath);
    }
  }

  /** Remember path in the audit list (most-recent first) and refresh the combo. */
  private void rememberEnvFile(String filename) {
    if (StringUtils.isBlank(filename)) {
      return;
    }
    AuditManagerGuiUtil.addLastUsedValue(AUDIT_TYPE_ENV_FILES, filename);
    refreshFileComboItems(filename);
  }

  private void onFileComboSelected() {
    if (wFileCombo == null || wFileCombo.isDisposed()) {
      return;
    }
    String text = wFileCombo.getText();
    if (StringUtils.isBlank(text)) {
      return;
    }
    String filename = text.trim();
    if (filename.equals(currentFilename)) {
      return;
    }
    if (!HopInstallSpecFiles.exists(filename, variables())) {
      if (confirmDiscardIfDirty()) {
        updateFileLabel();
        return;
      }
      currentFilename = filename;
      updateFileLabel();
      updateGuidance();
      updateYamlButtons();
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.NewPath.Header"));
      box.setMessage(BaseMessages.getString(PKG, "HopInstallSpecEditor.NewPath.Message", filename));
      box.open();
      markDirty();
      return;
    }
    if (confirmDiscardIfDirty()) {
      updateFileLabel();
      return;
    }
    loadFromFilename(filename);
  }

  private boolean confirmDiscardIfDirty() {
    if (!dirty) {
      return false;
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_WARNING);
    box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Dirty.Header"));
    box.setMessage(BaseMessages.getString(PKG, "HopInstallSpecEditor.Dirty.Message"));
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
    String path = presentSpecFileDialog(false);
    if (StringUtils.isBlank(path)) {
      return;
    }
    if (!HopInstallSpecFiles.exists(path, variables())) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.NotFound.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.NotFound.Message", path));
      box.open();
      return;
    }
    loadFromFilename(path);
  }

  private boolean save() {
    if (StringUtils.isBlank(currentFilename)) {
      return saveAs();
    }
    return writeTo(currentFilename);
  }

  private boolean saveAs() {
    String path = presentSpecFileDialog(true);
    if (StringUtils.isBlank(path)) {
      return false;
    }
    return writeTo(HopInstallSpecFiles.ensureSpecExtension(path));
  }

  private String presentSpecFileDialog(boolean save) {
    IVariables vars = variables();
    FileObject start = null;
    try {
      if (StringUtils.isNotBlank(currentFilename)) {
        start = HopVfs.getFileObject(HopInstallSpecFiles.resolve(currentFilename, vars), vars);
      } else {
        String folder = HopInstallSpecFiles.defaultSaveFolder(vars, hopHome);
        start = HopVfs.getFileObject(folder + "/" + HopInstallSpecFiles.DEFAULT_FILENAME, vars);
      }
    } catch (Exception e) {
      start = null;
    }
    String[] filterExtensions =
        save
            ? new String[] {"*.yaml;*.yml", "*.json", "*.*"}
            : new String[] {"*.yaml;*.yml;*.json", "*.*"};
    String[] filterNames =
        save
            ? new String[] {
              BaseMessages.getString(PKG, "HopInstallSpecEditor.Filter.Yaml"),
              BaseMessages.getString(PKG, "HopInstallSpecEditor.Filter.Json"),
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
            }
            : new String[] {
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.Env"),
              BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
            };
    return BaseDialog.presentFileDialog(
        save, shell, null, vars, start, filterExtensions, filterNames, false);
  }

  private boolean writeTo(String filename) {
    try {
      HopInstallSpec spec = collectSpec();
      String validationError = validateSpec(spec);
      if (validationError != null) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.Header"));
        box.setMessage(validationError);
        box.open();
        return false;
      }
      HopInstallSpecLoader.save(filename, spec, variables());
      currentFilename = filename;
      resultFilename = filename;
      saved = true;
      dirty = false;
      rememberEnvFile(filename);
      updateFileLabel();
      updateTitle();
      updateGuidance();
      updateYamlButtons();
      return true;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"),
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Save", filename),
          e);
      return false;
    }
  }

  public boolean saveCurrent() {
    return save();
  }

  private IVariables variables() {
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null && hopGui.getVariables() != null) {
        return hopGui.getVariables();
      }
    } catch (Exception ignored) {
      // headless / tests
    }
    return Variables.getADefaultVariableSpace();
  }

  public void reloadFromDisk() {
    if (StringUtils.isBlank(currentFilename)) {
      return;
    }
    if (confirmDiscardIfDirty()) {
      return;
    }
    loadFromFilename(currentFilename);
  }

  private void editYamlInExplorer() {
    if (StringUtils.isBlank(currentFilename)) {
      return;
    }
    if (dirty) {
      if (confirmDiscardIfDirty()) {
        return;
      }
    }
    try {
      HopGui hopGui = HopGui.getInstance();
      String resolved = HopInstallSpecFiles.resolve(currentFilename, variables());
      hopGui.fileDelegate.fileOpenWithType(resolved, new YamlExplorerFileType(), true);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"),
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.OpenYaml", currentFilename),
          e);
    }
  }

  private String validateSpec(HopInstallSpec spec) {
    for (HopInstallSpec.RepositoryRef ref : spec.getRepositories()) {
      if (StringUtils.isBlank(ref.getId()) || StringUtils.isBlank(ref.getUrl())) {
        return BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.RepoIdUrl");
      }
    }
    for (HopInstallSpec.PluginRef ref : spec.getPlugins()) {
      if (StringUtils.isBlank(ref.getArtifactId())) {
        return BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.PluginArtifact");
      }
    }
    for (HopInstallSpec.DependencyRef ref : spec.getDependencies()) {
      if (StringUtils.isAnyBlank(ref.getGroupId(), ref.getArtifactId(), ref.getVersion())) {
        return BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.DepGav");
      }
    }
    return null;
  }

  private boolean ensureSavedForAction() {
    if (dirty || StringUtils.isBlank(currentFilename)) {
      MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.SaveFirst.Header"));
      box.setMessage(BaseMessages.getString(PKG, "HopInstallSpecEditor.SaveFirst.Message"));
      if (box.open() != SWT.YES) {
        return true;
      }
      if (!save()) {
        return true;
      }
    }
    return !HopInstallSpecFiles.exists(currentFilename, variables());
  }

  private void validate() {
    if (ensureSavedForAction()) {
      return;
    }
    try {
      HopInstallSpec env = HopInstallSpecLoader.load(currentFilename, variables());
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
            BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Ok.Message", currentFilename));
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
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Validate.Error"),
          e);
    }
  }

  private void apply() {
    if (!MarketplaceSecurity.checkManagePlugins()) {
      return;
    }
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
        box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"));
        box.setMessage(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Disabled"));
        box.open();
        return;
      }
      HopInstallSpec env = HopInstallSpecLoader.load(currentFilename, variables());
      if (!runApply(env, live, prune)) {
        notifyStatus(BaseMessages.getString(PKG, "MarketplaceDialog.Status.Cancelled"));
        return;
      }
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Done.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Done.Message", currentFilename));
      box.open();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      showApplyError(e);
    } catch (Exception e) {
      showApplyError(e);
    }
  }

  private void showApplyError(Exception e) {
    // ProgressMonitorDialog wraps the real failure; unwrap so the dialog shows the cause.
    Throwable cause =
        e instanceof InvocationTargetException ite && ite.getCause() != null ? ite.getCause() : e;
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"),
        BaseMessages.getString(PKG, "MarketplaceDialog.Apply.Error"),
        cause);
  }

  /**
   * Apply the environment on a worker thread behind a cancellable progress dialog. This path can
   * install many plugins and download many jars, so running it inline froze the GUI for minutes.
   *
   * @return true when the environment was applied, false when the user cancelled
   */
  private boolean runApply(HopInstallSpec env, MarketplaceConfig live, boolean prune)
      throws InvocationTargetException, InterruptedException {
    EnvironmentApplier applier = new EnvironmentApplier(log, hopHome, live);
    AtomicBoolean cancelled = new AtomicBoolean(false);

    IRunnableWithProgress operation =
        monitor -> {
          ProgressMonitorInstallListener listener = new ProgressMonitorInstallListener(monitor);
          // No item name up front: apply() names each artifact via IInstallListener.item().
          listener.begin(BaseMessages.getString(PKG, "MarketplaceDialog.Progress.Applying"), null);
          try {
            applier.apply(env, prune, listener);
          } catch (Exception e) {
            if (!monitor.isCanceled()) {
              // Throw WITHOUT calling monitor.done(): done() disposes the progress shell and ends
              // ProgressMonitorDialog's pump loop before it can observe the exception, which would
              // report a failed apply as a success. Catching Exception rather than HopException
              // keeps a RuntimeException from killing the worker and hanging the dialog.
              throw new InvocationTargetException(e, e.getMessage());
            }
            cancelled.set(true);
          }
          listener.complete();
          monitor.done();
        };

    new ProgressMonitorDialog(shell).run(true, operation);
    return !cancelled.get();
  }

  private void populateExtraPlugins(HopInstallSpec env, EnvironmentDrift drift) throws Exception {
    Set<String> desired = new HashSet<>();
    if (env.getPlugins() != null) {
      for (HopInstallSpec.PluginRef ref : env.getPlugins()) {
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
    HopInstallSpec.RepositoryRef ref = new HopInstallSpec.RepositoryRef();
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
    HopInstallSpec.RepositoryRef ref = (HopInstallSpec.RepositoryRef) sel[0].getData();
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

  private boolean editRepositoryDialog(HopInstallSpec.RepositoryRef ref, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew ? "HopInstallSpecEditor.Repo.Edit.Add" : "HopInstallSpecEditor.Repo.Edit.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);
    int middle = 30;
    int margin = PropsUi.getMargin();

    Label wlId = labeled(dialog, "HopInstallSpecEditor.Repo.Column.Id", 0, middle, margin);
    Text wId = textField(dialog, wlId, middle, Const.NVL(ref.getId(), ""));
    Label wlUrl = labeled(dialog, "HopInstallSpecEditor.Repo.Column.Url", wId, middle, margin);
    Text wUrl = textField(dialog, wlUrl, middle, Const.NVL(ref.getUrl(), ""));
    Label wlUser =
        labeled(dialog, "HopInstallSpecEditor.Repo.Column.Username", wUrl, middle, margin);
    Text wUser = textField(dialog, wlUser, middle, Const.NVL(ref.getUsername(), ""));
    Label wlPass =
        labeled(dialog, "HopInstallSpecEditor.Repo.Column.Password", wUser, middle, margin);
    Text wPass = new Text(dialog, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wPass);
    wPass.setText(Const.NVL(ref.getPassword(), ""));
    String passwordTooltip =
        BaseMessages.getString(PKG, "HopInstallSpecEditor.Repo.Column.Password.Tooltip");
    wlPass.setToolTipText(passwordTooltip);
    wPass.setToolTipText(passwordTooltip);
    FormData fdPass = new FormData();
    fdPass.left = new FormAttachment(middle, 0);
    fdPass.top = new FormAttachment(wlPass, 0, SWT.CENTER);
    fdPass.right = new FormAttachment(100, 0);
    wPass.setLayoutData(fdPass);

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isBlank(wId.getText()) || StringUtils.isBlank(wUrl.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.Header"));
            box.setMessage(
                BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.RepoIdUrl"));
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
      confirm.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.ImportRepos.Header"));
      confirm.setMessage(BaseMessages.getString(PKG, "HopInstallSpecEditor.ImportRepos.Message"));
      if (confirm.open() != SWT.YES) {
        return;
      }
      wRepos.table.removeAll();
      for (MarketplaceRepository repo : live.getRepositories()) {
        if (repo == null || !repo.isEnabled()) {
          continue;
        }
        HopInstallSpec.RepositoryRef ref = new HopInstallSpec.RepositoryRef();
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
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.Header"),
          BaseMessages.getString(PKG, "HopInstallSpecEditor.Error.ImportRepos"),
          e);
    }
  }

  // --- plugin CRUD ---

  private void addPlugin() {
    HopInstallSpec.PluginRef ref = new HopInstallSpec.PluginRef();
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
    HopInstallSpec.PluginRef ref = (HopInstallSpec.PluginRef) sel[0].getData();
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

  private boolean editPluginDialog(HopInstallSpec.PluginRef ref, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew
                ? "HopInstallSpecEditor.Plugin.Edit.Add"
                : "HopInstallSpecEditor.Plugin.Edit.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);
    int middle = 30;
    int margin = PropsUi.getMargin();

    Label wlGroup =
        labeled(dialog, "HopInstallSpecEditor.Plugin.Column.GroupId", 0, middle, margin);
    Text wGroup = textField(dialog, wlGroup, middle, Const.NVL(ref.getGroupId(), ""));
    Label wlArt =
        labeled(dialog, "HopInstallSpecEditor.Plugin.Column.ArtifactId", wGroup, middle, margin);
    Text wArt = textField(dialog, wlArt, middle, Const.NVL(ref.getArtifactId(), ""));
    Label wlVer =
        labeled(dialog, "HopInstallSpecEditor.Plugin.Column.Version", wArt, middle, margin);
    Text wVer = textField(dialog, wlVer, middle, Const.NVL(ref.getVersion(), ""));

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isBlank(wArt.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.Header"));
            box.setMessage(
                BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.PluginArtifact"));
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
      if (item.getData() instanceof HopInstallSpec.PluginRef ref
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
      HopInstallSpec.PluginRef ref = new HopInstallSpec.PluginRef();
      ref.setArtifactId(info.getArtifactId());
      ref.setVersion(defaultVersion);
      addPluginItem(ref);
    }
    markDirty();
  }

  // --- dependency CRUD ---

  private void addDependency() {
    HopInstallSpec.DependencyRef ref = new HopInstallSpec.DependencyRef();
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
    HopInstallSpec.DependencyRef ref = (HopInstallSpec.DependencyRef) sel[0].getData();
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

  private boolean editDependencyDialog(HopInstallSpec.DependencyRef ref, boolean isNew) {
    Shell dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    PropsUi.setLook(dialog);
    dialog.setText(
        BaseMessages.getString(
            PKG,
            isNew ? "HopInstallSpecEditor.Dep.Edit.Add" : "HopInstallSpecEditor.Dep.Edit.Edit"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    dialog.setLayout(layout);
    int middle = 30;
    int margin = PropsUi.getMargin();

    Label wlGroup = labeled(dialog, "HopInstallSpecEditor.Dep.Column.GroupId", 0, middle, margin);
    Text wGroup = textField(dialog, wlGroup, middle, Const.NVL(ref.getGroupId(), ""));
    Label wlArt =
        labeled(dialog, "HopInstallSpecEditor.Dep.Column.ArtifactId", wGroup, middle, margin);
    Text wArt = textField(dialog, wlArt, middle, Const.NVL(ref.getArtifactId(), ""));
    Label wlVer = labeled(dialog, "HopInstallSpecEditor.Dep.Column.Version", wArt, middle, margin);
    Text wVer = textField(dialog, wlVer, middle, Const.NVL(ref.getVersion(), ""));
    Label wlTarget =
        labeled(dialog, "HopInstallSpecEditor.Dep.Column.Target", wVer, middle, margin);
    Text wTarget = textField(dialog, wlTarget, middle, Const.NVL(ref.getTarget(), "lib/jdbc"));

    final boolean[] ok = {false};
    Button wOk = new Button(dialog, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Ok"));
    Button wCancel = new Button(dialog, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dialog.dispose());
    wOk.addListener(
        SWT.Selection,
        e -> {
          if (StringUtils.isAnyBlank(wGroup.getText(), wArt.getText(), wVer.getText())) {
            MessageBox box = new MessageBox(dialog, SWT.OK | SWT.ICON_WARNING);
            box.setText(BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.Header"));
            box.setMessage(BaseMessages.getString(PKG, "HopInstallSpecEditor.Validation.DepGav"));
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
