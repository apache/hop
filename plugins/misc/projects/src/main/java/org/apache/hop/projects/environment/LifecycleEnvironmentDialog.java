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

package org.apache.hop.projects.environment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.RegionalSettingsPreview;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.AttributesDialogExtension;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.HopDescribedVariablesDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboFilterPopup;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class LifecycleEnvironmentDialog extends Dialog {
  private static final Class<?> PKG = LifecycleEnvironmentDialog.class;
  public static final String CONST_ERROR = "Error";

  private final LifecycleEnvironment environment;

  private String returnValue;

  private Shell shell;
  private final PropsUi props;

  private Text wName;
  private Combo wPurpose;
  private Combo wProject;
  private Text wCanvasText;
  private Combo wFormatLocale;
  private Combo wTimeZone;
  private Label wNumberPreview;
  private Label wDatePreview;
  private Label wTimeZonePreview;
  private List<Locale> localeList;
  private TableView wConfigFiles;

  private IVariables variables;
  private Button wbEdit;
  private Button wbImportVariables;

  private String originalName;

  private boolean needingEnvironmentRefresh;

  private AttributesDialogExtension dialogExtension;

  /** Last name we auto-suggested (for detecting when the user edits away from it). */
  private String lastSuggestedName;

  /** When true, project/purpose changes rewrite the name field (new environments only). */
  private boolean nameAutoManaged;

  private boolean updatingSuggestedName;

  /** Skip environment-refresh flags while widgets are populated from the environment. */
  private boolean loadingData;

  /** Localized purpose label → fixed English suffix (for new-environment name suggestion). */
  private Map<String, String> knownPurposeSuffixes;

  public LifecycleEnvironmentDialog(
      Shell parent, LifecycleEnvironment environment, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);

    this.environment = environment;
    this.variables = variables;

    this.originalName = environment.getName();

    props = PropsUi.getInstance();

    needingEnvironmentRefresh = false;
    lastSuggestedName = null;
    nameAutoManaged = StringUtils.isEmpty(originalName);
    updatingSuggestedName = false;
  }

  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "environment.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin() + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Shell.Name"));

    // Buttons go at the bottom of the dialog
    //
    Button wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOK.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOK, wCancel}, margin * 3, null);
    HelpUtils.createHelpButton(shell, Const.getDocUrl(Defaults.DOCUMENTATION_URI));

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wOK, -margin * 2);
    wTabFolder.setLayoutData(fdTabs);

    createGeneralTab(wTabFolder, margin);
    createRegionalTab(wTabFolder, margin);
    createConfigurationFilesTab(wTabFolder, margin);

    // Optional plugins (marketplace, resource checks, …) contribute extra tabs
    AttributesContext attributesContext = new AttributesContext(environment);
    attributesContext.setProjectName(environment.getProjectName());
    attributesContext.setEnvironmentName(environment.getName());
    attributesContext.setPurpose(environment.getPurpose());
    // Tabs resolve project relative paths against this; without it they fall back to the Hop
    // install directory (issue #8012).
    attributesContext.setProjectHome(projectHomeOf(environment.getProjectName()));
    if (environment.getConfigurationFiles() != null) {
      attributesContext.setConfigurationFiles(new ArrayList<>(environment.getConfigurationFiles()));
    }
    dialogExtension =
        new AttributesDialogExtension(shell, wTabFolder, variables, attributesContext);
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.UI,
          variables,
          HopExtensionPoint.HopGuiLifecycleEnvironmentDialogTabs.id,
          dialogExtension);
    } catch (Exception e) {
      new ErrorDialog(shell, CONST_ERROR, "Error loading optional environment dialog tabs", e);
    }

    wTabFolder.setSelection(0);

    getData();
    if (dialogExtension != null) {
      dialogExtension.runLoadCallbacks();
    }

    wName.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return returnValue;
  }

  private void createGeneralTab(CTabFolder folder, int margin) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Tab.General"));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    int middle = props.getMiddlePct();

    Label wlName = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Label.EnvironmentName"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    wName.setLayoutData(fdName);
    wName.addListener(SWT.Modify, e -> onNameModified());

    Label wlPurpose = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlPurpose);
    wlPurpose.setText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Label.EnvironmentPurpose"));
    FormData fdlPurpose = new FormData();
    fdlPurpose.left = new FormAttachment(0, 0);
    fdlPurpose.right = new FormAttachment(middle, 0);
    fdlPurpose.top = new FormAttachment(wName, margin);
    wlPurpose.setLayoutData(fdlPurpose);
    wPurpose = new Combo(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wPurpose);
    FormData fdPurpose = new FormData();
    fdPurpose.left = new FormAttachment(middle, margin);
    fdPurpose.right = new FormAttachment(100, 0);
    fdPurpose.top = new FormAttachment(wlPurpose, 0, SWT.CENTER);
    wPurpose.setLayoutData(fdPurpose);
    wPurpose.addListener(
        SWT.Modify,
        e -> {
          needingEnvironmentRefresh = true;
          updateSuggestedName();
        });

    Label wlProject = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlProject);
    wlProject.setText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Label.ReferencedProject"));
    FormData fdlProject = new FormData();
    fdlProject.left = new FormAttachment(0, 0);
    fdlProject.right = new FormAttachment(middle, 0);
    fdlProject.top = new FormAttachment(wPurpose, margin);
    wlProject.setLayoutData(fdlProject);
    wProject = new Combo(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wProject);
    FormData fdProject = new FormData();
    fdProject.left = new FormAttachment(middle, margin);
    fdProject.right = new FormAttachment(100, 0);
    fdProject.top = new FormAttachment(wlProject, 0, SWT.CENTER);
    wProject.setLayoutData(fdProject);
    wProject.addListener(
        SWT.Modify,
        e -> {
          needingEnvironmentRefresh = true;
          updateSuggestedName();
        });

    Label wlCanvasText = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlCanvasText);
    wlCanvasText.setText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Label.CanvasText"));
    FormData fdlCanvasText = new FormData();
    fdlCanvasText.left = new FormAttachment(0, 0);
    fdlCanvasText.right = new FormAttachment(middle, 0);
    fdlCanvasText.top = new FormAttachment(wProject, margin);
    wlCanvasText.setLayoutData(fdlCanvasText);
    wCanvasText = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wCanvasText);
    wCanvasText.setToolTipText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ToolTip.CanvasText"));
    FormData fdCanvasText = new FormData();
    fdCanvasText.left = new FormAttachment(middle, margin);
    fdCanvasText.right = new FormAttachment(100, 0);
    fdCanvasText.top = new FormAttachment(wlCanvasText, 0, SWT.CENTER);
    wCanvasText.setLayoutData(fdCanvasText);
  }

  private void createRegionalTab(CTabFolder folder, int margin) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Tab.Regional"));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    int middle = props.getMiddlePct();

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Regional.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    fdlHelp.top = new FormAttachment(0, margin);
    wlHelp.setLayoutData(fdlHelp);

    Label wlFormatLocale = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlFormatLocale);
    wlFormatLocale.setText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Label.FormatLocale"));
    FormData fdlFormatLocale = new FormData();
    fdlFormatLocale.left = new FormAttachment(0, 0);
    fdlFormatLocale.right = new FormAttachment(middle, 0);
    fdlFormatLocale.top = new FormAttachment(wlHelp, margin * 2);
    wlFormatLocale.setLayoutData(fdlFormatLocale);

    localeList = buildLocaleList();
    wFormatLocale = new Combo(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFormatLocale);
    wFormatLocale.setItems(localeComboItems());
    wFormatLocale.setToolTipText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ToolTip.FormatLocale"));
    FormData fdFormatLocale = new FormData();
    fdFormatLocale.left = new FormAttachment(middle, margin);
    fdFormatLocale.right = new FormAttachment(100, 0);
    fdFormatLocale.top = new FormAttachment(wlFormatLocale, 0, SWT.CENTER);
    wFormatLocale.setLayoutData(fdFormatLocale);
    ComboFilterPopup.attach(wFormatLocale, () -> Arrays.asList(wFormatLocale.getItems()), null);
    wFormatLocale.addListener(SWT.Modify, this::onRegionalChanged);

    Label wlTimeZone = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlTimeZone);
    wlTimeZone.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Label.TimeZone"));
    FormData fdlTimeZone = new FormData();
    fdlTimeZone.left = new FormAttachment(0, 0);
    fdlTimeZone.right = new FormAttachment(middle, 0);
    fdlTimeZone.top = new FormAttachment(wFormatLocale, margin);
    wlTimeZone.setLayoutData(fdlTimeZone);

    wTimeZone = new Combo(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTimeZone);
    wTimeZone.setItems(timeZoneComboItems());
    wTimeZone.setToolTipText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ToolTip.TimeZone"));
    FormData fdTimeZone = new FormData();
    fdTimeZone.left = new FormAttachment(middle, margin);
    fdTimeZone.right = new FormAttachment(100, 0);
    fdTimeZone.top = new FormAttachment(wlTimeZone, 0, SWT.CENTER);
    wTimeZone.setLayoutData(fdTimeZone);
    ComboFilterPopup.attach(wTimeZone, () -> Arrays.asList(wTimeZone.getItems()), null);
    wTimeZone.addListener(SWT.Modify, this::onRegionalChanged);

    Group preview = new Group(comp, SWT.SHADOW_NONE);
    PropsUi.setLook(preview);
    preview.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Regional.Preview"));
    FormLayout previewLayout = new FormLayout();
    previewLayout.marginWidth = PropsUi.getFormMargin();
    previewLayout.marginHeight = PropsUi.getFormMargin();
    preview.setLayout(previewLayout);
    FormData fdPreview = new FormData();
    fdPreview.left = new FormAttachment(0, 0);
    fdPreview.right = new FormAttachment(100, 0);
    fdPreview.top = new FormAttachment(wTimeZone, margin * 2);
    preview.setLayoutData(fdPreview);

    wNumberPreview =
        createPreviewRow(
            preview, "LifecycleEnvironmentDialog.Regional.Preview.Number", null, margin);
    wDatePreview =
        createPreviewRow(
            preview, "LifecycleEnvironmentDialog.Regional.Preview.Date", wNumberPreview, margin);
    wTimeZonePreview =
        createPreviewRow(
            preview, "LifecycleEnvironmentDialog.Regional.Preview.TimeZone", wDatePreview, margin);
  }

  private List<Locale> buildLocaleList() {
    return Arrays.stream(Locale.getAvailableLocales())
        .filter(l -> !l.getCountry().isEmpty())
        .filter(l -> l.getVariant().isEmpty() && l.getScript().isEmpty())
        .sorted(Comparator.comparing(Locale::getDisplayName))
        .toList();
  }

  private String inheritLabel() {
    return BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Regional.Inherit");
  }

  private String[] localeComboItems() {
    String[] items = new String[localeList.size() + 1];
    items[0] = inheritLabel();
    for (int i = 0; i < localeList.size(); i++) {
      items[i + 1] = localeList.get(i).getDisplayName();
    }
    return items;
  }

  private String[] timeZoneComboItems() {
    String[] zones = EnvUtil.getTimeZones();
    String[] items = new String[zones.length + 1];
    items[0] = inheritLabel();
    System.arraycopy(zones, 0, items, 1, zones.length);
    return items;
  }

  private void onRegionalChanged(Event event) {
    if (!loadingData) {
      needingEnvironmentRefresh = true;
    }
    refreshRegionalPreview();
  }

  private void refreshRegionalPreview() {
    if (wNumberPreview == null || wNumberPreview.isDisposed()) {
      return;
    }
    Locale locale = selectedFormatLocale();
    if (locale == null) {
      locale = Locale.getDefault(Locale.Category.FORMAT);
    }
    RegionalSettingsPreview preview = RegionalSettingsPreview.of(locale);
    wNumberPreview.setText(preview.getNumber());
    wDatePreview.setText(preview.getLongDate());

    String timeZoneId = selectedTimeZoneId();
    TimeZone timeZone =
        timeZoneId == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZoneId);
    wTimeZonePreview.setText(timeZone.getID() + " (" + timeZone.getDisplayName() + ")");
  }

  private Locale selectedFormatLocale() {
    int index = wFormatLocale.indexOf(wFormatLocale.getText());
    if (index <= 0) {
      return null;
    }
    return localeList.get(index - 1);
  }

  private String selectedTimeZoneId() {
    String text = wTimeZone.getText();
    if (StringUtils.isEmpty(text) || inheritLabel().equals(text)) {
      return null;
    }
    return text;
  }

  private Label createPreviewRow(Group group, String labelKey, Control lastControl, int margin) {
    Label caption = new Label(group, SWT.RIGHT);
    PropsUi.setLook(caption);
    caption.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdCaption = new FormData();
    fdCaption.left = new FormAttachment(0, 0);
    fdCaption.right = new FormAttachment(PropsUi.getInstance().getMiddlePct(), -margin);
    if (lastControl != null) {
      fdCaption.top = new FormAttachment(lastControl, margin);
    } else {
      fdCaption.top = new FormAttachment(0, margin);
    }
    caption.setLayoutData(fdCaption);

    Label value = new Label(group, SWT.LEFT);
    PropsUi.setLook(value);
    FormData fdValue = new FormData();
    fdValue.left = new FormAttachment(PropsUi.getInstance().getMiddlePct(), 0);
    fdValue.right = new FormAttachment(100, 0);
    fdValue.top = new FormAttachment(caption, 0, SWT.CENTER);
    value.setLayoutData(fdValue);
    return value;
  }

  private void createConfigurationFilesTab(CTabFolder folder, int margin) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Tab.ConfigurationFiles"));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlConfigFiles = new Label(comp, SWT.LEFT);
    PropsUi.setLook(wlConfigFiles);
    wlConfigFiles.setText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Group.Label.ConfigurationFiles"));
    FormData fdlConfigFiles = new FormData();
    fdlConfigFiles.left = new FormAttachment(0, 0);
    fdlConfigFiles.right = new FormAttachment(100, 0);
    fdlConfigFiles.top = new FormAttachment(0, 0);
    wlConfigFiles.setLayoutData(fdlConfigFiles);

    wbImportVariables = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wbImportVariables);
    wbImportVariables.setText(
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Button.ImportVariables"));
    FormData fdImportVariables = new FormData();
    fdImportVariables.right = new FormAttachment(100, 0);
    fdImportVariables.top = new FormAttachment(wlConfigFiles, margin);
    wbImportVariables.setLayoutData(fdImportVariables);
    wbImportVariables.addListener(SWT.Selection, this::importVariables);

    Button wbSelect = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wbSelect);
    wbSelect.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Button.Select"));
    FormData fdAdd = new FormData();
    fdAdd.left = new FormAttachment(wbImportVariables, 0, SWT.LEFT);
    fdAdd.right = new FormAttachment(100, 0);
    fdAdd.top = new FormAttachment(wbImportVariables, margin);
    wbSelect.setLayoutData(fdAdd);
    wbSelect.addListener(SWT.Selection, this::addConfigFile);

    ColumnInfo[] columnInfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.DetailTable.Label.Filename"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columnInfo[0].setUsingVariables(true);

    wConfigFiles =
        new TableView(
            variables,
            comp,
            SWT.SINGLE | SWT.BORDER,
            columnInfo,
            Math.max(environment.getConfigurationFiles().size(), 1),
            null,
            props);
    PropsUi.setLook(wConfigFiles);
    FormData fdConfigFiles = new FormData();
    fdConfigFiles.left = new FormAttachment(0, 0);
    fdConfigFiles.right = new FormAttachment(wbImportVariables, -2 * margin);
    fdConfigFiles.top = new FormAttachment(wlConfigFiles, margin);
    fdConfigFiles.bottom = new FormAttachment(100, 0);
    wConfigFiles.setLayoutData(fdConfigFiles);
    wConfigFiles.table.addListener(SWT.Selection, this::setButtonStates);

    Button wbNew = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wbNew);
    wbNew.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Button.New"));
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment(wConfigFiles, 2 * margin);
    fdNew.right = new FormAttachment(100, 0);
    fdNew.top = new FormAttachment(wbSelect, margin);
    wbNew.setLayoutData(fdNew);
    wbNew.addListener(SWT.Selection, this::newConfigFile);

    wbEdit = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wbEdit);
    wbEdit.setText(BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Button.Edit"));
    FormData fdEdit = new FormData();
    fdEdit.left = new FormAttachment(wConfigFiles, 2 * margin);
    fdEdit.right = new FormAttachment(100, 0);
    fdEdit.top = new FormAttachment(wbNew, margin);
    wbEdit.setLayoutData(fdEdit);
    wbEdit.addListener(SWT.Selection, this::editConfigFile);
  }

  private void editConfigFile(Event event) {
    try {

      int index = wConfigFiles.getSelectionIndex();
      if (index < 0) {
        return;
      }
      String configFilename = wConfigFiles.getItem(index, 1);
      if (StringUtils.isEmpty(configFilename)) {
        return;
      }
      String realConfigFilename = variables.resolve(configFilename);

      DescribedVariablesConfigFile variablesConfigFile =
          new DescribedVariablesConfigFile(realConfigFilename);

      FileObject file = HopVfs.getFileObject(realConfigFilename);
      if (!file.exists()) {
        MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Create file?");
        box.setMessage("This configuration file doesn't exist.  Do you want to create it?");
        int answer = box.open();
        if ((answer & SWT.NO) != 0) {
          return;
        }
      } else {
        variablesConfigFile.readFromFile();
      }

      boolean changed = HopGui.editConfigFile(shell, realConfigFilename, variablesConfigFile, null);
      if (changed) {
        needingEnvironmentRefresh = true;
      }

    } catch (Exception e) {
      new ErrorDialog(shell, CONST_ERROR, "Error editing configuration file", e);
    }
  }

  private void addConfigFile(Event event) {
    String configFile =
        BaseDialog.presentFileDialog(
            shell,
            null,
            variables,
            new String[] {"*.json", "*"},
            new String[] {"Config JSON files", "All files"},
            true);
    if (configFile != null) {
      TableItem item = new TableItem(wConfigFiles.table, SWT.NONE);
      item.setText(1, configFile);
      wConfigFiles.removeEmptyRows();
      wConfigFiles.setRowNums();
      wConfigFiles.optWidth(true);
      wConfigFiles.table.setSelection(item);
      needingEnvironmentRefresh = true;
    }
  }

  private void newConfigFile(Event event) {
    try {
      // What's the project folder?
      //
      String filename = "environment-conf.json";

      String projectName = wProject.getText();
      if (StringUtils.isNotEmpty(projectName)) {
        ProjectConfig projectConfig =
            ProjectsConfigSingleton.getConfig().findProjectConfig(projectName);
        if (projectConfig != null) {
          String environmentName = Const.NVL(wName.getText(), projectName);
          filename =
              projectConfig.getProjectHome()
                  + Const.FILE_SEPARATOR
                  + ".."
                  + Const.FILE_SEPARATOR
                  + environmentName
                  + "-config.json";
        }
      }
      FileObject fileObject = HopVfs.getFileObject(filename);

      String configFile =
          BaseDialog.presentFileDialog(
              true, // save dialog
              shell,
              null,
              variables,
              fileObject,
              new String[] {"*.json", "*"},
              new String[] {"Config JSON files", "All files"},
              true);
      if (configFile != null) {
        TableItem item = new TableItem(wConfigFiles.table, SWT.NONE);
        item.setText(1, configFile);
        wConfigFiles.removeEmptyRows();
        wConfigFiles.setRowNums();
        wConfigFiles.optWidth(true);
        wConfigFiles.table.setSelection(item);
        needingEnvironmentRefresh = true;
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          CONST_ERROR,
          "Error creating new environment configuration file",
          e);
    }
  }

  private void setButtonStates(Event event) {
    int index = wConfigFiles.getSelectionIndex();
    wbEdit.setEnabled(index >= 0);
    wbEdit.setGrayed(index < 0);
  }

  private void importVariables(Event event) {
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    try {
      String projectName = wProject.getText();
      if (StringUtils.isEmpty(projectName)) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        box.setText(
            BaseMessages.getString(
                PKG, "LifecycleEnvironmentDialog.ImportVariables.ProjectRequired.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "LifecycleEnvironmentDialog.ImportVariables.ProjectRequired.Message"));
        box.open();
        return;
      }

      ProjectConfig projectConfig =
          ProjectsConfigSingleton.getConfig().findProjectConfig(projectName);
      if (projectConfig == null) {
        throw new HopException(
            "Project '" + projectName + "' is not configured in Hop (projects config).");
      }

      List<String> configurationFiles = new ArrayList<>();
      for (TableItem item : wConfigFiles.getNonEmptyItems()) {
        configurationFiles.add(item.getText(1));
      }

      List<DescribedVariable> proposed =
          EnvironmentVariablesImportHelper.findMissingVariables(
              projectConfig, variables, configurationFiles, wName.getText());

      // Restore the pointer before modal dialogs so the user can interact normally.
      shell.setCursor(null);

      if (proposed.isEmpty()) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        box.setText(
            BaseMessages.getString(
                PKG, "LifecycleEnvironmentDialog.ImportVariables.NoneFound.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "LifecycleEnvironmentDialog.ImportVariables.NoneFound.Message"));
        box.open();
        return;
      }

      HopDescribedVariablesDialog variablesDialog =
          new HopDescribedVariablesDialog(
              shell,
              BaseMessages.getString(
                  PKG, "LifecycleEnvironmentDialog.ImportVariables.DialogMessage", projectName),
              proposed,
              null);
      List<DescribedVariable> confirmed = variablesDialog.open();
      if (confirmed == null) {
        return;
      }

      String configFilename = chooseConfigFileForImport(configurationFiles, projectName);
      if (StringUtils.isEmpty(configFilename)) {
        return;
      }

      shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
      String realConfigFilename = variables.resolve(configFilename);
      EnvironmentVariablesImportHelper.saveVariablesToConfigFile(realConfigFilename, confirmed);
      shell.setCursor(null);

      // Ensure the path is listed on the environment
      boolean alreadyListed = false;
      for (TableItem item : wConfigFiles.getNonEmptyItems()) {
        if (configFilename.equals(item.getText(1))
            || realConfigFilename.equals(variables.resolve(item.getText(1)))) {
          alreadyListed = true;
          break;
        }
      }
      if (!alreadyListed) {
        TableItem item = new TableItem(wConfigFiles.table, SWT.NONE);
        item.setText(1, configFilename);
        wConfigFiles.removeEmptyRows();
        wConfigFiles.setRowNums();
        wConfigFiles.optWidth(true);
        wConfigFiles.table.setSelection(item);
      }

      needingEnvironmentRefresh = true;

      MessageBox done = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      done.setText(
          BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ImportVariables.Saved.Title"));
      done.setMessage(
          BaseMessages.getString(
              PKG,
              "LifecycleEnvironmentDialog.ImportVariables.Saved.Message",
              Integer.toString(confirmed.size()),
              realConfigFilename));
      done.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ImportVariables.Error.Title"),
          BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ImportVariables.Error.Message"),
          e);
    } finally {
      if (shell != null && !shell.isDisposed()) {
        shell.setCursor(null);
      }
    }
  }

  /**
   * Pick an existing configuration file from the environment list, or create a new one.
   *
   * @return selected/created path (may contain variables), or null if cancelled
   */
  private String chooseConfigFileForImport(List<String> configurationFiles, String projectName)
      throws Exception {
    String createNewLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ImportVariables.CreateNewOption");

    if (configurationFiles != null && !configurationFiles.isEmpty()) {
      List<String> choices = new ArrayList<>(configurationFiles);
      choices.add(createNewLabel);
      EnterSelectionDialog selectionDialog =
          new EnterSelectionDialog(
              shell,
              choices.toArray(new String[0]),
              BaseMessages.getString(
                  PKG, "LifecycleEnvironmentDialog.ImportVariables.SelectConfig.Title"),
              BaseMessages.getString(
                  PKG, "LifecycleEnvironmentDialog.ImportVariables.SelectConfig.Message"));
      selectionDialog.setAvoidQuickSearch();
      String selected = selectionDialog.open();
      if (selected == null) {
        return null;
      }
      if (!createNewLabel.equals(selected)) {
        return selected;
      }
    }

    return promptNewConfigFilename(projectName);
  }

  private String promptNewConfigFilename(String projectName) throws Exception {
    String environmentName = Const.NVL(wName.getText(), projectName);
    String defaultName = EnvironmentVariablesImportHelper.defaultConfigFilename(environmentName);

    FileObject startFile;
    ProjectConfig projectConfig =
        ProjectsConfigSingleton.getConfig().findProjectConfig(projectName);
    if (projectConfig != null) {
      String filename =
          projectConfig.getProjectHome()
              + Const.FILE_SEPARATOR
              + ".."
              + Const.FILE_SEPARATOR
              + defaultName;
      startFile = HopVfs.getFileObject(variables.resolve(filename));
    } else {
      startFile = HopVfs.getFileObject(defaultName);
    }

    return BaseDialog.presentFileDialog(
        true,
        shell,
        null,
        variables,
        startFile,
        new String[] {"*.json", "*"},
        new String[] {
          BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ImportVariables.FileFilter.Json"),
          BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.ImportVariables.FileFilter.All")
        },
        true);
  }

  private void ok() {

    try {
      String environmentName = wName.getText();
      if (StringUtils.isEmpty(environmentName)) {
        throw new HopException("Please give your environment a name");
      }
      if (StringUtils.isNotEmpty(originalName) && !originalName.equals(environmentName)) {
        wName.setText(originalName);
        throw new HopException(
            "Sorry, renaming environment '" + originalName + "' is not supported.");
      }

      getInfo(environment);
      if (dialogExtension != null) {
        dialogExtension.getContext().setProjectName(environment.getProjectName());
        dialogExtension.getContext().setEnvironmentName(environment.getName());
        dialogExtension.getContext().setPurpose(environment.getPurpose());
        dialogExtension
            .getContext()
            .setConfigurationFiles(new ArrayList<>(environment.getConfigurationFiles()));
        dialogExtension.runSaveCallbacks();
        dialogExtension.getContext().copyAttributesTo(environment);
      }
      returnValue = environment.getName();

      dispose();
    } catch (Exception e) {
      new ErrorDialog(shell, CONST_ERROR, "There is a configuration error in the environment", e);
    }
  }

  private void cancel() {
    needingEnvironmentRefresh = false;
    returnValue = null;

    dispose();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /**
   * The home folder of the project this environment belongs to, with variables expanded. Empty when
   * the project is unknown or has no home configured.
   */
  private String projectHomeOf(String projectName) {
    if (StringUtils.isEmpty(projectName)) {
      return null;
    }
    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      ProjectConfig projectConfig = config == null ? null : config.findProjectConfig(projectName);
      String home = projectConfig == null ? null : projectConfig.getProjectHome();
      return StringUtils.isEmpty(home) ? null : variables.resolve(home);
    } catch (Exception e) {
      // Best effort: an unreadable projects config must not stop the dialog from opening.
      return null;
    }
  }

  private void getData() {
    loadingData = true;
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    String developmentLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Purpose.Text.Development");
    String testingLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Purpose.Text.Testing");
    String acceptanceLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Purpose.Text.Acceptance");
    String productionLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Purpose.Text.Production");
    String continuousIntegrationLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Purpose.Text.CI");
    String commonBuildLabel =
        BaseMessages.getString(PKG, "LifecycleEnvironmentDialog.Purpose.Text.CB");

    knownPurposeSuffixes =
        LifecycleEnvironmentNaming.knownPurposeSuffixes(
            developmentLabel,
            testingLabel,
            acceptanceLabel,
            productionLabel,
            continuousIntegrationLabel,
            commonBuildLabel);

    wProject.setItems(config.listProjectConfigNames().toArray(new String[0]));
    wPurpose.setItems(
        developmentLabel,
        testingLabel,
        acceptanceLabel,
        productionLabel,
        continuousIntegrationLabel,
        commonBuildLabel);

    // Setting project/purpose may fire Modify and update the suggested name for new envs.
    wPurpose.setText(Const.NVL(environment.getPurpose(), ""));
    wProject.setText(Const.NVL(environment.getProjectName(), ""));
    wCanvasText.setText(Const.NVL(environment.getCanvasText(), ""));

    if (StringUtils.isNotEmpty(environment.getName())) {
      // Provided name (edit or rare pre-fill): do not auto-overwrite.
      nameAutoManaged = false;
      setNameText(environment.getName());
    } else if (isNewEnvironment()) {
      nameAutoManaged = true;
      // Project/purpose listeners may already have suggested a name; ensure we do so if not.
      updateSuggestedName();
    } else {
      setNameText("");
    }

    wConfigFiles.table.removeAll();
    for (int i = 0; i < environment.getConfigurationFiles().size(); i++) {
      String configurationFile = environment.getConfigurationFiles().get(i);
      TableItem item = new TableItem(wConfigFiles.table, SWT.NONE);
      item.setText(1, Const.NVL(configurationFile, ""));
    }
    if (environment.getConfigurationFiles().isEmpty()) {
      new TableItem(wConfigFiles.table, SWT.NONE);
    }
    wConfigFiles.setRowNums();
    wConfigFiles.optWidth(true);

    // Select the first configuration file by default
    // That way you can immediately hit the edit button
    //
    if (!environment.getConfigurationFiles().isEmpty()) {
      wConfigFiles.setSelection(new int[] {0});
    }

    selectFormatLocale(environment.getFormatLocale());
    selectTimeZone(environment.getTimeZone());
    refreshRegionalPreview();
    loadingData = false;
  }

  private void selectFormatLocale(String localeCode) {
    if (StringUtils.isEmpty(localeCode)) {
      wFormatLocale.setText(inheritLabel());
      return;
    }
    Locale locale = EnvUtil.createLocale(localeCode);
    int index = locale == null ? -1 : localeList.indexOf(locale);
    if (index < 0 && locale != null) {
      localeList = new ArrayList<>(localeList);
      localeList.add(locale);
      wFormatLocale.setItems(localeComboItems());
      index = localeList.indexOf(locale);
    }
    if (index >= 0) {
      wFormatLocale.select(index + 1);
      wFormatLocale.setText(wFormatLocale.getItem(index + 1));
    } else {
      wFormatLocale.setText(localeCode);
    }
  }

  private void selectTimeZone(String timeZoneId) {
    if (StringUtils.isEmpty(timeZoneId)) {
      wTimeZone.setText(inheritLabel());
      return;
    }
    wTimeZone.setText(timeZoneId);
  }

  private boolean isNewEnvironment() {
    return StringUtils.isEmpty(originalName);
  }

  /**
   * When creating a new environment, keep the name field in sync with project + purpose until the
   * user edits the name manually.
   */
  private void updateSuggestedName() {
    if (!isNewEnvironment()
        || !nameAutoManaged
        || wName == null
        || wProject == null
        || wPurpose == null) {
      return;
    }

    String suggested =
        LifecycleEnvironmentNaming.suggestEnvironmentName(
            wProject.getText(), wPurpose.getText(), knownPurposeSuffixes);
    lastSuggestedName = suggested;
    setNameText(suggested);
  }

  private void setNameText(String text) {
    updatingSuggestedName = true;
    try {
      wName.setText(Const.NVL(text, ""));
    } finally {
      updatingSuggestedName = false;
    }
  }

  private void onNameModified() {
    if (updatingSuggestedName || !isNewEnvironment()) {
      return;
    }
    String current = wName.getText();
    // Cleared field or still matching last suggestion → keep auto-managing.
    if (StringUtils.isEmpty(current)
        || (lastSuggestedName != null && current.equals(lastSuggestedName))) {
      nameAutoManaged = true;
    } else {
      nameAutoManaged = false;
    }
  }

  private void getInfo(LifecycleEnvironment env) {
    env.setName(wName.getText());
    env.setPurpose(wPurpose.getText());
    env.setProjectName(wProject.getText());
    env.setCanvasText(wCanvasText.getText());
    Locale formatLocale = selectedFormatLocale();
    env.setFormatLocale(formatLocale == null ? null : formatLocale.toString());
    env.setTimeZone(selectedTimeZoneId());

    env.getConfigurationFiles().clear();
    for (TableItem item : wConfigFiles.getNonEmptyItems()) {
      env.getConfigurationFiles().add(item.getText(1));
    }
  }

  /**
   * Gets needingEnvironmentRefresh
   *
   * @return value of needingEnvironmentRefresh
   */
  public boolean isNeedingEnvironmentRefresh() {
    return needingEnvironmentRefresh;
  }
}
