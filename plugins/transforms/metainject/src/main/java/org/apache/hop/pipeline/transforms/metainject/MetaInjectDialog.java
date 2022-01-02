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

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class MetaInjectDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MetaInjectMeta.class; // For Translator

  public static final String CONST_VALUE = "<const>";

  private final MetaInjectMeta metaInjectMeta;

  private TextVar wPath;

  private CTabFolder wTabFolder;

  private PipelineMeta injectPipelineMeta = null;

  protected boolean transModified;

  private ModifyListener lsMod;

  protected Label wlRunConfiguration;
  protected ComboVar wRunConfiguration;

  // Create parent folder
  protected Button wCreateParentFolder;

  // the source transform
  //
  private CCombo wSourceTransform;

  // The source transform output fields...
  //
  private TableView wSourceFields;

  // the target file
  //
  private TextVar wTargetFile;

  // don't execute the transformation
  //
  private Button wNoExecution;

  private CCombo wStreamingSourceTransform;

  // the streaming target transform
  //
  private Label wlStreamingTargetTransform;
  private CCombo wStreamingTargetTransform;

  // The tree object to show the options...
  //
  private Tree wTree;

  private Map<TreeItem, TargetTransformAttribute> treeItemTargetMap;

  private final Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping;

  private Text wSearchText = null;
  private String filterString = null;

  public MetaInjectDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    metaInjectMeta = (MetaInjectMeta) in;
    transModified = false;

    targetSourceMapping = new HashMap<>();
    targetSourceMapping.putAll(metaInjectMeta.getTargetSourceMapping());
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, metaInjectMeta);

    lsMod = e -> metaInjectMeta.setChanged();
    changed = metaInjectMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MetaInjectDialog.Shell.Title"));

    Label wicon = new Label(shell, SWT.RIGHT);
    wicon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wicon.setLayoutData(fdlicon);
    props.setLook(wicon);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "MetaInjectDialog.Button.EnterMapping"));
    wGet.addListener(SWT.Selection, e -> enterMapping());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    positionBottomButtons(
        shell,
        new Button[] {
          wOk, wGet, wCancel,
        },
        props.getMargin(),
        null);

    // Transform Name line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MetaInjectDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, 0);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.right = new FormAttachment(90, 0);
    fdTransformName.left = new FormAttachment(0, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 5);
    wTransformName.setLayoutData(fdTransformName);

    Label spacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wTransformName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    Label wlPath = new Label(shell, SWT.LEFT);
    props.setLook(wlPath);
    wlPath.setText(BaseMessages.getString(PKG, "MetaInjectDialog.Pipeline.Label"));
    FormData fdlTransformation = new FormData();
    fdlTransformation.left = new FormAttachment(0, 0);
    fdlTransformation.top = new FormAttachment(spacer, 20);
    fdlTransformation.right = new FormAttachment(100, 0);
    wlPath.setLayoutData(fdlTransformation);

    Button wbBrowse = new Button(shell, SWT.PUSH);
    props.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "MetaInjectDialog.Browse.Label"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, -props.getMargin());
    fdBrowse.top = new FormAttachment(wlPath, Const.isOSX() ? 0 : 5);
    wbBrowse.setLayoutData(fdBrowse);

    wPath = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPath);
    FormData fdTransformation = new FormData();
    fdTransformation.left = new FormAttachment(0, 0);
    fdTransformation.top = new FormAttachment(wlPath, 5);
    fdTransformation.right = new FormAttachment(wbBrowse, -props.getMargin());
    wPath.setLayoutData(fdTransformation);
    wPath.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent focusEvent) {
            refreshTree();
          }
        });

    wbBrowse.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            selectFileTrans();
            refreshTree();
          }
        });

    wlRunConfiguration = new Label(shell, SWT.LEFT);
    wlRunConfiguration.setText(
        BaseMessages.getString(PKG, "MetaInjectDialog.RunConfiguration.Label"));
    props.setLook(wlRunConfiguration);
    FormData fdlRunConfiguration = new FormData();
    fdlRunConfiguration.left = new FormAttachment(0, 0);
    fdlRunConfiguration.top = new FormAttachment(wPath, 20);
    fdlRunConfiguration.right = new FormAttachment(100, 0);
    wlRunConfiguration.setLayoutData(fdlRunConfiguration);

    wRunConfiguration = new ComboVar(variables, shell, SWT.LEFT | SWT.BORDER);
    props.setLook(wlRunConfiguration);
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.left = new FormAttachment(0, 0);
    fdRunConfiguration.top = new FormAttachment(wlRunConfiguration, 10);
    fdRunConfiguration.right = new FormAttachment(100, 0);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
    props.setLook(wRunConfiguration);

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    Label hSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment(0, 0);
    fdhSpacer.bottom = new FormAttachment(wCancel, -15);
    fdhSpacer.right = new FormAttachment(100, 0);
    hSpacer.setLayoutData(fdhSpacer);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wRunConfiguration, 20);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(hSpacer, -15);
    wTabFolder.setLayoutData(fdTabFolder);

    addInjectTab();
    addOptionsTab();

    getData();
    metaInjectMeta.setChanged(changed);

    checkInvalidMapping();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "GenericTransform.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }

  private void checkInvalidMapping() {
    if (injectPipelineMeta == null) {
      try {
        if (!loadPipeline()) {
          return;
        }
      } catch (HopException e) {
        showErrorOnLoadTransformationDialog(e);
        return;
      }
    }
    Set<SourceTransformField> unavailableSourceTransforms =
        MetaInject.getUnavailableSourceTransforms(targetSourceMapping, pipelineMeta, transformMeta);
    Set<TargetTransformAttribute> unavailableTargetTransforms =
        MetaInject.getUnavailableTargetTransforms(targetSourceMapping, injectPipelineMeta);
    Set<TargetTransformAttribute> missingTargetKeys =
        MetaInject.getUnavailableTargetKeys(
            targetSourceMapping, injectPipelineMeta, unavailableTargetTransforms);
    if (unavailableSourceTransforms.isEmpty()
        && unavailableTargetTransforms.isEmpty()
        && missingTargetKeys.isEmpty()) {
      return;
    }
    showInvalidMappingDialog(
        unavailableSourceTransforms, unavailableTargetTransforms, missingTargetKeys);
  }

  private void showInvalidMappingDialog(
      Set<SourceTransformField> unavailableSourceTransforms,
      Set<TargetTransformAttribute> unavailableTargetTransforms,
      Set<TargetTransformAttribute> missingTargetKeys) {
    MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    mb.setMessage(BaseMessages.getString(PKG, "MetaInjectDialog.InvalidMapping.Question"));
    mb.setText(BaseMessages.getString(PKG, "MetaInjectDialog.InvalidMapping.Title"));
    int id = mb.open();
    if (id == SWT.YES) {
      MetaInject.removeUnavailableTransformsFromMapping(
          targetSourceMapping, unavailableSourceTransforms, unavailableTargetTransforms);
      for (TargetTransformAttribute target : missingTargetKeys) {
        targetSourceMapping.remove(target);
      }
    }
  }

  private void showErrorOnLoadTransformationDialog(HopException e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Title"),
        BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Message"),
        e);
  }

  private void addOptionsTab() {
    // ////////////////////////
    // START OF OPTIONS TAB ///
    // ////////////////////////

    int margin = props.getMargin();

    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setText(BaseMessages.getString(PKG, "MetaInjectDialog.OptionsTab.TabTitle"));

    ScrolledComposite wOptionsSComp =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wOptionsSComp.setLayout(new FillLayout());

    Composite wOptionsComp = new Composite(wOptionsSComp, SWT.NONE);
    props.setLook(wOptionsComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 15;
    fileLayout.marginHeight = 15;
    wOptionsComp.setLayout(fileLayout);

    Label wlSourceTransform = new Label(wOptionsComp, SWT.RIGHT);
    wlSourceTransform.setText(
        BaseMessages.getString(PKG, "MetaInjectDialog.SourceTransform.Label"));
    props.setLook(wlSourceTransform);
    FormData fdlSourceTransform = new FormData();
    fdlSourceTransform.left = new FormAttachment(0, 0);
    fdlSourceTransform.top = new FormAttachment(0, 0);
    wlSourceTransform.setLayoutData(fdlSourceTransform);

    wSourceTransform = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSourceTransform);
    wSourceTransform.addModifyListener(lsMod);
    FormData fdSourceTransform = new FormData();
    fdSourceTransform.right = new FormAttachment(100, 0);
    fdSourceTransform.left = new FormAttachment(0, 0);
    fdSourceTransform.top = new FormAttachment(wlSourceTransform, 5);
    wSourceTransform.setLayoutData(fdSourceTransform);
    wSourceTransform.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            setActive();
          }
        });

    final int fieldRows = metaInjectMeta.getSourceOutputFields().size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.ColumnInfo.Fieldname"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getAllValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wSourceFields =
        new TableView(
            variables,
            wOptionsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldRows,
            false,
            lsMod,
            props,
            false);
    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment(wSourceTransform, 10);
    fdFields.bottom = new FormAttachment(50, 0);
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    wSourceFields.setLayoutData(fdFields);
    wSourceFields.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 25, 25, 25, 25));

    Label wlTargetFile = new Label(wOptionsComp, SWT.RIGHT);
    wlTargetFile.setText(BaseMessages.getString(PKG, "MetaInjectDialog.TargetFile.Label"));
    props.setLook(wlTargetFile);
    FormData fdlTargetFile = new FormData();
    fdlTargetFile.left = new FormAttachment(0, 0);
    fdlTargetFile.top = new FormAttachment(wSourceFields, 10);
    wlTargetFile.setLayoutData(fdlTargetFile);

    // Browse for optional target file
    Button wbFilename = new Button(wOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wlTargetFile, margin);
    wbFilename.setLayoutData(fdbFilename);

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wTargetFile,
                variables,
                new String[] {"*.hpl"},
                new String[] {BaseMessages.getString(PKG, "System.FileType.Pipeline")},
                true));

    wTargetFile = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTargetFile);
    wTargetFile.addModifyListener(lsMod);
    FormData fdTargetFile = new FormData();
    fdTargetFile.right = new FormAttachment(wbFilename, -margin);
    fdTargetFile.left = new FormAttachment(0, 0);
    fdTargetFile.top = new FormAttachment(wlTargetFile, margin);
    wTargetFile.setLayoutData(fdTargetFile);

    wCreateParentFolder = new Button(wOptionsComp, SWT.CHECK);
    wCreateParentFolder.setText(
        BaseMessages.getString(PKG, "MetaInjectDialog.CreateParentFolder.Label"));
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "MetaInjectDialog.CreateParentFolder.Tooltip"));
    props.setLook(wCreateParentFolder);
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(0, 0);
    fdCreateParentFolder.top = new FormAttachment(wTargetFile, margin);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            metaInjectMeta.setChanged();
          }
        });

    // the streaming source transform
    //
    Label wlStreamingSourceTransform = new Label(wOptionsComp, SWT.RIGHT);
    wlStreamingSourceTransform.setText(
        BaseMessages.getString(PKG, "MetaInjectDialog.StreamingSourceTransform.Label"));
    props.setLook(wlStreamingSourceTransform);
    FormData fdlStreamingSourceTransform = new FormData();
    fdlStreamingSourceTransform.left = new FormAttachment(0, 0);
    fdlStreamingSourceTransform.top = new FormAttachment(wCreateParentFolder, 10);
    wlStreamingSourceTransform.setLayoutData(fdlStreamingSourceTransform);

    wStreamingSourceTransform = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wStreamingSourceTransform);
    FormData fdStreamingSourceTransform = new FormData();
    fdStreamingSourceTransform.right = new FormAttachment(100, 0);
    fdStreamingSourceTransform.left = new FormAttachment(0, 0);
    fdStreamingSourceTransform.top = new FormAttachment(wlStreamingSourceTransform, 5);
    wStreamingSourceTransform.setLayoutData(fdStreamingSourceTransform);
    wStreamingSourceTransform.setItems(pipelineMeta.getTransformNames());
    wStreamingSourceTransform.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            setActive();
          }
        });

    wlStreamingTargetTransform = new Label(wOptionsComp, SWT.RIGHT);
    wlStreamingTargetTransform.setText(
        BaseMessages.getString(PKG, "MetaInjectDialog.StreamingTargetTransform.Label"));
    props.setLook(wlStreamingTargetTransform);
    FormData fdlStreamingTargetTransform = new FormData();
    fdlStreamingTargetTransform.left = new FormAttachment(0, 0);
    fdlStreamingTargetTransform.top = new FormAttachment(wStreamingSourceTransform, 10);
    wlStreamingTargetTransform.setLayoutData(fdlStreamingTargetTransform);

    wStreamingTargetTransform = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wStreamingTargetTransform);
    FormData fdStreamingTargetTransform = new FormData();
    fdStreamingTargetTransform.right = new FormAttachment(100, 0);
    fdStreamingTargetTransform.left = new FormAttachment(0, 0);
    fdStreamingTargetTransform.top = new FormAttachment(wlStreamingTargetTransform, 5);
    wStreamingTargetTransform.setLayoutData(fdStreamingTargetTransform);

    wNoExecution = new Button(wOptionsComp, SWT.CHECK);
    wNoExecution.setText(BaseMessages.getString(PKG, "MetaInjectDialog.NoExecution.Label"));
    props.setLook(wNoExecution);
    FormData fdNoExecution = new FormData();
    fdNoExecution.width = 350;
    fdNoExecution.left = new FormAttachment(0, 0);
    fdNoExecution.top = new FormAttachment(wStreamingTargetTransform, 10);
    wNoExecution.setLayoutData(fdNoExecution);

    FormData fdOptionsComp = new FormData();
    fdOptionsComp.left = new FormAttachment(0, 0);
    fdOptionsComp.top = new FormAttachment(0, 0);
    fdOptionsComp.right = new FormAttachment(100, 0);
    fdOptionsComp.bottom = new FormAttachment(100, 0);
    wOptionsComp.setLayoutData(fdOptionsComp);

    wOptionsComp.pack();
    Rectangle bounds = wOptionsComp.getBounds();

    wOptionsSComp.setContent(wOptionsComp);
    wOptionsSComp.setExpandHorizontal(true);
    wOptionsSComp.setExpandVertical(true);
    wOptionsSComp.setMinWidth(bounds.width);
    wOptionsSComp.setMinHeight(bounds.height);

    wOptionsTab.setControl(wOptionsSComp);

    // ///////////////////////////////////////////////////////////
    // / END OF OPTIONS TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addInjectTab() {
    // ////////////////////////
    // START OF INJECT TAB ///
    // ////////////////////////

    CTabItem wInjectTab = new CTabItem(wTabFolder, SWT.NONE);
    wInjectTab.setText(BaseMessages.getString(PKG, "MetaInjectDialog.InjectTab.TabTitle"));

    ScrolledComposite wInjectSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wInjectSComp.setLayout(new FillLayout());

    Composite wInjectComp = new Composite(wInjectSComp, SWT.NONE);
    props.setLook(wInjectComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 15;
    fileLayout.marginHeight = 15;
    wInjectComp.setLayout(fileLayout);

    // Add a search bar at the top...
    ToolBar treeTb = new ToolBar(wInjectComp, SWT.HORIZONTAL | SWT.FLAT);
    props.setLook(treeTb);

    ToolItem wFilter = new ToolItem(treeTb, SWT.SEPARATOR);
    wSearchText = new Text(treeTb, SWT.SEARCH | SWT.CANCEL | SWT.ICON_SEARCH | SWT.ICON_CANCEL);
    props.setLook(wSearchText);
    wSearchText.setToolTipText(
        BaseMessages.getString(PKG, "MetaInjectDialog.InjectTab.FilterString.ToolTip"));
    wFilter.setControl(wSearchText);
    wFilter.setWidth((int) (150 * props.getZoomFactor()));

    ToolItem wExpandAll = new ToolItem(treeTb, SWT.PUSH);
    wExpandAll.setImage(GuiResource.getInstance().getImageExpandAll());
    wExpandAll.setToolTipText(
        BaseMessages.getString(PKG, "MetaInjectDialog.InjectTab.FilterString.ExpandAll"));

    wExpandAll.addListener(SWT.Selection, e -> setExpandedState(true));

    ToolItem wCollapseAll = new ToolItem(treeTb, SWT.PUSH);
    wCollapseAll.setImage(GuiResource.getInstance().getImageCollapseAll());
    wCollapseAll.setToolTipText(
        BaseMessages.getString(PKG, "MetaInjectDialog.InjectTab.FilterString.CollapseAll"));
    wCollapseAll.addListener(SWT.Selection, e -> setExpandedState(false));

    FormData fd = new FormData();
    fd.right = new FormAttachment(100);
    fd.top = new FormAttachment(0, 0);
    treeTb.setLayoutData(fd);

    Label wlFilter = new Label(wInjectComp, SWT.RIGHT);
    props.setLook(wlFilter);
    wlFilter.setText(BaseMessages.getString(PKG, "MetaInjectDialog.InjectTab.FilterString.Label"));
    FormData fdlFilter = new FormData();
    fdlFilter.top = new FormAttachment(0, 5);
    fdlFilter.right = new FormAttachment(treeTb, -5);
    wlFilter.setLayoutData(fdlFilter);

    wSearchText.addListener(SWT.Modify, e -> updateTransformationFilter());

    Label wlTree = new Label(wInjectComp, SWT.LEFT);
    props.setLook(wlTree);
    wlTree.setText(BaseMessages.getString(PKG, "MetaInjectDialog.InjectTab.CLickTree.Label"));

    // Transformation list
    wTree =
        new Tree(
            wInjectComp,
            SWT.SINGLE | SWT.FULL_SELECTION | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment(0, 0);
    fdTree.top = new FormAttachment(wlFilter, 5);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.bottom = new FormAttachment(100, 0);
    wTree.setLayoutData(fdTree);

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.Column.TargetTransform"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.Column.SourceTransform"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetaInjectDialog.Column.SourceField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false,
              true),
        };

    wTree.setHeaderVisible(true);
    for (ColumnInfo columnInfo : colinf) {
      TreeColumn treeColumn = new TreeColumn(wTree, columnInfo.getAlignment());
      treeColumn.setText(columnInfo.getName());
      treeColumn.setWidth((int) (300 * props.getZoomFactor()));
    }

    wTree.addListener(SWT.MouseDown, this::treeClicked);

    FormData fdInjectComp = new FormData();
    fdInjectComp.left = new FormAttachment(0, 0);
    fdInjectComp.top = new FormAttachment(wlFilter, 5);
    fdInjectComp.right = new FormAttachment(100, 0);
    fdInjectComp.bottom = new FormAttachment(100, 0);
    wInjectComp.setLayoutData(fdInjectComp);

    wInjectComp.pack();
    Rectangle bounds = wInjectComp.getBounds();

    wInjectSComp.setContent(wInjectComp);
    wInjectSComp.setExpandHorizontal(true);
    wInjectSComp.setExpandVertical(true);
    wInjectSComp.setMinWidth(bounds.width);
    wInjectSComp.setMinHeight(bounds.height);

    wInjectTab.setControl(wInjectSComp);

    // ///////////////////////////////////////////////////////////
    // / END OF INJECT TAB
    // ///////////////////////////////////////////////////////////
  }

  private void treeClicked(Event event) {
    try {
      Point point = new Point(event.x, event.y);
      TreeItem item = wTree.getItem(point);
      if (item != null) {
        TargetTransformAttribute target = treeItemTargetMap.get(item);
        if (target != null) {
          SourceTransformField source = targetSourceMapping.get(target);

          String[] prevTransformNames = pipelineMeta.getPrevTransformNames(transformMeta);
          Arrays.sort(prevTransformNames);

          Map<String, SourceTransformField> fieldMap = new HashMap<>();
          for (String prevTransformName : prevTransformNames) {
            IRowMeta fields = pipelineMeta.getTransformFields(variables, prevTransformName);
            for (IValueMeta field : fields.getValueMetaList()) {
              String key = buildTransformFieldKey(prevTransformName, field.getName());
              fieldMap.put(key, new SourceTransformField(prevTransformName, field.getName()));
            }
          }
          String[] sourceFields = fieldMap.keySet().toArray(new String[fieldMap.size()]);
          Arrays.sort(sourceFields);

          String constant =
              source != null && source.getTransformName() == null ? source.getField() : "";
          EnterSelectionDialog selectSourceFieldDialog =
              new EnterSelectionDialog(
                  shell,
                  sourceFields,
                  BaseMessages.getString(PKG, "MetaInjectDialog.SourceFieldDialog.Title"),
                  BaseMessages.getString(PKG, "MetaInjectDialog.SourceFieldDialog.Label"),
                  constant,
                  variables);
          selectSourceFieldDialog.setAddNoneOption(true);
          if (source != null) {
            if (source.getTransformName() != null && !Utils.isEmpty(source.getTransformName())) {
              String key = buildTransformFieldKey(source.getTransformName(), source.getField());
              selectSourceFieldDialog.setCurrentValue(key);
              int index = Const.indexOfString(key, sourceFields);
              if (index >= 0) {
                selectSourceFieldDialog.setSelectedNrs(
                    new int[] {
                      index,
                    });
              }
            } else {
              selectSourceFieldDialog.setCurrentValue(source.getField());
            }
          }
          String selectedTransformField = selectSourceFieldDialog.open();
          if (selectedTransformField != null) {
            SourceTransformField newSource = fieldMap.get(selectedTransformField);
            if (newSource == null) {
              newSource = new SourceTransformField(null, selectedTransformField);
              item.setText(1, CONST_VALUE);
              item.setText(2, selectedTransformField);
            } else {
              item.setText(1, newSource.getTransformName());
              item.setText(2, newSource.getField());
            }
            targetSourceMapping.put(target, newSource);
          } else {
            if (selectSourceFieldDialog.isNoneClicked()) {
              item.setText(1, "");
              item.setText(2, "");
              targetSourceMapping.remove(target);
            }
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Oops", "Unexpected Error", e);
    }
  }

  private void selectFileTrans() {
    try {
      HopPipelineFileType<PipelineMeta> fileType =
          HopGui.getDataOrchestrationPerspective().getPipelineFileType();
      String filename =
          BaseDialog.presentFileDialog(
              shell,
              wPath,
              variables,
              fileType.getFilterExtensions(),
              fileType.getFilterNames(),
              true);
      if (filename != null) {
        loadPipelineFile(filename);
        wPath.setText(filename);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingPipeline.DialogTitle"),
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingPipeline.DialogMessage"),
          e);
    }
  }

  private void loadPipelineFile(String filename) throws HopException {
    String realFilename = variables.resolve(filename);
    try {
      injectPipelineMeta = new PipelineMeta(realFilename, metadataProvider, true, variables);
      injectPipelineMeta.clearChanged();
    } catch (HopXmlException e) {
      MessageBox box = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      box.setText(
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Title"));
      box.setMessage(
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Message"));
      box.open();
    }
  }

  private boolean loadPipeline() throws HopException {
    String filename = wPath.getText();
    if (Utils.isEmpty(filename)) {
      return false;
    }
    if (!filename.endsWith(".hpl")) {
      filename = filename + ".hpl";
      wPath.setText(filename);
    }
    loadPipelineFile(filename);
    if (injectPipelineMeta == null) {
      return false;
    }
    return true;
  }

  public void setActive() {
    boolean outputCapture = !Utils.isEmpty(wSourceTransform.getText());
    wSourceFields.setEnabled(outputCapture);

    boolean streaming = !Utils.isEmpty(wStreamingSourceTransform.getText());
    wStreamingTargetTransform.setEnabled(streaming);
    wlStreamingTargetTransform.setEnabled(streaming);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wPath.setText(Const.NVL(metaInjectMeta.getFileName(), ""));

    try {
      List<String> runConfigurations =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).listObjectNames();

      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiRunConfiguration.id,
            new Object[] {runConfigurations, PipelineMeta.XML_TAG});
      } catch (HopException e) {
        // Ignore errors
      }

      wRunConfiguration.setItems(runConfigurations.toArray(new String[0]));
      wRunConfiguration.setText(Const.NVL(metaInjectMeta.getRunConfigurationName(), ""));

      if (Utils.isEmpty(metaInjectMeta.getRunConfigurationName())) {
        wRunConfiguration.select(0);
      } else {
        wRunConfiguration.setText(metaInjectMeta.getRunConfigurationName());
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting pipeline run configurations", e);
    }

    wSourceTransform.setText(Const.NVL(metaInjectMeta.getSourceTransformName(), ""));
    int rownr = 0;
    for (MetaInjectOutputField field : metaInjectMeta.getSourceOutputFields()) {
      int colNr = 1;
      wSourceFields.setText(field.getName(), colNr++, rownr);
      wSourceFields.setText(field.getTypeDescription(), colNr++, rownr);
      wSourceFields.setText(
          field.getLength() < 0 ? "" : Integer.toString(field.getLength()), colNr++, rownr);
      wSourceFields.setText(
          field.getPrecision() < 0 ? "" : Integer.toString(field.getPrecision()), colNr, rownr);
      rownr++;
    }

    wTargetFile.setText(Const.NVL(metaInjectMeta.getTargetFile(), ""));
    wCreateParentFolder.setSelection(metaInjectMeta.isCreateParentFolder());
    wNoExecution.setSelection(!metaInjectMeta.isNoExecution());

    wStreamingSourceTransform.setText(
        Const.NVL(
            metaInjectMeta.getStreamSourceTransform() == null
                ? null
                : metaInjectMeta.getStreamSourceTransform().getName(),
            ""));
    wStreamingTargetTransform.setText(Const.NVL(metaInjectMeta.getStreamTargetTransformName(), ""));

    setActive();
    refreshTree();

    wTabFolder.setSelection(0);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  protected String buildTransformFieldKey(String transformName, String field) {
    return transformName + " : " + field;
  }

  protected void updateTransformationFilter() {
    filterString = null;
    if (wSearchText != null && !wSearchText.isDisposed() && !Utils.isEmpty(wSearchText.getText())) {
      filterString = wSearchText.getText().toUpperCase();
    }
    refreshTree();
    setExpandedState(true);
  }

  private void refreshTree() {
    try {
      loadPipeline();

      treeItemTargetMap = new HashMap<>();

      wTree.removeAll();

      List<TransformMeta> injectTransforms = new ArrayList<>();
      for (TransformMeta transformMeta : injectPipelineMeta.getTransforms()) {
        ITransformMeta meta = transformMeta.getTransform();
        if (BeanInjectionInfo.isInjectionSupported(meta.getClass())) {
          injectTransforms.add(transformMeta);
        }
      }
      Collections.sort(injectTransforms);

      for (TransformMeta transformMeta : injectTransforms) {
        TreeItem transformItem = new TreeItem(wTree, SWT.NONE);
        transformItem.setText(transformMeta.getName());
        boolean expanded = false;

        Image image =
            GuiResource.getInstance()
                .getImagesTransforms()
                .get(transformMeta.getPluginId())
                .getAsBitmapForSize(shell.getDisplay(), ConstUi.ICON_SIZE, ConstUi.ICON_SIZE);
        transformItem.setImage(image);

        // For each transform, add the keys
        //
        ITransformMeta metaInterface = transformMeta.getTransform();
        if (BeanInjectionInfo.isInjectionSupported(metaInterface.getClass())) {
          expanded = expanded || processMDIDescription(transformMeta, transformItem, metaInterface);
        }

        transformItem.setExpanded(expanded);
      }

    } catch (Throwable t) {
      // Ignore errors
    }

    // Also set the source transform combo values
    //
    if (injectPipelineMeta != null) {
      String[] sourceTransforms = injectPipelineMeta.getTransformNames();
      Arrays.sort(sourceTransforms);
      wSourceTransform.setItems(sourceTransforms);
      wStreamingTargetTransform.setItems(sourceTransforms);
    }
  }

  /**
   * @param transformMeta The transform meta
   * @param transformItem The TreeItem
   * @param metaInterface The transform ITransformMeta
   * @return true if there was at least one used key
   */
  private boolean processMDIDescription(
      TransformMeta transformMeta, TreeItem transformItem, ITransformMeta metaInterface) {
    boolean hasUsedKeys = false;
    BeanInjectionInfo transformInjectionInfo = new BeanInjectionInfo(metaInterface.getClass());

    List<BeanInjectionInfo.Group> groupsList = transformInjectionInfo.getGroups();

    for (BeanInjectionInfo.Group gr : groupsList) {
      if (!gr.hasMatchingProperty(filterString)) {
        continue;
      }
      boolean rootGroup = StringUtils.isEmpty(gr.getKey());

      TreeItem groupItem;
      if (!rootGroup) {
        groupItem = new TreeItem(transformItem, SWT.NONE);
        groupItem.setText(Const.NVL(gr.getTranslatedDescription(), gr.getKey()));
        groupItem.setExpanded(true);
      } else {
        groupItem = null;
      }

      List<BeanInjectionInfo.Property> propertyList = gr.getProperties();

      for (BeanInjectionInfo.Property property : propertyList) {
        if (!property.hasMatch(filterString)) {
          continue;
        }

        if (!property.isExcludedFromInjection()) {
          TreeItem treeItem = new TreeItem(rootGroup ? transformItem : groupItem, SWT.NONE);
          treeItem.setText(Const.NVL(property.getTranslatedDescription(), property.getKey()));

          TargetTransformAttribute target =
              new TargetTransformAttribute(transformMeta.getName(), property.getKey(), !rootGroup);
          treeItemTargetMap.put(treeItem, target);

          SourceTransformField source = targetSourceMapping.get(target);
          if (source != null) {
            hasUsedKeys = true;
            treeItem.setText(
                1,
                Const.NVL(
                    source.getTransformName() == null ? CONST_VALUE : source.getTransformName(),
                    ""));
            treeItem.setText(2, Const.NVL(source.getField(), ""));
          }
        }
      }
    }
    return hasUsedKeys;
  }

  private void setExpandedState(boolean state) {
    for (TreeItem item : wTree.getItems()) {
      expandItemAndChildren(item, state);
    }
  }

  private void expandItemAndChildren(TreeItem item, boolean state) {
    // only expand root item
    item.setExpanded(state);
    for (TreeItem item2 : item.getItems()) {
      expandItemAndChildren(item2, state);
    }
  }

  private void cancel() {
    transformName = null;
    metaInjectMeta.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    try {
      loadPipeline();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Title"),
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Message"),
          e);
    }

    getInfo(metaInjectMeta);
    dispose();
  }

  private void getInfo(MetaInjectMeta meta) {
    meta.setFileName(wPath.getText());
    meta.setSourceTransformName(wSourceTransform.getText());
    meta.setRunConfigurationName(wRunConfiguration.getText());

    meta.setSourceOutputFields(new ArrayList<>());
    for (int i = 0; i < wSourceFields.nrNonEmpty(); i++) {
      TableItem item = wSourceFields.getNonEmpty(i);
      int colIndex = 1;
      String name = item.getText(colIndex++);
      int type = ValueMetaFactory.getIdForValueMeta(item.getText(colIndex++));
      int length = Const.toInt(item.getText(colIndex++), -1);
      int precision = Const.toInt(item.getText(colIndex++), -1);
      meta.getSourceOutputFields().add(new MetaInjectOutputField(name, type, length, precision));
    }

    meta.setTargetFile(wTargetFile.getText());
    meta.setCreateParentFolder(wCreateParentFolder.getSelection());
    meta.setNoExecution(!wNoExecution.getSelection());

    final TransformMeta streamSourceTransform =
        pipelineMeta.findTransform(wStreamingSourceTransform.getText());
    meta.setStreamSourceTransform(streamSourceTransform);
    // Save streamSourceTransformName to find streamSourceTransform when loading
    meta.setStreamSourceTransformName(
        streamSourceTransform != null ? streamSourceTransform.getName() : "");
    meta.setStreamTargetTransformName(wStreamingTargetTransform.getText());

    meta.setTargetSourceMapping(targetSourceMapping);
    meta.setChanged(true);
  }

  private class MappingSource {
    public TransformMeta transformMeta;
    public IValueMeta valueMeta;

    public MappingSource(TransformMeta transformMeta, IValueMeta valueMeta) {
      this.transformMeta = transformMeta;
      this.valueMeta = valueMeta;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MappingSource that = (MappingSource) o;
      return Objects.equals(transformMeta, that.transformMeta)
          && Objects.equals(valueMeta, that.valueMeta);
    }
  }

  private class MappingTarget {
    public TransformMeta transformMeta;
    public String attributeKey;
    public boolean detail;

    public MappingTarget(TransformMeta transformMeta, String attributeKey, boolean detail) {
      this.transformMeta = transformMeta;
      this.attributeKey = attributeKey;
      this.detail = detail;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MappingTarget that = (MappingTarget) o;
      return Objects.equals(transformMeta, that.transformMeta)
          && Objects.equals(attributeKey, that.attributeKey);
    }
  }

  /** Enter the mapping between (unmapped) source fields and target fields. */
  private void enterMapping() {

    try {
      loadPipeline();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Title"),
          BaseMessages.getString(PKG, "MetaInjectDialog.ErrorLoadingSpecifiedPipeline.Message"),
          e);
      return;
    }

    MetaInjectMeta meta = new MetaInjectMeta();
    getInfo(meta);

    // The sources...
    //
    List<MappingSource> mappingSources = new ArrayList<>();
    List<String> sourceStrings = new ArrayList<>();
    Map<String, IRowMeta> sourceRowMetas = new HashMap<>();
    for (TransformMeta previousTransformMeta : pipelineMeta.findPreviousTransforms(transformMeta)) {
      try {
        IRowMeta previousRowMeta =
            pipelineMeta.getTransformFields(variables, previousTransformMeta);
        // Remember this for later...
        //
        sourceRowMetas.put(previousTransformMeta.getName(), previousRowMeta);

        for (IValueMeta previousValueMeta : previousRowMeta.getValueMetaList()) {
          mappingSources.add(new MappingSource(previousTransformMeta, previousValueMeta));
          sourceStrings.add(previousTransformMeta.getName() + " - " + previousValueMeta.getName());
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            "Error",
            "Error determining output row of transform '" + previousTransformMeta.getName() + "'",
            e);
      }
    }

    // The targets...
    //
    List<MappingTarget> mappingTargets = new ArrayList<>();
    List<String> targetStrings = new ArrayList<>();
    for (TransformMeta transformMeta : injectPipelineMeta.getTransforms()) {
      ITransformMeta iTransformMeta = transformMeta.getTransform();
      if (BeanInjectionInfo.isInjectionSupported(iTransformMeta.getClass())) {
        // Add the groups...
        //
        BeanInjectionInfo transformInjectionInfo = new BeanInjectionInfo(iTransformMeta.getClass());
        List<BeanInjectionInfo.Group> groupsList = transformInjectionInfo.getGroups();
        for (BeanInjectionInfo.Group group : groupsList) {
          boolean detail = StringUtils.isNotEmpty(group.getKey());
          List<BeanInjectionInfo.Property> propertyList = group.getProperties();
          for (BeanInjectionInfo.Property property : propertyList) {
            mappingTargets.add(new MappingTarget(transformMeta, property.getKey(), detail));
            String groupName = group.getKey();
            String targetString = transformMeta.getName() + " | ";
            if (StringUtils.isNotEmpty(groupName)) {
              targetString += groupName + " - ";
            }
            targetString += property.getKey();
            targetString += " : ";
            targetString += property.getTranslatedDescription();
            targetStrings.add(targetString);
          }
        }
      }
    }

    // Calculate the existing mappings...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping =
        meta.getTargetSourceMapping();
    for (TargetTransformAttribute targetTransformAttribute : targetSourceMapping.keySet()) {
      SourceTransformField sourceTransformField = targetSourceMapping.get(targetTransformAttribute);
      if (sourceTransformField == null) {
        continue;
      }
      int sourceIndex = -1;
      TransformMeta sourceTransformMeta =
          pipelineMeta.findTransform(sourceTransformField.getTransformName());
      if (sourceTransformMeta != null) {
        IRowMeta sourceRowMeta = sourceRowMetas.get(sourceTransformMeta.getName());
        if (sourceRowMeta != null) {
          IValueMeta sourceValueMeta =
              sourceRowMeta.searchValueMeta(sourceTransformField.getField());
          if (sourceValueMeta != null) {
            MappingSource mappingSource = new MappingSource(sourceTransformMeta, sourceValueMeta);
            sourceIndex = mappingSources.indexOf(mappingSource);
          }
        }
      }
      int targetIndex = -1;
      TransformMeta targetTransformMeta =
          injectPipelineMeta.findTransform(targetTransformAttribute.getTransformName());
      if (targetTransformMeta != null) {
        MappingTarget mapingTarget =
            new MappingTarget(
                targetTransformMeta,
                targetTransformAttribute.getAttributeKey(),
                targetTransformAttribute.isDetail());
        targetIndex = mappingTargets.indexOf(mapingTarget);
      }
      if (sourceIndex >= 0 && targetIndex >= 0) {
        mappings.add(new SourceToTargetMapping(sourceIndex, targetIndex));
      }
    }

    String[] src = sourceStrings.toArray(new String[0]);
    String[] tgt = targetStrings.toArray(new String[0]);
    EnterMappingDialog dialog = new EnterMappingDialog(shell, src, tgt, mappings);
    dialog.setSourceSeparator(" - ");
    dialog.setTargetSeparator(" : ");

    List<SourceToTargetMapping> newMappings = dialog.open();
    if (newMappings != null) {

      // Add the mappings...
      //
      targetSourceMapping.clear();

      for (SourceToTargetMapping newMapping : newMappings) {
        MappingSource mappingSource = mappingSources.get(newMapping.getSourcePosition());
        SourceTransformField sourceTransformField =
            new SourceTransformField(
                mappingSource.transformMeta.getName(), mappingSource.valueMeta.getName());

        MappingTarget mappingTarget = mappingTargets.get(newMapping.getTargetPosition());
        TargetTransformAttribute targetTransformAttribute =
            new TargetTransformAttribute(
                mappingTarget.transformMeta.getName(),
                mappingTarget.attributeKey,
                mappingTarget.detail);

        targetSourceMapping.put(targetTransformAttribute, sourceTransformField);

        // Refresh the tree...
        //
        refreshTree();
      }
    }
  }
}
