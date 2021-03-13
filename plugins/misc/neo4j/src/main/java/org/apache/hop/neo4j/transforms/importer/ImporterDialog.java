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
 *
 */

package org.apache.hop.neo4j.transforms.importer;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ImporterDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = ImporterMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;

  private CCombo wFilenameField;
  private CCombo wFileTypeField;
  private TextVar wDatabaseFilename;
  private TextVar wAdminCommand;
  private TextVar wBaseFolder;

  private Button wVerbose;
  private Button wHighIo;
  private Button wCacheOnHeap;
  private Button wIgnoreEmptyStrings;
  private Button wIgnoreExtraColumns;
  private Button wLegacyStyleQuoting;
  private Button wMultiLine;
  private Button wNormalizeTypes;
  private Button wSkipBadEntriesLogging;
  private Button wSkipBadRelationships;
  private Button wSkipDuplicateNodes;
  private Button wTrimStrings;
  private TextVar wBadTolerance;
  private TextVar wMaxMemory;
  private TextVar wReadBufferSize;
  private TextVar wProcessors;

  private ImporterMeta input;

  public ImporterDialog(
      Shell parent,
      IVariables variables,
      Object inputMetadata,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, pipelineMeta, transformName);
    input = (ImporterMeta) inputMetadata;

    metadataProvider = HopGui.getInstance().getMetadataProvider();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout shellLayout = new FormLayout();
    shell.setLayout(shellLayout);
    shell.setText("Neo4j Importer");

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite =
        new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout(scFormLayout);
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment(0, 0);
    fdSComposite.right = new FormAttachment(100, 0);
    fdSComposite.top = new FormAttachment(0, 0);
    fdSComposite.bottom = new FormAttachment(100, 0);
    wScrolledComposite.setLayoutData(fdSComposite);

    Composite wComposite = new Composite(wScrolledComposite, SWT.NONE);
    props.setLook(wComposite);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    wComposite.setLayoutData(fdComposite);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout(formLayout);

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transform name line
    //
    Label wlTransformName = new Label(wComposite, SWT.RIGHT);
    wlTransformName.setText("Transform name");
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    String[] fieldnames = new String[] {};
    try {
      fieldnames = pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames();
    } catch (HopTransformException e) {
      log.logError("error getting input field names: ", e);
    }

    // Filename field
    //
    Label wlFilenameField = new Label(wComposite, SWT.RIGHT);
    wlFilenameField.setText("Filename field ");
    props.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, 2 * margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new CCombo(wComposite, SWT.CHECK | SWT.BORDER);
    wFilenameField.setItems(fieldnames);
    props.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.right = new FormAttachment(100, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    wFilenameField.setLayoutData(fdFilenameField);
    lastControl = wlFilenameField;

    // FileType field
    //
    Label wlFileTypeField = new Label(wComposite, SWT.RIGHT);
    wlFileTypeField.setText("File type field ");
    props.setLook(wlFileTypeField);
    FormData fdlFileTypeField = new FormData();
    fdlFileTypeField.left = new FormAttachment(0, 0);
    fdlFileTypeField.right = new FormAttachment(middle, -margin);
    fdlFileTypeField.top = new FormAttachment(lastControl, 2 * margin);
    wlFileTypeField.setLayoutData(fdlFileTypeField);
    wFileTypeField = new CCombo(wComposite, SWT.CHECK | SWT.BORDER);
    wFileTypeField.setItems(fieldnames);
    props.setLook(wFileTypeField);
    FormData fdFileTypeField = new FormData();
    fdFileTypeField.left = new FormAttachment(middle, 0);
    fdFileTypeField.right = new FormAttachment(100, 0);
    fdFileTypeField.top = new FormAttachment(wlFileTypeField, 0, SWT.CENTER);
    wFileTypeField.setLayoutData(fdFileTypeField);
    lastControl = wlFileTypeField;

    // The database filename to gencsv to
    //
    Label wlDatabaseFilename = new Label(wComposite, SWT.RIGHT);
    wlDatabaseFilename.setText("Database filename ");
    props.setLook(wlDatabaseFilename);
    FormData fdlDatabaseFilename = new FormData();
    fdlDatabaseFilename.left = new FormAttachment(0, 0);
    fdlDatabaseFilename.right = new FormAttachment(middle, -margin);
    fdlDatabaseFilename.top = new FormAttachment(lastControl, 2 * margin);
    wlDatabaseFilename.setLayoutData(fdlDatabaseFilename);
    wDatabaseFilename = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDatabaseFilename);
    wDatabaseFilename.addModifyListener(lsMod);
    FormData fdDatabaseFilename = new FormData();
    fdDatabaseFilename.left = new FormAttachment(middle, 0);
    fdDatabaseFilename.right = new FormAttachment(100, 0);
    fdDatabaseFilename.top = new FormAttachment(wlDatabaseFilename, 0, SWT.CENTER);
    wDatabaseFilename.setLayoutData(fdDatabaseFilename);
    lastControl = wDatabaseFilename;

    // The path to the neo4j-admin command to use
    //
    Label wlAdminCommand = new Label(wComposite, SWT.RIGHT);
    wlAdminCommand.setText("neo4j-admin command path ");
    props.setLook(wlAdminCommand);
    FormData fdlAdminCommand = new FormData();
    fdlAdminCommand.left = new FormAttachment(0, 0);
    fdlAdminCommand.right = new FormAttachment(middle, -margin);
    fdlAdminCommand.top = new FormAttachment(lastControl, 2 * margin);
    wlAdminCommand.setLayoutData(fdlAdminCommand);
    wAdminCommand = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAdminCommand);
    wAdminCommand.addModifyListener(lsMod);
    FormData fdAdminCommand = new FormData();
    fdAdminCommand.left = new FormAttachment(middle, 0);
    fdAdminCommand.right = new FormAttachment(100, 0);
    fdAdminCommand.top = new FormAttachment(wlAdminCommand, 0, SWT.CENTER);
    wAdminCommand.setLayoutData(fdAdminCommand);
    lastControl = wAdminCommand;

    // The base folder to run the command from
    //
    Label wlBaseFolder = new Label(wComposite, SWT.RIGHT);
    wlBaseFolder.setText("Base folder (below import/ folder) ");
    props.setLook(wlBaseFolder);
    FormData fdlBaseFolder = new FormData();
    fdlBaseFolder.left = new FormAttachment(0, 0);
    fdlBaseFolder.right = new FormAttachment(middle, -margin);
    fdlBaseFolder.top = new FormAttachment(lastControl, 2 * margin);
    wlBaseFolder.setLayoutData(fdlBaseFolder);
    wBaseFolder = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBaseFolder);
    wBaseFolder.addModifyListener(lsMod);
    FormData fdBaseFolder = new FormData();
    fdBaseFolder.left = new FormAttachment(middle, 0);
    fdBaseFolder.right = new FormAttachment(100, 0);
    fdBaseFolder.top = new FormAttachment(wlBaseFolder, 0, SWT.CENTER);
    wBaseFolder.setLayoutData(fdBaseFolder);
    lastControl = wBaseFolder;

    // Verbose output?
    //
    Label wlVerbose = new Label(wComposite, SWT.RIGHT);
    wlVerbose.setText("Verbose output? ");
    props.setLook(wlVerbose);
    FormData fdlVerbose = new FormData();
    fdlVerbose.left = new FormAttachment(0, 0);
    fdlVerbose.right = new FormAttachment(middle, -margin);
    fdlVerbose.top = new FormAttachment(lastControl, 2 * margin);
    wlVerbose.setLayoutData(fdlVerbose);
    wVerbose = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wVerbose);
    FormData fdVerbose = new FormData();
    fdVerbose.left = new FormAttachment(middle, 0);
    fdVerbose.right = new FormAttachment(100, 0);
    fdVerbose.top = new FormAttachment(wlVerbose, 0, SWT.CENTER);
    wVerbose.setLayoutData(fdVerbose);
    lastControl = wlVerbose;

    // High IO?
    //
    Label wlHighIo = new Label(wComposite, SWT.RIGHT);
    wlHighIo.setText("High IO? ");
    props.setLook(wlHighIo);
    FormData fdlHighIo = new FormData();
    fdlHighIo.left = new FormAttachment(0, 0);
    fdlHighIo.right = new FormAttachment(middle, -margin);
    fdlHighIo.top = new FormAttachment(lastControl, 2 * margin);
    wlHighIo.setLayoutData(fdlHighIo);
    wHighIo = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wHighIo);
    FormData fdHighIo = new FormData();
    fdHighIo.left = new FormAttachment(middle, 0);
    fdHighIo.right = new FormAttachment(100, 0);
    fdHighIo.top = new FormAttachment(wlHighIo, 0, SWT.CENTER);
    wHighIo.setLayoutData(fdHighIo);
    lastControl = wlHighIo;

    // Cache on heap?
    //
    Label wlCacheOnHeap = new Label(wComposite, SWT.RIGHT);
    wlCacheOnHeap.setText("Cache on heap? ");
    props.setLook(wlCacheOnHeap);
    FormData fdlCacheOnHeap = new FormData();
    fdlCacheOnHeap.left = new FormAttachment(0, 0);
    fdlCacheOnHeap.right = new FormAttachment(middle, -margin);
    fdlCacheOnHeap.top = new FormAttachment(lastControl, 2 * margin);
    wlCacheOnHeap.setLayoutData(fdlCacheOnHeap);
    wCacheOnHeap = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wCacheOnHeap);
    FormData fdCacheOnHeap = new FormData();
    fdCacheOnHeap.left = new FormAttachment(middle, 0);
    fdCacheOnHeap.right = new FormAttachment(100, 0);
    fdCacheOnHeap.top = new FormAttachment(wlCacheOnHeap, 0, SWT.CENTER);
    wCacheOnHeap.setLayoutData(fdCacheOnHeap);
    lastControl = wlCacheOnHeap;

    // Ignore empty strings?
    //
    Label wlIgnoreEmptyStrings = new Label(wComposite, SWT.RIGHT);
    wlIgnoreEmptyStrings.setText("Ignore extra columns? ");
    props.setLook(wlIgnoreEmptyStrings);
    FormData fdlIgnoreEmptyStrings = new FormData();
    fdlIgnoreEmptyStrings.left = new FormAttachment(0, 0);
    fdlIgnoreEmptyStrings.right = new FormAttachment(middle, -margin);
    fdlIgnoreEmptyStrings.top = new FormAttachment(lastControl, 2 * margin);
    wlIgnoreEmptyStrings.setLayoutData(fdlIgnoreEmptyStrings);
    wIgnoreEmptyStrings = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wIgnoreEmptyStrings);
    FormData fdIgnoreEmptyStrings = new FormData();
    fdIgnoreEmptyStrings.left = new FormAttachment(middle, 0);
    fdIgnoreEmptyStrings.right = new FormAttachment(100, 0);
    fdIgnoreEmptyStrings.top = new FormAttachment(wlIgnoreEmptyStrings, 0, SWT.CENTER);
    wIgnoreEmptyStrings.setLayoutData(fdIgnoreEmptyStrings);
    lastControl = wIgnoreEmptyStrings;

    // Ignore extra columns?
    //
    Label wlIgnoreExtraColumns = new Label(wComposite, SWT.RIGHT);
    wlIgnoreExtraColumns.setText("Ignore extra columns? ");
    props.setLook(wlIgnoreExtraColumns);
    FormData fdlIgnoreExtraColumns = new FormData();
    fdlIgnoreExtraColumns.left = new FormAttachment(0, 0);
    fdlIgnoreExtraColumns.right = new FormAttachment(middle, -margin);
    fdlIgnoreExtraColumns.top = new FormAttachment(lastControl, 2 * margin);
    wlIgnoreExtraColumns.setLayoutData(fdlIgnoreExtraColumns);
    wIgnoreExtraColumns = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wIgnoreExtraColumns);
    FormData fdIgnoreExtraColumns = new FormData();
    fdIgnoreExtraColumns.left = new FormAttachment(middle, 0);
    fdIgnoreExtraColumns.right = new FormAttachment(100, 0);
    fdIgnoreExtraColumns.top = new FormAttachment(wlIgnoreExtraColumns, 0, SWT.CENTER);
    wIgnoreExtraColumns.setLayoutData(fdIgnoreExtraColumns);
    lastControl = wIgnoreExtraColumns;

    // Legacy style quoting?
    //
    Label wlLegacyStyleQuoting = new Label(wComposite, SWT.RIGHT);
    wlLegacyStyleQuoting.setText("Legacy style quoting? ");
    props.setLook(wlLegacyStyleQuoting);
    FormData fdlLegacyStyleQuoting = new FormData();
    fdlLegacyStyleQuoting.left = new FormAttachment(0, 0);
    fdlLegacyStyleQuoting.right = new FormAttachment(middle, -margin);
    fdlLegacyStyleQuoting.top = new FormAttachment(lastControl, 2 * margin);
    wlLegacyStyleQuoting.setLayoutData(fdlLegacyStyleQuoting);
    wLegacyStyleQuoting = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wLegacyStyleQuoting);
    FormData fdLegacyStyleQuoting = new FormData();
    fdLegacyStyleQuoting.left = new FormAttachment(middle, 0);
    fdLegacyStyleQuoting.right = new FormAttachment(100, 0);
    fdLegacyStyleQuoting.top = new FormAttachment(wlLegacyStyleQuoting, 0, SWT.CENTER);
    wLegacyStyleQuoting.setLayoutData(fdLegacyStyleQuoting);
    lastControl = wlLegacyStyleQuoting;

    // Whether or not fields from input source can span multiple lines
    //
    Label wlMultiLine = new Label(wComposite, SWT.RIGHT);
    wlMultiLine.setText("Fields can have multi-line data? ");
    props.setLook(wlMultiLine);
    FormData fdlMultiLine = new FormData();
    fdlMultiLine.left = new FormAttachment(0, 0);
    fdlMultiLine.right = new FormAttachment(middle, -margin);
    fdlMultiLine.top = new FormAttachment(lastControl, 2 * margin);
    wlMultiLine.setLayoutData(fdlMultiLine);
    wMultiLine = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wMultiLine);
    FormData fdMultiLine = new FormData();
    fdMultiLine.left = new FormAttachment(middle, 0);
    fdMultiLine.right = new FormAttachment(100, 0);
    fdMultiLine.top = new FormAttachment(wlMultiLine, 0, SWT.CENTER);
    wMultiLine.setLayoutData(fdMultiLine);
    lastControl = wlMultiLine;

    // Whether or not fields from input source can span multiple lines
    //
    Label wlNormalizeTypes = new Label(wComposite, SWT.RIGHT);
    wlNormalizeTypes.setText("Normalize types? ");
    props.setLook(wlNormalizeTypes);
    FormData fdlNormalizeTypes = new FormData();
    fdlNormalizeTypes.left = new FormAttachment(0, 0);
    fdlNormalizeTypes.right = new FormAttachment(middle, -margin);
    fdlNormalizeTypes.top = new FormAttachment(lastControl, 2 * margin);
    wlNormalizeTypes.setLayoutData(fdlNormalizeTypes);
    wNormalizeTypes = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wNormalizeTypes);
    FormData fdNormalizeTypes = new FormData();
    fdNormalizeTypes.left = new FormAttachment(middle, 0);
    fdNormalizeTypes.right = new FormAttachment(100, 0);
    fdNormalizeTypes.top = new FormAttachment(wlNormalizeTypes, 0, SWT.CENTER);
    wNormalizeTypes.setLayoutData(fdNormalizeTypes);
    lastControl = wlNormalizeTypes;

    // skip logging bad entries detected during import
    //
    Label wlSkipBadEntriesLogging = new Label(wComposite, SWT.RIGHT);
    wlSkipBadEntriesLogging.setText("Skip logging bad entries during import? ");
    props.setLook(wlSkipBadEntriesLogging);
    FormData fdlSkipBadEntriesLogging = new FormData();
    fdlSkipBadEntriesLogging.left = new FormAttachment(0, 0);
    fdlSkipBadEntriesLogging.right = new FormAttachment(middle, -margin);
    fdlSkipBadEntriesLogging.top = new FormAttachment(lastControl, 2 * margin);
    wlSkipBadEntriesLogging.setLayoutData(fdlSkipBadEntriesLogging);
    wSkipBadEntriesLogging = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wSkipBadEntriesLogging);
    FormData fdSkipBadEntriesLogging = new FormData();
    fdSkipBadEntriesLogging.left = new FormAttachment(middle, 0);
    fdSkipBadEntriesLogging.right = new FormAttachment(100, 0);
    fdSkipBadEntriesLogging.top = new FormAttachment(wlSkipBadEntriesLogging, 0, SWT.CENTER);
    wSkipBadEntriesLogging.setLayoutData(fdSkipBadEntriesLogging);
    lastControl = wlSkipBadEntriesLogging;

    // Whether or not to skip importing relationships that refers to missing node ids
    //
    Label wlSkipBadRelationships = new Label(wComposite, SWT.RIGHT);
    wlSkipBadRelationships.setText("Skip bad relationships? ");
    props.setLook(wlSkipBadRelationships);
    FormData fdlSkipBadRelationships = new FormData();
    fdlSkipBadRelationships.left = new FormAttachment(0, 0);
    fdlSkipBadRelationships.right = new FormAttachment(middle, -margin);
    fdlSkipBadRelationships.top = new FormAttachment(lastControl, 2 * margin);
    wlSkipBadRelationships.setLayoutData(fdlSkipBadRelationships);
    wSkipBadRelationships = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wSkipBadRelationships);
    FormData fdSkipBadRelationships = new FormData();
    fdSkipBadRelationships.left = new FormAttachment(middle, 0);
    fdSkipBadRelationships.right = new FormAttachment(100, 0);
    fdSkipBadRelationships.top = new FormAttachment(wlSkipBadRelationships, 0, SWT.CENTER);
    wSkipBadRelationships.setLayoutData(fdSkipBadRelationships);
    lastControl = wlSkipBadRelationships;

    // Ignore duplicate nodes?
    //
    Label wlSkipDuplicateNodes = new Label(wComposite, SWT.RIGHT);
    wlSkipDuplicateNodes.setText("Skip duplicate nodes? ");
    props.setLook(wlSkipDuplicateNodes);
    FormData fdlSkipDuplicateNodes = new FormData();
    fdlSkipDuplicateNodes.left = new FormAttachment(0, 0);
    fdlSkipDuplicateNodes.right = new FormAttachment(middle, -margin);
    fdlSkipDuplicateNodes.top = new FormAttachment(lastControl, 2 * margin);
    wlSkipDuplicateNodes.setLayoutData(fdlSkipDuplicateNodes);
    wSkipDuplicateNodes = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wSkipDuplicateNodes);
    FormData fdSkipDuplicateNodes = new FormData();
    fdSkipDuplicateNodes.left = new FormAttachment(middle, 0);
    fdSkipDuplicateNodes.right = new FormAttachment(100, 0);
    fdSkipDuplicateNodes.top = new FormAttachment(wlSkipDuplicateNodes, 0, SWT.CENTER);
    wSkipDuplicateNodes.setLayoutData(fdSkipDuplicateNodes);
    lastControl = wlSkipDuplicateNodes;

    // Ignore duplicate nodes?
    //
    Label wlTrimStrings = new Label(wComposite, SWT.RIGHT);
    wlTrimStrings.setText("Trim strings? ");
    props.setLook(wlTrimStrings);
    FormData fdlTrimStrings = new FormData();
    fdlTrimStrings.left = new FormAttachment(0, 0);
    fdlTrimStrings.right = new FormAttachment(middle, -margin);
    fdlTrimStrings.top = new FormAttachment(lastControl, 2 * margin);
    wlTrimStrings.setLayoutData(fdlTrimStrings);
    wTrimStrings = new Button(wComposite, SWT.CHECK | SWT.LEFT);
    props.setLook(wTrimStrings);
    FormData fdTrimStrings = new FormData();
    fdTrimStrings.left = new FormAttachment(middle, 0);
    fdTrimStrings.right = new FormAttachment(100, 0);
    fdTrimStrings.top = new FormAttachment(wlTrimStrings, 0, SWT.CENTER);
    wTrimStrings.setLayoutData(fdTrimStrings);
    lastControl = wlTrimStrings;

    // The max memory used
    //
    Label wlBadTolerance = new Label(wComposite, SWT.RIGHT);
    wlBadTolerance.setText("Bad tolerance");
    String ttBadTolerance = "Number of bad entries before the import is considered failed";
    wlBadTolerance.setToolTipText(ttBadTolerance);
    props.setLook(wlBadTolerance);
    FormData fdlBadTolerance = new FormData();
    fdlBadTolerance.left = new FormAttachment(0, 0);
    fdlBadTolerance.right = new FormAttachment(middle, -margin);
    fdlBadTolerance.top = new FormAttachment(lastControl, 2 * margin);
    wlBadTolerance.setLayoutData(fdlBadTolerance);
    wBadTolerance = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBadTolerance.setToolTipText(ttBadTolerance);
    props.setLook(wBadTolerance);
    wBadTolerance.addModifyListener(lsMod);
    FormData fdBadTolerance = new FormData();
    fdBadTolerance.left = new FormAttachment(middle, 0);
    fdBadTolerance.right = new FormAttachment(100, 0);
    fdBadTolerance.top = new FormAttachment(wlBadTolerance, 0, SWT.CENTER);
    wBadTolerance.setLayoutData(fdBadTolerance);
    lastControl = wBadTolerance;

    // The max memory used
    //
    Label wlMaxMemory = new Label(wComposite, SWT.RIGHT);
    wlMaxMemory.setText("Max memory) ");
    props.setLook(wlMaxMemory);
    FormData fdlMaxMemory = new FormData();
    fdlMaxMemory.left = new FormAttachment(0, 0);
    fdlMaxMemory.right = new FormAttachment(middle, -margin);
    fdlMaxMemory.top = new FormAttachment(lastControl, 2 * margin);
    wlMaxMemory.setLayoutData(fdlMaxMemory);
    wMaxMemory = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaxMemory);
    wMaxMemory.addModifyListener(lsMod);
    FormData fdMaxMemory = new FormData();
    fdMaxMemory.left = new FormAttachment(middle, 0);
    fdMaxMemory.right = new FormAttachment(100, 0);
    fdMaxMemory.top = new FormAttachment(wlMaxMemory, 0, SWT.CENTER);
    wMaxMemory.setLayoutData(fdMaxMemory);
    lastControl = wMaxMemory;

    // Size of each buffer for reading input data. It has to at least be large enough
    // to hold the biggest single value in the input data.
    //
    Label wlReadBufferSize = new Label(wComposite, SWT.RIGHT);
    wlReadBufferSize.setText("Read buffer size) ");
    props.setLook(wlReadBufferSize);
    FormData fdlReadBufferSize = new FormData();
    fdlReadBufferSize.left = new FormAttachment(0, 0);
    fdlReadBufferSize.right = new FormAttachment(middle, -margin);
    fdlReadBufferSize.top = new FormAttachment(lastControl, 2 * margin);
    wlReadBufferSize.setLayoutData(fdlReadBufferSize);
    wReadBufferSize = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wReadBufferSize);
    wReadBufferSize.addModifyListener(lsMod);
    FormData fdReadBufferSize = new FormData();
    fdReadBufferSize.left = new FormAttachment(middle, 0);
    fdReadBufferSize.right = new FormAttachment(100, 0);
    fdReadBufferSize.top = new FormAttachment(wlReadBufferSize, 0, SWT.CENTER);
    wReadBufferSize.setLayoutData(fdReadBufferSize);
    lastControl = wReadBufferSize;

    // Processors
    //
    Label wlProcessors = new Label(wComposite, SWT.RIGHT);
    wlProcessors.setText("Processors) ");
    props.setLook(wlProcessors);
    FormData fdlProcessors = new FormData();
    fdlProcessors.left = new FormAttachment(0, 0);
    fdlProcessors.right = new FormAttachment(middle, -margin);
    fdlProcessors.top = new FormAttachment(lastControl, 2 * margin);
    wlProcessors.setLayoutData(fdlProcessors);
    wProcessors = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wProcessors);
    wProcessors.addModifyListener(lsMod);
    FormData fdProcessors = new FormData();
    fdProcessors.left = new FormAttachment(middle, 0);
    fdProcessors.right = new FormAttachment(100, 0);
    fdProcessors.top = new FormAttachment(wlProcessors, 0, SWT.CENTER);
    wProcessors.setLayoutData(fdProcessors);
    lastControl = wProcessors;

    // Some buttons
    wOk = new Button(wComposite, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(wComposite, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent(wComposite);

    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);
    wScrolledComposite.setMinWidth(bounds.width);
    wScrolledComposite.setMinHeight(bounds.height);

    // Add listeners
    //
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wFilenameField.addSelectionListener(lsDef);
    wFileTypeField.addSelectionListener(lsDef);
    wDatabaseFilename.addSelectionListener(lsDef);
    wAdminCommand.addSelectionListener(lsDef);
    wBaseFolder.addSelectionListener(lsDef);
    wBadTolerance.addSelectionListener(lsDef);
    wMaxMemory.addSelectionListener(lsDef);
    wReadBufferSize.addSelectionListener(lsDef);
    wProcessors.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();
    input.setChanged(changed);

    shell.open();

    // Set the shell size, based upon previous time...
    setSize();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {

    wTransformName.setText(Const.NVL(transformName, ""));
    wFilenameField.setText(Const.NVL(input.getFilenameField(), ""));
    wFileTypeField.setText(Const.NVL(input.getFileTypeField(), ""));
    wDatabaseFilename.setText(Const.NVL(input.getDatabaseName(), ""));
    wAdminCommand.setText(Const.NVL(input.getAdminCommand(), ""));
    wBaseFolder.setText(Const.NVL(input.getBaseFolder(), ""));

    wHighIo.setSelection(input.isHighIo());
    wCacheOnHeap.setSelection(input.isCacheOnHeap());
    wIgnoreEmptyStrings.setSelection(input.isIgnoringEmptyStrings());
    wIgnoreExtraColumns.setSelection(input.isIgnoringExtraColumns());
    wLegacyStyleQuoting.setSelection(input.isQuotingLegacyStyle());
    wMultiLine.setSelection(input.isMultiLine());
    wNormalizeTypes.setSelection(input.isNormalizingTypes());
    wSkipBadEntriesLogging.setSelection(input.isSkippingBadEntriesLogging());
    wSkipBadRelationships.setSelection(input.isSkippingBadRelationships());
    wSkipDuplicateNodes.setSelection(input.isSkippingDuplicateNodes());
    wTrimStrings.setSelection(input.isTrimmingStrings());
    wBadTolerance.setText(Const.NVL(input.getBadTolerance(), ""));
    wMaxMemory.setText(Const.NVL(input.getMaxMemory(), ""));
    wReadBufferSize.setText(Const.NVL(input.getReadBufferSize(), ""));
    wProcessors.setText(Const.NVL(input.getProcessors(), ""));
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo(ImporterMeta meta) {
    meta.setFilenameField(wFilenameField.getText());
    meta.setFileTypeField(wFileTypeField.getText());
    meta.setAdminCommand(wAdminCommand.getText());
    meta.setDatabaseName(wDatabaseFilename.getText());
    meta.setBaseFolder(wBaseFolder.getText());

    meta.setHighIo(wHighIo.getSelection());
    meta.setCacheOnHeap(wCacheOnHeap.getSelection());
    meta.setIgnoringEmptyStrings(wIgnoreEmptyStrings.getSelection());
    meta.setIgnoringExtraColumns(wIgnoreExtraColumns.getSelection());
    meta.setQuotingLegacyStyle(wLegacyStyleQuoting.getSelection());
    meta.setMultiLine(wMultiLine.getSelection());
    meta.setNormalizingTypes(wNormalizeTypes.getSelection());
    meta.setSkippingBadEntriesLogging(wSkipBadEntriesLogging.getSelection());
    meta.setSkippingBadRelationships(wSkipBadRelationships.getSelection());
    meta.setSkippingDuplicateNodes(wSkipDuplicateNodes.getSelection());
    meta.setBadTolerance(wBadTolerance.getText());
    meta.setMaxMemory(wMaxMemory.getText());
    meta.setReadBufferSize(wReadBufferSize.getText());
    meta.setProcessors(wProcessors.getText());
  }
}
