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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;


import java.nio.charset.Charset;
import java.util.List;
import java.util.*;

public class JsonOutputDialog extends BaseTransformDialog implements ITransformDialog {
    private static Class<?> PKG = JsonOutputMeta.class; // needed by Translator!!

    public static final String STRING_SORT_WARNING_PARAMETER = "JSONSortWarning";

    private CTabItem wGeneralTab, wFieldsTab, wKeyConfigTab, wAdditionalFieldsConfigTab;

    private Label wlEncoding;
    private ComboVar wEncoding;

    private Label wlOutputValue;
    private TextVar wOutputValue;

    private Label wlUseArrayWithSingleInstance;
    private Button wUseArrayWithSingleInstance;

    private Label wlBlocName;
    private TextVar wBlocName;

    private TableView wFields;

    private JsonOutputMeta input;

    private boolean gotEncodings = false;
    private boolean gotPreviousFields = false;

    private ColumnInfo[] colinf;
    private ColumnInfo[] keyColInf;

    private TableView wKeyFields;

    private Label wlAddToResult;
    private Button wAddToResult;

    private Group wFileName;

    private Label wlFilename;
    private Button wbFilename;
    private TextVar wFilename;

    private Label wlExtension;
    private TextVar wExtension;

    private Label wlCreateParentFolder;
    private Button wCreateParentFolder;

    private Label wlDoNotOpenNewFileInit;
    private Button wDoNotOpenNewFileInit;

    private Label wlAddDate;
    private Button wAddDate;

    private Label wlJSONPrittified;
    private Button wJSONPrittified;

    private Label wlSplitOutputAfter;
    private TextVar wSplitOutputAfter;

    private Label wlAddTime;
    private Button wAddTime;

    private Button wbShowFiles;

    private Label wlAppend;
    private Button wAppend;

    private Label wlOperation;
    private CCombo wOperation;

    private Label wlJSONSizeFieldname;
    private TextVar wJSONSizeFieldname;

    private Group wSettings;

    private Map<String, Integer> inputFields;

    public JsonOutputDialog(Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
        super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
        input = (JsonOutputMeta) in;
        inputFields = new HashMap<>();
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, input);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
        };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "JsonOutputDialog.DialogTitle"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlTransformName = new Label(shell, SWT.RIGHT);
        wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
        props.setLook(wlTransformName);
        fdlTransformName = new FormData();
        fdlTransformName.left = new FormAttachment(0, 0);
        fdlTransformName.top = new FormAttachment(0, margin);
        fdlTransformName.right = new FormAttachment(middle, -margin);
        wlTransformName.setLayoutData(fdlTransformName);
        wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wTransformName.setText(transformName);
        props.setLook(wTransformName);
        wTransformName.addModifyListener(lsMod);
        fdTransformName = new FormData();
        fdTransformName.left = new FormAttachment(middle, 0);
        fdTransformName.top = new FormAttachment(0, margin);
        fdTransformName.right = new FormAttachment(100, 0);
        wTransformName.setLayoutData(fdTransformName);

        // Buttons at the bottom
        //
        wOk = new Button( shell, SWT.PUSH );
        wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        wOk.addListener( SWT.Selection, e->ok() );
        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
        wCancel.addListener( SWT.Selection, e->cancel() );
        setButtonPositions( new Button[] { wOk, wCancel }, margin, null);

        CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
        props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

        // ////////////////////////
        // START OF General TAB///
        // /
        wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
        wGeneralTab.setText(BaseMessages.getString(PKG, "JsonOutputDialog.GeneralTab.TabTitle"));

        FormLayout GeneralLayout = new FormLayout();
        GeneralLayout.marginWidth = 3;
        GeneralLayout.marginHeight = 20;

        Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wGeneralComp);
        wGeneralComp.setLayout(GeneralLayout);

        // Operation
        wlOperation = new Label(wGeneralComp, SWT.RIGHT);
        wlOperation.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Operation.Label"));
        props.setLook(wlOperation);
        FormData fdlOperation = new FormData();
        fdlOperation.left = new FormAttachment(0, 0);
        fdlOperation.right = new FormAttachment(middle, -margin);
        fdlOperation.top = new FormAttachment(wlBlocName, margin);
        wlOperation.setLayoutData(fdlOperation);

        wOperation = new CCombo(wGeneralComp, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wOperation);
        wOperation.addModifyListener(lsMod);
        FormData fdOperation = new FormData();
        fdOperation.left = new FormAttachment(middle, 0);
        fdOperation.top = new FormAttachment(wlBlocName, margin);
        fdOperation.right = new FormAttachment(100, -margin);
        wOperation.setLayoutData(fdOperation);
        wOperation.setItems(JsonOutputMeta.operationTypeDesc);
        wOperation.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                updateOperation();

            }
        });

        createSettingsGroup(lsMod, middle, margin, wGeneralComp);
        createFilenameGroup(lsMod, middle, margin, wGeneralComp);

        FormData fdGeneralComp = new FormData();
        fdGeneralComp.left = new FormAttachment(0, 0);
        fdGeneralComp.top = new FormAttachment(wTransformName, margin);
        fdGeneralComp.right = new FormAttachment(100, 0);
        fdGeneralComp.bottom = new FormAttachment(100, 0);
        wGeneralComp.setLayoutData(fdGeneralComp);


        wGeneralComp.layout();
        wGeneralTab.setControl(wGeneralComp);

        // ///////////////////////////////////////////////////////////
        // / END OF General TAB
        // ///////////////////////////////////////////////////////////

        // ///////////////////////////////////////////////////////////
        // START OF Key Configuration TAB///
        // ///////////////////////////////////////////////////////////
        wKeyConfigTab = new CTabItem(wTabFolder, SWT.NONE);
        wKeyConfigTab.setText(BaseMessages.getString(PKG, "JsonOutputDialog.KeyConfigTab.TabTitle"));

        FormLayout keyConfigLayout = new FormLayout();
        keyConfigLayout.marginWidth = 3;
        keyConfigLayout.marginHeight = 3;

        Composite wKeyConfigComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wKeyConfigComp);

        final int keyFieldsRows = input.getKeyFields().length;

        keyColInf =
                new ColumnInfo[]{
                        new ColumnInfo(BaseMessages.getString(PKG, "JsonOutputDialog.Fieldname.Column"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false),
                        new ColumnInfo(BaseMessages.getString(PKG, "JsonOutputDialog.ElementName.Column"),
                                ColumnInfo.COLUMN_TYPE_TEXT, false)
                };
        keyColInf[1].setUsingVariables(true);
        wKeyFields =
                new TableView(variables, wKeyConfigComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, keyColInf, keyFieldsRows, lsMod,
                        props);

        FormData fdKeyFields = new FormData();
        fdKeyFields.left = new FormAttachment(0, 0);
        fdKeyFields.top = new FormAttachment(0, 0);
        fdKeyFields.right = new FormAttachment(100, 0);
        fdKeyFields.bottom = new FormAttachment(100, 0);
        wKeyFields.setLayoutData(fdKeyFields);

        wKeyConfigComp.setLayout(keyConfigLayout);
        wKeyConfigComp.layout();
        wKeyConfigTab.setControl(wKeyConfigComp);

        // ///////////////////////////////////////////////////////////
        // END OF Key Configuration TAB
        // ///////////////////////////////////////////////////////////

        // ///////////////////////////////////////////////////////////
        // Fields tab...
        // ///////////////////////////////////////////////////////////
        wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
        wFieldsTab.setText(BaseMessages.getString(PKG, "JsonOutputDialog.FieldsTab.TabTitle"));

        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginWidth = Const.FORM_MARGIN;
        fieldsLayout.marginHeight = Const.FORM_MARGIN;

        Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
        wFieldsComp.setLayout(fieldsLayout);
        props.setLook(wFieldsComp);

        wGet = new Button(wFieldsComp, SWT.PUSH);
        wGet.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Get.Button"));
        wGet.setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.Get.Tooltip"));

        setButtonPositions(new Button[]{wGet}, margin, null);

        final int fieldsRows = input.getOutputFields().length;

        colinf =
                new ColumnInfo[]{
                        new ColumnInfo(BaseMessages.getString(PKG, "JsonOutputDialog.Fieldname.Column"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false),
                        new ColumnInfo(BaseMessages.getString(PKG, "JsonOutputDialog.ElementName.Column"),
                                ColumnInfo.COLUMN_TYPE_TEXT, false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "JsonOutputDialog.JSONFragment.Column"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{
                                BaseMessages.getString(PKG, "System.Combo.Yes"),
                                BaseMessages.getString(PKG, "System.Combo.No")}, true),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "JsonOutputDialog.RemoveIfBlank.Column"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{
                                BaseMessages.getString(PKG, "System.Combo.Yes"),
                                BaseMessages.getString(PKG, "System.Combo.No")}, true),
                };
        colinf[1].setUsingVariables(true);
        wFields =
                new TableView( variables, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, lsMod,
                        props );

        FormData fdFields = new FormData();
        fdFields.left = new FormAttachment(0, 0);
        fdFields.top = new FormAttachment(0, 0);
        fdFields.right = new FormAttachment(100, 0);
        fdFields.bottom = new FormAttachment(wGet, -margin);
        wFields.setLayoutData(fdFields);

        // ///////////////////////////////////////////////////////////
        // START OF Additional Fields Configuration TAB///
        // ///////////////////////////////////////////////////////////
        wAdditionalFieldsConfigTab = new CTabItem(wTabFolder, SWT.NONE);
        wAdditionalFieldsConfigTab.setText(BaseMessages.getString(PKG, "JsonOutputDialog.AdditionalFieldsConfigTab.TabTitle"));

        FormLayout additionalFieldsConfigLayout = new FormLayout();
        additionalFieldsConfigLayout.marginWidth = 3;
        additionalFieldsConfigLayout.marginHeight = 3;

        Composite wAdditionalFieldsConfigComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wAdditionalFieldsConfigComp);

        // JSON Size field
        wlJSONSizeFieldname = new Label(wAdditionalFieldsConfigComp, SWT.RIGHT);
        wlJSONSizeFieldname.setText(BaseMessages.getString(PKG, "JsonOutputDialog.JSONSize.Label"));
        props.setLook(wlJSONSizeFieldname);
        FormData fdlJSONSizeFieldname = new FormData();
        fdlJSONSizeFieldname.left = new FormAttachment(0, 0);
        fdlJSONSizeFieldname.right = new FormAttachment(middle, -margin);
        fdlJSONSizeFieldname.top = new FormAttachment(wlBlocName, margin);
        wlJSONSizeFieldname.setLayoutData(fdlJSONSizeFieldname);
        wJSONSizeFieldname = new TextVar(variables, wAdditionalFieldsConfigComp, SWT.BORDER | SWT.READ_ONLY);
        wJSONSizeFieldname.setEditable(true);
        props.setLook(wJSONSizeFieldname);
        wJSONSizeFieldname.addModifyListener(lsMod);
        FormData fdJSONSizeFieldname = new FormData();
        fdJSONSizeFieldname.left = new FormAttachment(middle, 0);
        fdJSONSizeFieldname.top = new FormAttachment(wBlocName, margin);
        fdJSONSizeFieldname.right = new FormAttachment(100, 0);
        wJSONSizeFieldname.setLayoutData(fdJSONSizeFieldname);

        wAdditionalFieldsConfigComp.setLayout(additionalFieldsConfigLayout);
        wAdditionalFieldsConfigComp.layout();
        wAdditionalFieldsConfigTab.setControl(wAdditionalFieldsConfigComp);

        // ///////////////////////////////////////////////////////////
        // END OF Additional Fields Configuration TAB
        // ///////////////////////////////////////////////////////////


        //
        // Search the fields in the background

        final Runnable runnable = () -> {
            TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
                if (transformMeta != null) {
                    try {
                        IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

                        // Remember these fields...
                        for (int i = 0; i < row.size(); i++) {
                            inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        }
                        setFieldListComboBoxes();
                    } catch (HopException e) {
                        logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
        };
        new Thread(runnable).start();

        FormData fdFieldsComp = new FormData();
        fdFieldsComp.left = new FormAttachment(0, 0);
        fdFieldsComp.top = new FormAttachment(0, 0);
        fdFieldsComp.right = new FormAttachment(100, 0);
        fdFieldsComp.bottom = new FormAttachment(100, 0);
        wFieldsComp.setLayoutData(fdFieldsComp);

        wFieldsComp.layout();
        wFieldsTab.setControl(wFieldsComp);

        FormData fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment(0, 0);
        fdTabFolder.top = new FormAttachment(wTransformName, margin);
        fdTabFolder.right = new FormAttachment(100, 0);
        fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
        wTabFolder.setLayoutData(fdTabFolder);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                ok();
            }
        };
        wGet.addListener( SWT.Selection, e -> get() );

        wTransformName.addSelectionListener( lsDef );
        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener( new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                cancel();
            }
        } );

        lsResize = event -> {
            Point size = shell.getSize();
            wFields.setSize( size.x - 10, size.y - 50 );
            wFields.table.setSize( size.x - 10, size.y - 50 );
            wFields.redraw();
        };
        shell.addListener( SWT.Resize, lsResize );

        wTabFolder.setSelection( 0 );

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        updateOperation();
        input.setChanged( changed );

        shell.open();
        while ( !shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }
        return transformName;
    }

    private void createSettingsGroup(ModifyListener lsMod, int middle, int margin, Composite wGeneralComp) {
        // ////////////////////////
        // START OF Settings GROUP
        //

        wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
        props.setLook(wSettings);
        wSettings.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Group.Settings.Label"));

        FormLayout groupFileLayout = new FormLayout();
        groupFileLayout.marginWidth = 10;
        groupFileLayout.marginHeight = 10;
        wSettings.setLayout(groupFileLayout);

        wlBlocName = new Label(wSettings, SWT.RIGHT);
        wlBlocName.setText(BaseMessages.getString(PKG, "JsonOutputDialog.BlocName.Label"));
        props.setLook(wlBlocName);
        FormData fdlBlocName = new FormData();
        fdlBlocName.left = new FormAttachment(0, 0);
        fdlBlocName.top = new FormAttachment(wlOperation, margin);
        fdlBlocName.right = new FormAttachment(middle, -margin);
        wlBlocName.setLayoutData(fdlBlocName);
        wBlocName = new TextVar(variables, wSettings, SWT.BORDER | SWT.READ_ONLY);
        wBlocName.setEditable(true);
        props.setLook(wBlocName);
        wBlocName.addModifyListener(lsMod);
        FormData fdBlocName = new FormData();
        fdBlocName.left = new FormAttachment(middle, 0);
        fdBlocName.top = new FormAttachment(wOperation, margin);
        fdBlocName.right = new FormAttachment(100, 0);
        wBlocName.setLayoutData(fdBlocName);

        wlOutputValue = new Label(wSettings, SWT.RIGHT);
        wlOutputValue.setText(BaseMessages.getString(PKG, "JsonOutputDialog.OutputValue.Label"));
        props.setLook(wlOutputValue);
        FormData fdlOutputValue = new FormData();
        fdlOutputValue.left = new FormAttachment(0, 0);
        fdlOutputValue.top = new FormAttachment(wBlocName, margin);
        fdlOutputValue.right = new FormAttachment(middle, -margin);
        wlOutputValue.setLayoutData(fdlOutputValue);
        wOutputValue = new TextVar(variables, wSettings, SWT.BORDER | SWT.READ_ONLY);
        wOutputValue.setEditable(true);
        wOutputValue.setToolTipText("JsonOutputDialog.OutputValue.Tooltip");
        props.setLook(wOutputValue);
        wOutputValue.addModifyListener(lsMod);
        FormData fdOutputValue = new FormData();
        fdOutputValue.left = new FormAttachment(middle, 0);
        fdOutputValue.top = new FormAttachment(wBlocName, margin);
        fdOutputValue.right = new FormAttachment(100, 0);
        wOutputValue.setLayoutData(fdOutputValue);

        wlUseArrayWithSingleInstance = new Label(wSettings, SWT.RIGHT);
        wlUseArrayWithSingleInstance.setText(BaseMessages.getString(PKG, "JsonOutputDialog.UseArrayWihSingleInstanceMode.Label"));
        props.setLook(wlUseArrayWithSingleInstance);
        FormData fdlUseArrayWithSingleInstance = new FormData();
        fdlUseArrayWithSingleInstance.left = new FormAttachment(0, 0);
        fdlUseArrayWithSingleInstance.top = new FormAttachment(wOutputValue, margin);
        fdlUseArrayWithSingleInstance.right = new FormAttachment(middle, -margin);
        wlUseArrayWithSingleInstance.setLayoutData(fdlUseArrayWithSingleInstance);
        wUseArrayWithSingleInstance = new Button(wSettings, SWT.CHECK);
        wUseArrayWithSingleInstance.setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.UseArrayWihSingleInstanceMode.Tooltip"));
        props.setLook(wUseArrayWithSingleInstance);
        FormData fdUseArrayWithSingleInstance = new FormData();
        fdUseArrayWithSingleInstance.left = new FormAttachment(middle, 0);
        fdUseArrayWithSingleInstance.top = new FormAttachment( wlUseArrayWithSingleInstance, 0, SWT.CENTER );
        fdUseArrayWithSingleInstance.right = new FormAttachment(100, 0);
        wUseArrayWithSingleInstance.setLayoutData(fdUseArrayWithSingleInstance);
        wUseArrayWithSingleInstance.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        wlJSONPrittified = new Label(wSettings, SWT.RIGHT);
        wlJSONPrittified.setText(BaseMessages.getString(PKG, "JsonOutputDialog.JSONPrittified.Label"));
        props.setLook(wlJSONPrittified);
        FormData fdlJSONPrittified = new FormData();
        fdlJSONPrittified.left = new FormAttachment(0, 0);
        fdlJSONPrittified.top = new FormAttachment(wUseArrayWithSingleInstance, margin);
        fdlJSONPrittified.right = new FormAttachment(middle, -margin);
        wlJSONPrittified.setLayoutData(fdlJSONPrittified);
        wJSONPrittified = new Button(wSettings, SWT.CHECK);
        wJSONPrittified.setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.JSONPrittified.Tooltip"));
        props.setLook(wJSONPrittified);
        FormData fdJSONPrittified = new FormData();
        fdJSONPrittified.left = new FormAttachment(middle, 0);
        fdJSONPrittified.top = new FormAttachment( wlJSONPrittified, 0, SWT.CENTER );
        fdJSONPrittified.right = new FormAttachment(100, 0);
        wJSONPrittified.setLayoutData(fdJSONPrittified);
        wJSONPrittified.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        FormData fdSettings = new FormData();
        fdSettings.left = new FormAttachment(0, margin);
        fdSettings.top = new FormAttachment(wOperation, 2 * margin);
        fdSettings.right = new FormAttachment(100, -margin);
        wSettings.setLayoutData(fdSettings);

        // ///////////////////////////////////////////////////////////
        // / END OF Settings GROUP
        // ///////////////////////////////////////////////////////////
    }

    private void createFilenameGroup(ModifyListener lsMod,
                                     int middle,
                                     int margin,
                                     Composite wGeneralComp) {
        // Connection grouping?
        // ////////////////////////
        // START OF FileName GROUP
        //

        wFileName = new Group(wGeneralComp, SWT.SHADOW_NONE);
        props.setLook(wFileName);
        wFileName.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Group.File.Label"));

        FormLayout groupfilenameayout = new FormLayout();
        groupfilenameayout.marginWidth = 10;
        groupfilenameayout.marginHeight = 10;
        wFileName.setLayout(groupfilenameayout);

        // Filename line
        wlFilename = new Label(wFileName, SWT.RIGHT);
        wlFilename.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Filename.Label"));
        props.setLook(wlFilename);
        FormData fdlFilename = new FormData();
        fdlFilename.left = new FormAttachment(0, 0);
        fdlFilename.top = new FormAttachment(wOperation, margin);
        fdlFilename.right = new FormAttachment(middle, -margin);
        wlFilename.setLayoutData(fdlFilename);

        wbFilename = new Button(wFileName, SWT.PUSH | SWT.CENTER);
        props.setLook(wbFilename);
        wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
        FormData fdbFilename = new FormData();
        fdbFilename.right = new FormAttachment(100, 0);
        fdbFilename.top = new FormAttachment(wSettings, 0);
        wbFilename.setLayoutData(fdbFilename);

        wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( true, shell, wFilename, variables,
                new String[] {"*.js", "*.json", "*"},
                new String[] {
                        BaseMessages.getString( PKG, "System.FileType.JsonFiles" ),
                        BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
                true )
        );

        wFilename = new TextVar(variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wFilename);
        wFilename.addModifyListener(lsMod);
        FormData fdFilename = new FormData();
        fdFilename.left = new FormAttachment(middle, 0);
        fdFilename.top = new FormAttachment(wOutputValue, margin);
        fdFilename.right = new FormAttachment(wbFilename, -margin);
        wFilename.setLayoutData(fdFilename);

        // Append to end of file?
        wlAppend = new Label(wFileName, SWT.RIGHT);
        wlAppend.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Append.Label"));
        props.setLook(wlAppend);
        FormData fdlAppend = new FormData();
        fdlAppend.left = new FormAttachment(0, 0);
        fdlAppend.top = new FormAttachment(wFilename, margin);
        fdlAppend.right = new FormAttachment(middle, -margin);
        wlAppend.setLayoutData(fdlAppend);
        wAppend = new Button(wFileName, SWT.CHECK);
        wAppend.setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.Append.Tooltip"));
        props.setLook(wAppend);
        FormData fdAppend = new FormData();
        fdAppend.left = new FormAttachment(middle, 0);
        fdAppend.top = new FormAttachment( wlAppend, 0, SWT.CENTER );
        fdAppend.right = new FormAttachment(100, 0);
        wAppend.setLayoutData(fdAppend);
        wAppend.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        wlSplitOutputAfter = new Label(wFileName, SWT.RIGHT);
        wlSplitOutputAfter.setText(BaseMessages.getString(PKG, "JsonOutputDialog.splitOutputAfter.Label"));
        props.setLook(wlSplitOutputAfter);
        FormData fdlSplitOutputAfter = new FormData();
        fdlSplitOutputAfter.left = new FormAttachment(0, 0);
        fdlSplitOutputAfter.top = new FormAttachment(wAppend, margin);
        fdlSplitOutputAfter.right = new FormAttachment(middle, -margin);
        wlSplitOutputAfter.setLayoutData(fdlSplitOutputAfter);
        wSplitOutputAfter = new TextVar(variables, wFileName, SWT.BORDER | SWT.READ_ONLY);
        wSplitOutputAfter.setEditable(true);
        wSplitOutputAfter.setToolTipText("JsonOutputDialog.splitOutputAfter.Tooltip");
        props.setLook(wSplitOutputAfter);
        FormData fdSplitOutputAfter = new FormData();
        fdSplitOutputAfter.left = new FormAttachment(middle, 0);
        fdSplitOutputAfter.top = new FormAttachment(wlAppend, margin);
        fdSplitOutputAfter.right = new FormAttachment(100, 0);
        wSplitOutputAfter.setLayoutData(fdSplitOutputAfter);
        wSplitOutputAfter.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        // Create Parent Folder
        wlCreateParentFolder = new Label(wFileName, SWT.RIGHT);
        wlCreateParentFolder.setText(BaseMessages.getString(PKG, "JsonOutputDialog.CreateParentFolder.Label"));
        props.setLook(wlCreateParentFolder);
        FormData fdlCreateParentFolder = new FormData();
        fdlCreateParentFolder.left = new FormAttachment(0, 0);
        fdlCreateParentFolder.top = new FormAttachment(wSplitOutputAfter, margin);
        fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
        wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
        wCreateParentFolder = new Button(wFileName, SWT.CHECK);
        wCreateParentFolder.setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.CreateParentFolder.Tooltip"));
        props.setLook(wCreateParentFolder);
        FormData fdCreateParentFolder = new FormData();
        fdCreateParentFolder.left = new FormAttachment(middle, 0);
        fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
        fdCreateParentFolder.right = new FormAttachment(100, 0);
        wCreateParentFolder.setLayoutData(fdCreateParentFolder);
        wCreateParentFolder.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        // Open new File at Init
        wlDoNotOpenNewFileInit = new Label(wFileName, SWT.RIGHT);
        wlDoNotOpenNewFileInit.setText(BaseMessages.getString(PKG, "JsonOutputDialog.DoNotOpenNewFileInit.Label"));
        props.setLook(wlDoNotOpenNewFileInit);
        FormData fdlDoNotOpenNewFileInit = new FormData();
        fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
        fdlDoNotOpenNewFileInit.top = new FormAttachment(wCreateParentFolder, margin);
        fdlDoNotOpenNewFileInit.right = new FormAttachment(middle, -margin);
        wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
        wDoNotOpenNewFileInit = new Button(wFileName, SWT.CHECK);
        wDoNotOpenNewFileInit
                .setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.DoNotOpenNewFileInit.Tooltip"));
        props.setLook(wDoNotOpenNewFileInit);
        FormData fdDoNotOpenNewFileInit = new FormData();
        fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
        fdDoNotOpenNewFileInit.top = new FormAttachment( wlDoNotOpenNewFileInit, 0, SWT.CENTER );
        fdDoNotOpenNewFileInit.right = new FormAttachment(100, 0);
        wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
        wDoNotOpenNewFileInit.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        createExtensionLine(lsMod, middle, margin);

        // Create multi-part file?
        wlAddDate = new Label(wFileName, SWT.RIGHT);
        wlAddDate.setText(BaseMessages.getString(PKG, "JsonOutputDialog.AddDate.Label"));
        props.setLook(wlAddDate);
        FormData fdlAddDate = new FormData();
        fdlAddDate.left = new FormAttachment(0, 0);
        fdlAddDate.top = new FormAttachment(wEncoding, margin);
        fdlAddDate.right = new FormAttachment(middle, -margin);
        wlAddDate.setLayoutData(fdlAddDate);
        wAddDate = new Button(wFileName, SWT.CHECK);
        props.setLook(wAddDate);
        FormData fdAddDate = new FormData();
        fdAddDate.left = new FormAttachment(middle, 0);
        fdAddDate.top = new FormAttachment(wEncoding, margin);
        fdAddDate.right = new FormAttachment(100, 0);
        wAddDate.setLayoutData(fdAddDate);
        wAddDate.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        // Create multi-part file?
        wlAddTime = new Label(wFileName, SWT.RIGHT);
        wlAddTime.setText(BaseMessages.getString(PKG, "JsonOutputDialog.AddTime.Label"));
        props.setLook(wlAddTime);
        FormData fdlAddTime = new FormData();
        fdlAddTime.left = new FormAttachment(0, 0);
        fdlAddTime.top = new FormAttachment(wAddDate, margin);
        fdlAddTime.right = new FormAttachment(middle, -margin);
        wlAddTime.setLayoutData(fdlAddTime);
        wAddTime = new Button(wFileName, SWT.CHECK);
        props.setLook(wAddTime);
        FormData fdAddTime = new FormData();
        fdAddTime.left = new FormAttachment(middle, 0);
        fdAddTime.top = new FormAttachment(wAddDate, margin);
        fdAddTime.right = new FormAttachment(100, 0);
        wAddTime.setLayoutData(fdAddTime);
        wAddTime.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                input.setChanged();
            }
        });

        wbShowFiles = new Button(wFileName, SWT.PUSH | SWT.CENTER);
        props.setLook(wbShowFiles);
        wbShowFiles.setText(BaseMessages.getString(PKG, "JsonOutputDialog.ShowFiles.Button"));
        FormData fdbShowFiles = new FormData();
        fdbShowFiles.left = new FormAttachment(middle, 0);
        fdbShowFiles.top = new FormAttachment(wAddTime, margin * 2);
        wbShowFiles.setLayoutData(fdbShowFiles);
        wbShowFiles.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
                JsonOutputMeta tfoi = new JsonOutputMeta();
                getInfo( tfoi );
                String[] files = tfoi.getFiles( variables );
                if ( files != null && files.length > 0 ) {
                    EnterSelectionDialog esd =
                            new EnterSelectionDialog( shell, files, BaseMessages.getString( PKG,
                                    "JsonOutputDialog.SelectOutputFiles.DialogTitle" ), BaseMessages.getString( PKG,
                                    "JsonOutputDialog.SelectOutputFiles.DialogMessage" ) );
                    esd.setViewOnly();
                    esd.open();
                } else {
                    MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
                    mb.setMessage( BaseMessages.getString( PKG, "JsonOutputDialog.NoFilesFound.DialogMessage" ) );
                    mb.setText( BaseMessages.getString( PKG, "System.DialogTitle.Error" ) );
                    mb.open();
                }
            }
        } );

        // Add File to the result files name
        wlAddToResult = new Label(wFileName, SWT.RIGHT);
        wlAddToResult.setText(BaseMessages.getString(PKG, "JsonOutputDialog.AddFileToResult.Label"));
        props.setLook(wlAddToResult);
        FormData fdlAddToResult = new FormData();
        fdlAddToResult.left = new FormAttachment(0, 0);
        fdlAddToResult.top = new FormAttachment(wbShowFiles, margin);
        fdlAddToResult.right = new FormAttachment(middle, -margin);
        wlAddToResult.setLayoutData(fdlAddToResult);
        wAddToResult = new Button(wFileName, SWT.CHECK);
        wAddToResult.setToolTipText(BaseMessages.getString(PKG, "JsonOutputDialog.AddFileToResult.Tooltip"));
        props.setLook(wAddToResult);
        FormData fdAddToResult = new FormData();
        fdAddToResult.left = new FormAttachment(middle, 0);
        fdAddToResult.top = new FormAttachment( wlAddToResult, 0, SWT.CENTER );
        fdAddToResult.right = new FormAttachment(100, 0);
        wAddToResult.setLayoutData(fdAddToResult);
        SelectionAdapter lsSelR = new SelectionAdapter() {
            public void widgetSelected(SelectionEvent arg0) {
                input.setChanged();
            }
        };
        wAddToResult.addSelectionListener(lsSelR);

        FormData fdFileName = new FormData();
        fdFileName.left = new FormAttachment(0, margin);
        fdFileName.top = new FormAttachment(wSettings, 2 * margin);
        fdFileName.right = new FormAttachment(100, -margin);
        wFileName.setLayoutData(fdFileName);

        // ///////////////////////////////////////////////////////////
        // / END OF FileName GROUP
        // ///////////////////////////////////////////////////////////
    }

    private void createExtensionLine(ModifyListener lsMod, int middle, int margin) {
        // Extension line
        wlExtension = new Label(wFileName, SWT.RIGHT);
        wlExtension.setText(BaseMessages.getString(PKG, "System.Label.Extension"));
        props.setLook(wlExtension);
        FormData fdlExtension = new FormData();
        fdlExtension.left = new FormAttachment(0, 0);
        fdlExtension.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
        fdlExtension.right = new FormAttachment(middle, -margin);
        wlExtension.setLayoutData(fdlExtension);

        wExtension = new TextVar(variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wExtension);
        wExtension.addModifyListener(lsMod);
        FormData fdExtension = new FormData();
        fdExtension.left = new FormAttachment(middle, 0);
        fdExtension.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
        fdExtension.right = new FormAttachment(100, -margin);
        wExtension.setLayoutData(fdExtension);

        wlEncoding = new Label(wFileName, SWT.RIGHT);
        wlEncoding.setText(BaseMessages.getString(PKG, "JsonOutputDialog.Encoding.Label"));
        props.setLook(wlEncoding);
        FormData fdlEncoding = new FormData();
        fdlEncoding.left = new FormAttachment(0, 0);
        fdlEncoding.top = new FormAttachment(wExtension, margin);
        fdlEncoding.right = new FormAttachment(middle, -margin);
        wlEncoding.setLayoutData(fdlEncoding);
        wEncoding = new ComboVar(variables, wFileName, SWT.BORDER | SWT.READ_ONLY);
        wEncoding.setEditable(true);
        props.setLook(wEncoding);
        wEncoding.addModifyListener(lsMod);
        FormData fdEncoding = new FormData();
        fdEncoding.left = new FormAttachment(middle, 0);
        fdEncoding.top = new FormAttachment(wExtension, margin);
        fdEncoding.right = new FormAttachment(100, 0);
        wEncoding.setLayoutData(fdEncoding);
        wEncoding.addFocusListener(new FocusListener() {
            public void focusLost(FocusEvent e) {
            }

            public void focusGained(FocusEvent e) {
                Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                shell.setCursor(busy);
                setEncodings();
                shell.setCursor(null);
                busy.dispose();
            }
        });
    }

    protected void setFieldListComboBoxes() {
        // Something was changed in the row.
        //
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        // Add the currentMeta fields...
        fields.putAll(inputFields);

        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String[] fieldNames = entries.toArray(new String[entries.size()]);

        Const.sortStrings(fieldNames);
        colinf[0].setComboValues(fieldNames);
        keyColInf[0].setComboValues(fieldNames);
    }

    private void setEncodings() {
        // Encoding of the text file:
        if (!gotEncodings) {
            gotEncodings = true;

            wEncoding.removeAll();
            List<Charset> values = new ArrayList<Charset>(Charset.availableCharsets().values());
            for (int i = 0; i < values.size(); i++) {
                Charset charSet = values.get(i);
                wEncoding.add(charSet.displayName());
            }

            // Now select the default!
            String defEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
            int idx = Const.indexOfString(defEncoding, wEncoding.getItems());
            if (idx >= 0) {
                wEncoding.select(idx);
            } else {
                wEncoding.select(Const.indexOfString("UTF-8", wEncoding.getItems()));
            }
        }
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    private void getData() {

        wBlocName.setText(Const.NVL(input.getJsonBloc(), ""));
        wEncoding.setText(Const.NVL(input.getEncoding(), ""));
        wOutputValue.setText(Const.NVL(input.getOutputValue(), ""));
        wUseArrayWithSingleInstance.setSelection(input.isUseArrayWithSingleInstance());
        wJSONPrittified.setSelection(input.isJsonPrittified());
        wSplitOutputAfter.setText(Integer.toString(input.getSplitOutputAfter()));
        wOperation.setText(JsonOutputMeta.getOperationTypeDesc(input.getOperationType()));
        wFilename.setText(Const.NVL(input.getFileName(), ""));
        wCreateParentFolder.setSelection(input.isCreateParentFolder());
        wExtension.setText(Const.NVL(input.getExtension(), "js"));

        wAddDate.setSelection(input.isDateInFilename());
        wAddTime.setSelection(input.isTimeInFilename());
        wAppend.setSelection(input.isFileAppended());

        wEncoding.setText(Const.NVL(input.getEncoding(), ""));
        wAddToResult.setSelection(input.AddToResult());
        wDoNotOpenNewFileInit.setSelection(input.isDoNotOpenNewFileInit());

        wJSONSizeFieldname.setText(Const.NVL(input.getJsonSizeFieldname(), ""));

        if (isDebug()) {
            logDebug(BaseMessages.getString(PKG, "JsonOutputDialog.Log.GettingFieldsInfo"));
        }

        for (int i = 0; i < input.getKeyFields().length; i++) {
            JsonOutputKeyField field = input.getKeyFields()[i];

            TableItem item = wKeyFields.table.getItem(i);
            item.setText(1, Const.NVL(field.getFieldName(), ""));
            item.setText(2, Const.NVL(field.getElementName(), ""));

        }

        for (int i = 0; i < input.getOutputFields().length; i++) {
            JsonOutputField field = input.getOutputFields()[i];

            TableItem item = wFields.table.getItem(i);
            item.setText(1, Const.NVL(field.getFieldName(), ""));
            item.setText(2, Const.NVL(field.getElementName(), ""));
            String jsonFragment =
                    field.isJSONFragment() ? BaseMessages.getString(PKG, "System.Combo.Yes") : BaseMessages.getString(
                            PKG, "System.Combo.No");
            if (jsonFragment != null) {
                item.setText(3, jsonFragment);
            }
            String removeIfBlank =
                    field.isRemoveIfBlank() ? BaseMessages.getString(PKG, "System.Combo.Yes") : BaseMessages.getString(
                            PKG, "System.Combo.No");
            if (removeIfBlank != null) {
                item.setText(4, removeIfBlank);
            }

        }

        wFields.optWidth(true);
        wTransformName.selectAll();
        wTransformName.setFocus();
    }

    private void cancel() {
        transformName = null;

        input.setChanged(backupChanged);

        dispose();
    }

    private void getInfo(JsonOutputMeta jsometa) {

        jsometa.setJsonBloc(wBlocName.getText());
        jsometa.setEncoding(wEncoding.getText());
        jsometa.setOutputValue(wOutputValue.getText());
        jsometa.setUseArrayWithSingleInstance(wUseArrayWithSingleInstance.getSelection());
        jsometa.setOperationType(JsonOutputMeta.getOperationTypeByDesc(wOperation.getText()));
        jsometa.setJsonPrittified(wJSONPrittified.getSelection());
        jsometa.setSplitOutputAfter(Integer.parseInt(wSplitOutputAfter.getText().length() == 0 ? "0" : wSplitOutputAfter.getText()));
        jsometa.setCreateParentFolder(wCreateParentFolder.getSelection());
        jsometa.setFileName(wFilename.getText());
        jsometa.setExtension(wExtension.getText());
        jsometa.setFileAppended(wAppend.getSelection());
        jsometa.setDateInFilename(wAddDate.getSelection());
        jsometa.setTimeInFilename(wAddTime.getSelection());

        jsometa.setEncoding(wEncoding.getText());
        jsometa.setAddToResult(wAddToResult.getSelection());
        jsometa.setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection());

        jsometa.setJsonSizeFieldname(wJSONSizeFieldname.getText());

        int nrKeyFields = wKeyFields.nrNonEmpty();

        jsometa.allocateKey(nrKeyFields);

        for (int i = 0; i < nrKeyFields; i++) {
            JsonOutputKeyField field = new JsonOutputKeyField();

            TableItem item = wKeyFields.getNonEmpty(i);
            field.setFieldName(item.getText(1));
            field.setElementName(item.getText(2));
            jsometa.getKeyFields()[i] = field;
        }

        int nrfields = wFields.nrNonEmpty();

        jsometa.allocate(nrfields);

        for (int i = 0; i < nrfields; i++) {
            JsonOutputField field = new JsonOutputField();

            TableItem item = wFields.getNonEmpty(i);
            field.setFieldName(item.getText(1));
            field.setElementName(item.getText(2));
            field.setJSONFragment(BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(3)));
            field.setRemoveIfBlank(BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(4)));
            // CHECKSTYLE:Indentation:OFF
            jsometa.getOutputFields()[i] = field;
        }
    }

    private void ok() {
        if (Utils.isEmpty(wTransformName.getText())) {
            return;
        }

        transformName = wTransformName.getText(); // return value

        getInfo(input);

        if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
            MessageDialogWithToggle md = new MessageDialogWithToggle( shell,
                    BaseMessages.getString( PKG, "JsonOutputDialog.InputNeedSort.DialogTitle" ),
                    BaseMessages.getString( PKG, "JsonOutputDialog.InputNeedSort.DialogMessage", Const.CR ) + Const.CR,
                    SWT.ICON_WARNING,
                    new String[] { BaseMessages.getString( PKG, "JsonOutputDialog.InputNeedSort.Option1" ) },
                    BaseMessages.getString( PKG, "JsonOutputDialog.InputNeedSort.Option2" ),
                    "N".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
            md.open();
            props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
        }

        dispose();
    }

    private void get() {
        if (gotPreviousFields) {
            return;
        }
        try {
            IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
            if ( r != null ) {
                BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] { 3 }, 5, 6,
                        ( tableItem, v ) -> {
                            if ( v.isNumber() ) {
                                if ( v.getLength() > 0 ) {
                                    int le = v.getLength();
                                    int pr = v.getPrecision();

                                    if ( v.getPrecision() <= 0 ) {
                                        pr = 0;
                                    }

                                    String mask = " ";
                                    for ( int m = 0; m < le - pr; m++ ) {
                                        mask += "0";
                                    }
                                    if ( pr > 0 ) {
                                        mask += ".";
                                    }
                                    for ( int m = 0; m < pr; m++ ) {
                                        mask += "0";
                                    }
                                    tableItem.setText( 4, mask );
                                }
                            }
                            return true;
                        } );
            }
        } catch (HopException ke) {
            new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages
                    .getString(PKG, "System.Dialog.GetFieldsFailed.Message"), ke);
        }
    }

    private void updateOperation() {

        int opType = JsonOutputMeta.getOperationTypeByDesc(wOperation.getText());
        boolean activeFile = opType != JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE;

        wlFilename.setEnabled(activeFile);
        wFilename.setEnabled(activeFile);
        wbFilename.setEnabled(activeFile);
        wlExtension.setEnabled(activeFile);
        wExtension.setEnabled(activeFile);
        wlEncoding.setEnabled(activeFile);
        wEncoding.setEnabled(activeFile);
        wlAppend.setEnabled(activeFile);
        wAppend.setEnabled(activeFile);
        wlSplitOutputAfter.setEnabled(activeFile);
        wSplitOutputAfter.setEnabled(activeFile);
        wlCreateParentFolder.setEnabled(activeFile);
        wCreateParentFolder.setEnabled(activeFile);
        wlDoNotOpenNewFileInit.setEnabled(activeFile);
        wDoNotOpenNewFileInit.setEnabled(activeFile);
        wlAddDate.setEnabled(activeFile);
        wAddDate.setEnabled(activeFile);
        wlAddTime.setEnabled(activeFile);
        wAddTime.setEnabled(activeFile);
        wlAddToResult.setEnabled(activeFile);
        wAddToResult.setEnabled(activeFile);
        wbShowFiles.setEnabled(activeFile);
    }
}
