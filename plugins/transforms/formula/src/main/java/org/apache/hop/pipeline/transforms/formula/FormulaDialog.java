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

package org.apache.hop.pipeline.transforms.formula;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.formula.editor.FormulaEditor;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.*;
import java.util.List;

public class FormulaDialog extends BaseTransformDialog implements ITransformDialog {
    private static final Class<?> PKG = FormulaDialog.class; // For Translator

    private Label wlFields;
    private TableView wFields;
    private FormData fdlFields, fdFields;

    private FormulaMeta currentMeta;
    private FormulaMeta originalMeta;

    private Map<String, Integer> inputFields;
    private ColumnInfo[] colinf;

    private String[] fieldNames;

    public FormulaDialog(Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
        super(parent, variables, (BaseTransformMeta) in, tr, sname);

        // The order here is important... currentMeta is looked at for changes
        currentMeta = (FormulaMeta) baseTransformMeta;
        originalMeta = (FormulaMeta) currentMeta.clone();
        inputFields = new HashMap<String, Integer>();
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, currentMeta);

        ModifyListener lsMod = e -> currentMeta.setChanged();

        changed = currentMeta.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "FormulaDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = props.getMargin();

        // TransformName line
        //
        wlTransformName = new Label(shell, SWT.RIGHT);
        wlTransformName.setText(BaseMessages.getString(PKG, "FormulaDialog.TransformName.Label"));
        props.setLook(wlTransformName);
        fdlTransformName = new FormData();
        fdlTransformName.left = new FormAttachment(0, 0);
        fdlTransformName.right = new FormAttachment(middle, -margin);
        fdlTransformName.top = new FormAttachment(0, margin);
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

        wlFields = new Label( shell, SWT.NONE );
        wlFields.setText( BaseMessages.getString( PKG, "FormulaDialog.Fields.Label" ) );
        props.setLook( wlFields );
        fdlFields = new FormData();
        fdlFields.left = new FormAttachment( 0, 0 );
        fdlFields.top = new FormAttachment( wTransformName, margin );
        wlFields.setLayoutData( fdlFields );

        final int FieldsRows = currentMeta.getFormulas() != null ? currentMeta.getFormulas().size() : 1;

        colinf =
                new ColumnInfo[]{
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "FormulaDialog.NewField.Column"), ColumnInfo.COLUMN_TYPE_TEXT, false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "FormulaDialog.Formula.Column"), ColumnInfo.COLUMN_TYPE_TEXT, false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "FormulaDialog.ValueType.Column"), ColumnInfo.COLUMN_TYPE_CCOMBO,
                                ValueMetaFactory.getValueMetaNames()),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "FormulaDialog.Length.Column"), ColumnInfo.COLUMN_TYPE_TEXT, false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "FormulaDialog.Precision.Column"), ColumnInfo.COLUMN_TYPE_TEXT,
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "FormulaDialog.Replace.Column"), ColumnInfo.COLUMN_TYPE_CCOMBO,
                                new String[]{}),};

        wFields = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

        fdFields = new FormData();
        fdFields.left = new FormAttachment( 0, 0 );
        fdFields.top = new FormAttachment( wlFields, margin );
        fdFields.right = new FormAttachment( 100, 0 );
        fdFields.bottom = new FormAttachment( 100, -50 );
        wFields.setLayoutData(fdFields);

        new Thread(() -> {
            TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
            if (transformMeta != null) {
                try {
                    IRowMeta row = pipelineMeta.getPrevTransformFields(variables,transformMeta);

                    // Remember these fields...
                    for (int i = 0; i < row.size(); i++) {
                        inputFields.put(row.getValueMeta(i).getName(), new Integer(i));
                    }

                    setComboBoxes();
                } catch (HopTransformException e) {
                    logError(BaseMessages.getString(PKG, "FormulaDialog.Log.UnableToFindInput"));
                }
            }
        }).start();

        colinf[1].setSelectionAdapter( new SelectionAdapter() {
            @Override
            public void widgetSelected( SelectionEvent e ) {
                if ( fieldNames == null ) {
                    return;
                }

                TableView tv = (TableView) e.widget;
                TableItem item = tv.table.getItem( e.y );
                String formula = item.getText( e.x );

                try {
                    if ( !shell.isDisposed() ) {
                        FormulaEditor libFormulaEditor =
                                new FormulaEditor( shell, SWT.APPLICATION_MODAL | SWT.SHEET, Const.NVL( formula, "" ), fieldNames );
                        formula = libFormulaEditor.open();
                        if ( formula != null && !tv.isDisposed() ) {
                            tv.setText( formula, e.x, e.y );
                        }
                    }
                } catch ( Exception ex ) {
                    new ErrorDialog( shell, "Error", "There was an unexpected error in the formula editor", ex );
                }

            }
        } );

        wFields.addModifyListener( new ModifyListener() {
            @Override
            public void modifyText( ModifyEvent arg0 ) {
                // Now set the combo's
                shell.getDisplay().asyncExec( new Runnable() {
                    @Override
                    public void run() {
                        setComboBoxes();
                    }

                } );

            }
        } );

        // Some buttons
        wOk = new Button( shell, SWT.PUSH );
        wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

        setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

        wCancel.addListener( SWT.Selection, e -> cancel() );
        wOk.addListener( SWT.Selection, e -> ok() );


        wTransformName.addSelectionListener( new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                ok();
            }
        } );

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener( new ShellAdapter() {
            @Override
            public void shellClosed( ShellEvent e ) {
                cancel();
            }
        } );

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        currentMeta.setChanged( changed );

        shell.open();
        while ( !shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }
        return transformName;
    }

    protected void setComboBoxes() {
        // Something was changed in the row.
        //
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        // Add the currentMeta fields...
        fields.putAll( inputFields );

        shell.getDisplay().syncExec( () -> {
                // Add the newly create fields.
                //
                /*
                 * int nrNonEmptyFields = wFields.nrNonEmpty(); for (int i=0;i<nrNonEmptyFields;i++) { TableItem item =
                 * wFields.getNonEmpty(i); fields.put(item.getText(1), new Integer(1000000+i)); // The number is just to debug
                 * the origin of the fieldname }
                 */

                Set<String> keySet = fields.keySet();
                List<String> entries = new ArrayList<String>( keySet );

                String[] fieldNames = entries.toArray( new String[entries.size()] );

                Const.sortStrings( fieldNames );

                colinf[5].setComboValues( fieldNames );
                FormulaDialog.this.fieldNames = fieldNames;
            }
         );

    }
    /**
     * Copy information from the meta-data currentMeta to the dialog fields.
     */
    public void getData() {

        if ( currentMeta.getFormulas() != null ) {
            for (int i = 0; i < currentMeta.getFormulas().size(); i++ ) {
                FormulaMetaFunction fn = currentMeta.getFormulas().get(i);
                TableItem item = wFields.table.getItem( i );
                item.setText( 1, Const.NVL( fn.getFieldName(), "" ) );
                item.setText( 2, Const.NVL( fn.getFormula(), "" ) );
                item.setText( 3, Const.NVL( ValueMetaFactory.getValueMetaName( fn.getValueType() ), "" ) );
                if ( fn.getValueLength() >= 0 ) {
                    item.setText( 4, "" + fn.getValueLength() );
                }
                if ( fn.getValuePrecision() >= 0 ) {
                    item.setText( 5, "" + fn.getValuePrecision() );
                }
                item.setText( 6, Const.NVL( fn.getReplaceField(), "" ) );
            }
        }

        wFields.setRowNums();
        wFields.optWidth( true );

        wTransformName.selectAll();
        wTransformName.setFocus();
    }

    private void cancel() {
        transformName = null;
        currentMeta.setChanged( changed );
        dispose();
    }

    private void ok() {
        if ( Utils.isEmpty( wTransformName.getText() ) ) {
            return;
        }

        transformName = wTransformName.getText(); // return value

        currentMeta.getFormulas().clear();

        int nrNonEmptyFields = wFields.nrNonEmpty();
        for ( int i = 0; i < nrNonEmptyFields; i++ ) {
            TableItem item = wFields.getNonEmpty( i );

            String fieldName = item.getText( 1 );
            String formulaString = item.getText( 2 );
            int valueType = ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) );
            int valueLength = Const.toInt( item.getText( 4 ), -1 );
            int valuePrecision = Const.toInt( item.getText( 5 ), -1 );
            String replaceField = item.getText( 6 );

            //CHECKSTYLE:Indentation:OFF
            currentMeta.getFormulas().add(new FormulaMetaFunction( fieldName, formulaString, valueType,
                    valueLength, valuePrecision, replaceField ));
        }

        if ( !originalMeta.equals(currentMeta) ) {
            currentMeta.setChanged();
            changed = currentMeta.hasChanged();
        }

        dispose();
    }
}
