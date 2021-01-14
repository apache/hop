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

package org.apache.hop.pipeline.transforms.streamschemamerge;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;


public class StreamSchemaDialog extends BaseTransformDialog implements ITransformDialog {

	private static final Class<?> PKG = StreamSchemaMeta.class; // For Translator

	// this is the object the stores the transform's settings
	// the dialog reads the settings from it when opening
	// the dialog writes the settings to it when confirmed 
	private StreamSchemaMeta meta;

	private String[] previousTransforms;  // transforms sending data in to this transform

	// text field holding the name of the field to add to the row stream
	private Label wlTransforms;
	private TableView wTransforms;
	private FormData fdlTransforms, fdTransforms;

	public StreamSchemaDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String transformName) {
		super(parent, variables, (BaseTransformMeta)in, pipelineMeta, transformName );
		meta = (StreamSchemaMeta)in;
	}

	/**
	 * The constructor should simply invoke super() and save the incoming meta
	 * object to a local variable, so it can conveniently read and write settings
	 * from/to it.
	 * 
	 * or null if the user cancelled the dialog.
	 */
	public String open() {

		// store some convenient SWT variables 
		Shell parent = getParent();
		Display display = parent.getDisplay();

		// SWT code for preparing the dialog
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, meta);
		
		// Save the value of the changed flag on the meta object. If the user cancels
		// the dialog, it will be restored to this saved value.
		// The "changed" variable is inherited from BaseTransformDialog
		changed = meta.hasChanged();

		// The ModifyListener used on all controls. It will update the meta object to 
		// indicate that changes are being made.
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				meta.setChanged();
			}
		};
		
		// ------------------------------------------------------- //
		// SWT code for building the actual settings dialog        //
		// ------------------------------------------------------- //
		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "StreamSchemaTransform.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// TransformName line
		wlTransformName = new Label(shell, SWT.RIGHT);
		wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
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

        // OK, get and cancel buttons
        wOk = new Button( shell, SWT.PUSH );
        wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wGet = new Button( shell, SWT.PUSH );
        wGet.setText( BaseMessages.getString( PKG, "StreamSchema.getPreviousTransforms.Label") );
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOk, wGet, wCancel}, margin, null);

		// Table with fields for inputting transform names
		wlTransforms = new Label( shell, SWT.NONE );
		wlTransforms.setText(BaseMessages.getString(PKG, "StreamSchemaTransformDialog.Transforms.Label"));
		props.setLook(wlTransforms);
		fdlTransforms = new FormData();
		fdlTransforms.left = new FormAttachment( 0, 0 );
		fdlTransforms.top = new FormAttachment( wTransformName, margin );
		wlTransforms.setLayoutData(fdlTransforms);

		final int FieldsCols = 1;
        final int FieldsRows = meta.getNumberOfTransforms();

        previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

		ColumnInfo[] colinf = new ColumnInfo[FieldsCols];
		colinf[0] =
				new ColumnInfo(
						BaseMessages.getString( PKG, "StreamSchemaTransformDialog.TransformName.Column"),
						ColumnInfo.COLUMN_TYPE_CCOMBO, previousTransforms, false );

		wTransforms =
				new TableView(
						variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

		fdTransforms = new FormData();
		fdTransforms.left = new FormAttachment( 0, 0 );
		fdTransforms.top = new FormAttachment(wlTransforms, margin );
		fdTransforms.right = new FormAttachment( 100, 0 );
		fdTransforms.bottom = new FormAttachment( wOk, -2 * margin );
		wTransforms.setLayoutData(fdTransforms);

		// Add listeners for cancel and OK
		lsCancel = new Listener() {
			public void handleEvent(Event e) {cancel();}
		};
		lsOk = new Listener() {
			public void handleEvent(Event e) {ok();}
		};
        lsGet = new Listener() {
            public void handleEvent( Event e ) {
                get();
            }
        };

		wCancel.addListener(SWT.Selection, lsCancel);
		wOk.addListener(SWT.Selection, lsOk);
        wGet.addListener( SWT.Selection, lsGet );

		// default listener (for hitting "enter")
		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {ok();}
		};
		wTransformName.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {cancel();}
		});
		
		// Set/Restore the dialog size based on last position on screen
		// The setSize() method is inherited from BaseTransformDialog
		setSize();

		// populate the dialog with the values from the meta object
		populateDialog();
		
		// restore the changed flag to original value, as the modify listeners fire during dialog population 
		meta.setChanged(changed);

		// open dialog and enter event loop 
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}

		// at this point the dialog has closed, so either ok() or cancel() have been executed
		// The "TransformName" variable is inherited from BaseTransformDialog
		return transformName;
	}
	
	/**
	 * This helper method puts the transform configuration stored in the meta object
	 * and puts it into the dialog controls.
	 */
    private void populateDialog() {
        Table table = wTransforms.table;
        if ( meta.getNumberOfTransforms() > 0 ) {
            table.removeAll();
        }
        String[] TransformNames = meta.getTransformsToMerge();
        for ( int i = 0; i < TransformNames.length; i++ ) {
            TableItem ti = new TableItem( table, SWT.NONE );
            ti.setText( 0, "" + ( i + 1 ) );
            if ( TransformNames[i] != null ) {
                ti.setText( 1, TransformNames[i] );
            }
        }

        wTransforms.removeEmptyRows();
        wTransforms.setRowNums();
        wTransforms.optWidth(true);

        wTransformName.selectAll();
        wTransformName.setFocus();
	}

    /**
     * Populates the table with a list of fields that have incoming hops
     */
    private void get() {
        wTransforms.removeAll();
        Table table = wTransforms.table;

        for ( int i = 0; i < previousTransforms.length; i++ ) {
            TableItem ti = new TableItem( table, SWT.NONE );
            ti.setText( 0, "" + ( i + 1 ) );
            ti.setText( 1, previousTransforms[i] );
        }
        wTransforms.removeEmptyRows();
        wTransforms.setRowNums();
        wTransforms.optWidth(true);

    }

	/**
	 * Called when the user cancels the dialog.  
	 */
	private void cancel() {
		// The "TransformName" variable will be the return value for the open() method.
		// Setting to null to indicate that dialog was cancelled.
		transformName = null;
		// Restoring original "changed" flag on the met aobject
		meta.setChanged(changed);
		// close the SWT dialog window
		dispose();
	}

    /**
     * Helping method to update meta information when ok is selected
     * @param inputTransforms Names of the transforms that are being merged together
     */
    private void getMeta(String[] inputTransforms) {
        List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

        if ( infoStreams.size() == 0 || inputTransforms.length < infoStreams.size()) {
            if ( inputTransforms.length != 0 ) {
//                meta.wipeTransformIoMeta();
                for (String inputTransform : inputTransforms) {
                    meta.getTransformIOMeta().addStream(
                            new Stream(IStream.StreamType.INFO, null, "", StreamIcon.INFO, null));
                }
                infoStreams = meta.getTransformIOMeta().getInfoStreams();
            }
        } else if ( infoStreams.size() < inputTransforms.length ) {
            int requiredStreams = inputTransforms.length - infoStreams.size();

            for ( int i = 0; i < requiredStreams; i++ ) {
                meta.getTransformIOMeta().addStream(
                        new Stream( IStream.StreamType.INFO, null, "", StreamIcon.INFO, null ) );
            }
            infoStreams = meta.getTransformIOMeta().getInfoStreams();
        }
        int streamCount = infoStreams.size();

        String[] transformsToMerge = meta.getTransformsToMerge();
        for ( int i = 0; i < streamCount; i++ ) {
            String transform = transformsToMerge[i];
            IStream infoStream = infoStreams.get( i );
            infoStream.setTransformMeta( pipelineMeta.findTransform( transform ) );
            infoStream.setSubject(transform);
        }
    }
	
	/**
	 * Called when the user confirms the dialog
	 */
	private void ok() {
		// The "TransformName" variable will be the return value for the open() method.
		// Setting to transform name from the dialog control
		transformName = wTransformName.getText();
		// set output field name

        // TODO eliminate copying here and copying when placed in meta
        int nrtransforms = wTransforms.nrNonEmpty();
        String[] TransformNames = new String[nrtransforms];
        for ( int i = 0; i < nrtransforms; i++ ) {
            TableItem ti = wTransforms.getNonEmpty(i);
            TransformMeta tm = pipelineMeta.findTransform(ti.getText(1));
            if (tm != null) {
                TransformNames[i] = tm.getName();
            }
        }
        meta.setTransformsToMerge(TransformNames);
		getMeta(TransformNames);

		// close the SWT dialog window
		dispose();
	}
}
