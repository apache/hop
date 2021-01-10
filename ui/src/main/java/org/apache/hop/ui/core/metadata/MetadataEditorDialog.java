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

package org.apache.hop.ui.core.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;


public class MetadataEditorDialog extends Dialog implements IMetadataDialog {
	private static final Class<?> PKG = MetadataEditorDialog.class; // For Translator

	private Shell shell;
	private MetadataEditor<?> editor;
	private String result;

	
	public MetadataEditorDialog(Shell parent, MetadataEditor<?> editor) {
		super(parent);
		this.editor = editor;
	}
	
	public String open() {
		PropsUi props = PropsUi.getInstance();
		
		Shell parent = getParent();
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		shell.setText(editor.getTitle());
		shell.setImage(editor.getTitleImage());
		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN*2;
		formLayout.marginHeight = Const.FORM_MARGIN*2;
		shell.setLayout(formLayout);
		props.setLook(shell);
		
		// Create buttons
		Button wOk = new Button(shell, SWT.PUSH);
		wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wOk.addListener(SWT.Selection, e -> onOk());

		Button wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
		wCancel.addListener(SWT.Selection, e -> onCancel());

		List<Button> buttons = new ArrayList<>();
		buttons.add(wOk);
		Button[] extras = editor.createButtonsForButtonBar(shell);
		if ( extras!=null ) {
			for(Button button:extras) {
				buttons.add(button);	
			}
		}
		buttons.add(wCancel);
		BaseTransformDialog.positionBottomButtons(shell, buttons.toArray(new Button[0]), props.getMargin(), null);

		// Create editor content area
		Composite area = new Composite(shell, SWT.NONE);
		FormLayout layout = new FormLayout();
		layout.marginWidth = 0;
		layout.marginHeight = 0;
		area.setLayout(layout);
		FormData fdArea = new FormData();
		fdArea.left = new FormAttachment(0, 0);
		fdArea.top = new FormAttachment(0, 0);
		fdArea.right = new FormAttachment(100, 0);
		fdArea.bottom = new FormAttachment(wOk, -props.getMargin());
		area.setLayoutData(fdArea);
		props.setLook(area);

		// Create editor controls
		editor.createControl(area);
	
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				onCancel();
			}
		});

		// Restore windows size
		BaseTransformDialog.setSize(shell);

		shell.open();
		Display display = parent.getDisplay();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return result;
	}
	
	protected void onCancel() {
		dispose();
	}

	protected void onOk() {
		try {
		    // Save it in the metadata		     
			editor.save();
			
			result = editor.getMetadata().getName();
			
			dispose();
		} catch (HopException e) {
			new ErrorDialog(getParent(), "Error", "Error saving metadata", e);
		} 
	}

	public void dispose() {		
		PropsUi.getInstance().setScreen(new WindowProperty(shell));		
		editor.dispose();
		shell.dispose();
	}
}
