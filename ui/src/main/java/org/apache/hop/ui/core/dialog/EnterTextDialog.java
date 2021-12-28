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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.IDescription;
import org.apache.hop.core.Props;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/** Dialog to enter a text. (descriptions etc.) */
public class EnterTextDialog extends Dialog {
  private static final Class<?> PKG = EnterTextDialog.class; // For Translator

  private final String title;
  private final String message;

  private Label wlDesc;
  private Text wDesc;
  private Button wOk;
  private final Shell parent;
  private Shell shell;
  private final PropsUi props;
  private String text;
  private boolean fixed;
  private boolean readonly;
  private boolean modal;
  private boolean singleLine;
  private String origText;

  /**
   * Dialog to allow someone to show or enter a text
   *
   * @param parent The parent shell to use
   * @param title The dialog title
   * @param message The message to display
   * @param text The text to display or edit
   * @param fixed true if you want the font to be in fixed-width
   */
  public EnterTextDialog(Shell parent, String title, String message, String text, boolean fixed) {
    this(parent, title, message, text);
    this.fixed = fixed;
  }

  /**
   * Dialog to allow someone to show or enter a text in variable width font
   *
   * @param parent The parent shell to use
   * @param title The dialog title
   * @param message The message to display
   * @param text The text to display or edit
   */
  public EnterTextDialog(Shell parent, String title, String message, String text) {
    super(parent, SWT.NONE);
    this.parent = parent;
    props = PropsUi.getInstance();
    this.title = title;
    this.message = message;
    this.text = text;
    fixed = false;
    readonly = false;
    singleLine = false;
  }

  public void setReadOnly() {
    readonly = true;
  }

  public void setModal() {
    modal = true;
  }

  public void setSingleLine() {
    singleLine = true;
  }

  public String open() {
    modal |=
        Const.isLinux(); // On Linux, this dialog seems to behave strangely except when shown modal

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM
                | SWT.RESIZE
                | SWT.MAX
                | SWT.MIN
                | (modal ? SWT.APPLICATION_MODAL | SWT.SHEET : SWT.NONE));
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(title);

    int margin = props.getMargin();

    // Some buttons at the bottom
    if (!readonly) {
      wOk = new Button(shell, SWT.PUSH);
      wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
      wOk.addListener(SWT.Selection, e -> ok());
      Button wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      wCancel.addListener(SWT.Selection, e -> cancel());
      BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);
    } else {
      wOk = new Button(shell, SWT.PUSH);
      wOk.setText(BaseMessages.getString(PKG, "System.Button.Close"));
      wOk.addListener(SWT.Selection, e -> ok());
      BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk}, margin, null);
    }

    // From transform line
    wlDesc = new Label(shell, SWT.NONE);
    wlDesc.setText(message);
    props.setLook(wlDesc);
    FormData fdlDesc = new FormData();
    fdlDesc.left = new FormAttachment(0, 0);
    fdlDesc.top = new FormAttachment(0, margin);
    wlDesc.setLayoutData(fdlDesc);

    if (singleLine) {
      wDesc = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    } else {
      wDesc = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    }

    wDesc.setText("");
    if (fixed) {
      props.setLook(wDesc, Props.WIDGET_STYLE_FIXED);
    } else {
      props.setLook(wDesc);
    }
    FormData fdDesc = new FormData();
    fdDesc.left = new FormAttachment(0, 0);
    fdDesc.top = new FormAttachment(wlDesc, margin);
    fdDesc.right = new FormAttachment(100, 0);
    fdDesc.bottom = new FormAttachment(wOk, -2 * margin);
    wDesc.setLayoutData(fdDesc);
    wDesc.setEditable(!readonly);
    wDesc.addListener(SWT.DefaultSelection, e -> ok());
    wDesc.addListener(
        SWT.Modify,
        e -> {
          Text source = (Text) e.widget;
          this.text = source.getText();
        });

    enrich(this);

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            checkCancel(e);
          }
        });

    origText = text;
    getData();

    // Set the size as well...
    //
    BaseTransformDialog.setSize(shell);

    // Open the shell
    //
    shell.open();

    // Handle the event loop until we're done with this shell...
    //
    Display display = shell.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return text;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    if (text != null) {
      wDesc.setText(text);
    }

    if (readonly) {
      wOk.setFocus();
    } else {
      wDesc.setFocus();
    }
  }

  public void checkCancel(ShellEvent e) {
    String newText = wDesc.getText();
    if (!newText.equals(origText)) {
      int save = HopGuiWorkflowGraph.showChangedWarning(shell, title);
      if (save == SWT.CANCEL) {
        e.doit = false;
      } else if (save == SWT.YES) {
        ok();
      } else {
        cancel();
      }
    } else {
      cancel();
    }
  }

  private void cancel() {
    text = null;
    dispose();
  }

  private void ok() {
    text = wDesc.getText();
    dispose();
  }

  public static final void editDescription(
      Shell shell, IDescription iDescription, String shellText, String message) {
    EnterTextDialog textDialog =
        new EnterTextDialog(shell, shellText, message, iDescription.getDescription());
    String description = textDialog.open();
    if (description != null) {
      iDescription.setDescription(description);
    }
  }

  public boolean isFixed() {
    return fixed;
  }

  public void setFixed(boolean fixed) {
    this.fixed = fixed;
  }

  // An enrich method is provided for enrich the shell. By default, it does nothing.
  public void enrich(EnterTextDialog enterTextDialog) {}

  public Label getWlDesc() {
    return wlDesc;
  }

  public Text getwDesc() {
    return wDesc;
  }

  public Shell getShell() {
    return shell;
  }

  @Override
  public String getText() {
    return text;
  }

  public void setWOkListener(Listener listener) {
    wOk.addListener(SWT.Selection, listener);
  }
}
