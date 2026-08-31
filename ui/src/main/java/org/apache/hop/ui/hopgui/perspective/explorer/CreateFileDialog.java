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

package org.apache.hop.ui.hopgui.perspective.explorer;

import java.util.List;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Asks for a file name and a file type, and reports the full path of the file to create. */
public class CreateFileDialog extends Dialog {
  private static final Class<?> PKG = ExplorerPerspective.class;

  private final String folderPath;
  private final List<IHopFileType> fileTypes;
  private final PropsUi props;

  private Shell shell;
  private Text wName;
  private Combo wType;
  private Label wPreview;
  private Button wOk;

  private String filePath;
  private IHopFileType selectedFileType;

  public CreateFileDialog(Shell parent, String folderPath, List<IHopFileType> fileTypes) {
    super(parent, SWT.NONE);
    this.folderPath = folderPath;
    this.fileTypes = fileTypes;
    this.props = PropsUi.getInstance();
  }

  /** Opens the dialog. Returns the full path of the file to create, or null when cancelled. */
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "ExplorerPerspective.CreateFile.Header"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    Label wlName = new Label(shell, SWT.NONE);
    wlName.setText(BaseMessages.getString(PKG, "ExplorerPerspective.CreateFile.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);

    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(0, 0);
    fdName.top = new FormAttachment(wlName, margin);
    fdName.right = new FormAttachment(100, -margin);
    wName.setLayoutData(fdName);
    wName.addModifyListener(e -> updateState());

    Label wlType = new Label(shell, SWT.NONE);
    wlType.setText(BaseMessages.getString(PKG, "ExplorerPerspective.CreateFile.Type.Label"));
    PropsUi.setLook(wlType);
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment(0, 0);
    fdlType.top = new FormAttachment(wName, margin);
    wlType.setLayoutData(fdlType);

    wType = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wType);
    for (IHopFileType fileType : fileTypes) {
      wType.add(fileType.getName());
    }
    if (!fileTypes.isEmpty()) {
      wType.select(0);
    }
    FormData fdType = new FormData();
    fdType.left = new FormAttachment(0, 0);
    fdType.top = new FormAttachment(wlType, margin);
    fdType.right = new FormAttachment(100, -margin);
    wType.setLayoutData(fdType);
    wType.addListener(SWT.Selection, e -> updateState());

    Label wlPreview = new Label(shell, SWT.NONE);
    wlPreview.setText(BaseMessages.getString(PKG, "ExplorerPerspective.CreateFile.Preview.Label"));
    PropsUi.setLook(wlPreview);
    FormData fdlPreview = new FormData();
    fdlPreview.left = new FormAttachment(0, 0);
    fdlPreview.top = new FormAttachment(wType, margin);
    wlPreview.setLayoutData(fdlPreview);

    wPreview = new Label(shell, SWT.NONE);
    PropsUi.setLook(wPreview);
    FormData fdPreview = new FormData();
    fdPreview.left = new FormAttachment(0, 0);
    fdPreview.top = new FormAttachment(wlPreview, margin);
    fdPreview.right = new FormAttachment(100, -margin);
    wPreview.setLayoutData(fdPreview);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, wPreview);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    updateState();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return filePath;
  }

  /** The file type picked in the combo. Only meaningful when {@link #open()} returned a path. */
  public IHopFileType getSelectedFileType() {
    return selectedFileType;
  }

  private IHopFileType currentFileType() {
    int index = wType.getSelectionIndex();
    if (index < 0 || index >= fileTypes.size()) {
      return null;
    }
    return fileTypes.get(index);
  }

  private String currentPath() {
    IHopFileType fileType = currentFileType();
    if (fileType == null || !ExplorerCreateUtils.isSimpleFileName(wName.getText())) {
      return null;
    }
    String fileName =
        ExplorerCreateUtils.applyExtension(wName.getText(), fileType.getDefaultFileExtension());
    String path = ExplorerCreateUtils.childPath(folderPath, fileName);
    if (!ExplorerCreateUtils.resolvesInsideFolder(folderPath, path)) {
      return null;
    }
    return path;
  }

  private void updateState() {
    String path = currentPath();
    wPreview.setText(path == null ? "" : path);
    wOk.setEnabled(path != null);
  }

  private void ok() {
    String path = currentPath();
    if (path == null) {
      return;
    }
    filePath = path;
    selectedFileType = currentFileType();
    dispose();
  }

  private void cancel() {
    filePath = null;
    selectedFileType = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
