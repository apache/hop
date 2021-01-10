/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.model.repository;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.HopGitPerspective;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metadata.IMetadataDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class GitRepositoryDialog implements IMetadataDialog {

  private static final Class<?> PKG = GitRepositoryDialog.class; // For Translator

  private final Shell parentShell;
  private Text nameText;
  private Text descText;
  private TextVar directoryText;

  protected PropsUi props;
  protected GitRepository repo;
  private final IHopMetadataProvider metadataProvider;
  protected IVariables variables;
  protected static String APPLICATION_NAME;

  protected String returnValue;
  protected Shell shell;

  /**
   * A dialog to edit repository settings.
   *
   * @param parentShell
   * @param repo a git repository to edit. Can be null to create a new one.
   */
  public GitRepositoryDialog(
      Shell parentShell,
      IHopMetadataProvider metadataProvider,
      GitRepository repo,
      IVariables variables) {
    this.parentShell = parentShell;
    this.repo = repo;
    this.metadataProvider = metadataProvider;
    this.variables = variables;
    props = PropsUi.getInstance();
    APPLICATION_NAME = "Edit Repository";
  }

  @Override
  public String open() {
    Control lastControl = addStandardWidgets();

    addButtons(lastControl);

    // Set the shell size, based upon previous time...
    BaseTransformDialog.setSize(shell);

    shell.open();

    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }

    return returnValue;
  }

  protected void addButtons(Control lastControl) {
    // Buttons go at the bottom of the dialog
    //
    Button wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOK.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOK, wCancel}, props.getMargin(), lastControl);
  }

  protected Control addStandardWidgets() {
    shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE);
    shell.setText(APPLICATION_NAME);
    shell.setImage(HopGitPerspective.getInstance().getGitImage());
    shell.addListener(SWT.Close, e -> cancel());
    shell.setLayout(new FormLayout());

    // The name line...
    //
    Label nameLabel = new Label(shell, SWT.RIGHT);
    nameLabel.setText("Name ");
    FormData fdNameLabel = new FormData();
    fdNameLabel.left = new FormAttachment(0, 0);
    fdNameLabel.right = new FormAttachment(props.getMiddlePct(), 0);
    fdNameLabel.top = new FormAttachment(0, props.getMargin());
    nameLabel.setLayoutData(fdNameLabel);

    nameText = new Text(shell, SWT.SINGLE | SWT.BORDER);
    nameText.setText(Const.NVL(repo.getName(), ""));
    FormData fdNameText = new FormData();
    fdNameText.left = new FormAttachment(props.getMiddlePct(), props.getMargin());
    fdNameText.right = new FormAttachment(100, 0);
    fdNameText.top = new FormAttachment(nameLabel, 0, SWT.CENTER);
    nameText.setLayoutData(fdNameText);
    Control lastControl = nameText;

    // The description line
    //
    Label descLabel = new Label(shell, SWT.RIGHT);
    descLabel.setText("Description ");
    FormData fdDescLabel = new FormData();
    fdDescLabel.left = new FormAttachment(0, 0);
    fdDescLabel.right = new FormAttachment(props.getMiddlePct(), 0);
    fdDescLabel.top = new FormAttachment(lastControl, props.getMargin());
    descLabel.setLayoutData(fdDescLabel);

    descText = new Text(shell, SWT.SINGLE | SWT.BORDER);
    descText.setText(Const.NVL(repo.getDescription(), ""));
    FormData fdDescText = new FormData();
    fdDescText.left = new FormAttachment(props.getMiddlePct(), props.getMargin());
    fdDescText.right = new FormAttachment(100, 0);
    fdDescText.top = new FormAttachment(descLabel, 0, SWT.CENTER);
    descText.setLayoutData(fdDescText);
    lastControl = descText;

    // The directory
    //
    Label directoryLabel = new Label(shell, SWT.RIGHT);
    directoryLabel.setText("Directory ");
    FormData fdDirectoryLabel = new FormData();
    fdDirectoryLabel.left = new FormAttachment(0, 0);
    fdDirectoryLabel.right = new FormAttachment(props.getMiddlePct(), 0);
    fdDirectoryLabel.top = new FormAttachment(lastControl, props.getMargin());
    directoryLabel.setLayoutData(fdDirectoryLabel);

    Button directoryButton = new Button(shell, SWT.PUSH);
    directoryButton.setText("Browse");
    directoryButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            String filterPath = BaseDialog.presentDirectoryDialog(shell, variables);
            if (filterPath != null) {
              directoryText.setText(filterPath);
            }
          }
        });
    FormData fdDirectoryButton = new FormData();
    fdDirectoryButton.right = new FormAttachment(100, 0);
    fdDirectoryButton.top = new FormAttachment(directoryLabel, 0, SWT.CENTER);
    directoryButton.setLayoutData(fdDirectoryButton);

    directoryText = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER);
    directoryText.setText(Const.NVL(repo.getDirectory(), ""));
    FormData fdDirectoryText = new FormData();
    fdDirectoryText.left = new FormAttachment(props.getMiddlePct(), props.getMargin());
    fdDirectoryText.right = new FormAttachment(directoryButton, -props.getMargin());
    fdDirectoryText.top = new FormAttachment(directoryLabel, 0, SWT.CENTER);
    directoryText.setLayoutData(fdDirectoryText);
    lastControl = directoryText;

    return lastControl;
  }

  protected void ok() {
    repo.setName(nameText.getText());
    repo.setDescription(descText.getText());
    repo.setDirectory(directoryText.getText());
    returnValue = repo.getName();
    dispose();
  }

  public void cancel() {
    returnValue = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
