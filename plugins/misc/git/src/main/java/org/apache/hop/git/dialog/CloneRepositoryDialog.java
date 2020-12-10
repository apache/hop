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

package org.apache.hop.git.dialog;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.model.repository.GitRepository;
import org.apache.hop.git.model.repository.GitRepositoryDialog;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.net.URISyntaxException;

public class CloneRepositoryDialog extends GitRepositoryDialog {

  private Text urlText;
  private String url;
  private Text cloneAsText;
  private String cloneAs;

  public CloneRepositoryDialog(
      Shell parentShell,
      IHopMetadataProvider metadataProvider,
      GitRepository repo,
      IVariables variables) {
    super(parentShell, metadataProvider, repo, variables);
    APPLICATION_NAME = "Clone Repository";
  }

  @Override
  public String open() {
    Control lastControl = addStandardWidgets();
    lastControl = addExtraControls(lastControl);
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

  protected Control addExtraControls(Control lastControl) {

    Label urlLabel = new Label(shell, SWT.RIGHT);
    urlLabel.setText("Source URL: ");
    FormData fdUrlLabel = new FormData();
    fdUrlLabel.top = new FormAttachment(lastControl, props.getMargin());
    fdUrlLabel.left = new FormAttachment(0, 0);
    fdUrlLabel.right = new FormAttachment(props.getMiddlePct(), 0);
    urlLabel.setLayoutData(fdUrlLabel);

    urlText = new Text(shell, SWT.SINGLE | SWT.BORDER);
    FormData fdUrlText = new FormData();
    fdUrlText.top = new FormAttachment(urlLabel, 0, SWT.CENTER);
    fdUrlText.left = new FormAttachment(props.getMiddlePct(), props.getMargin());
    fdUrlText.right = new FormAttachment(100, 0);
    urlText.setLayoutData(fdUrlText);
    lastControl = urlText;

    Label cloneAsLabel = new Label(shell, SWT.RIGHT);
    cloneAsLabel.setText("Clone As: ");
    FormData fdCloneAsLabel = new FormData();
    fdCloneAsLabel.top = new FormAttachment(lastControl, props.getMargin());
    fdCloneAsLabel.left = new FormAttachment(0, 0);
    fdCloneAsLabel.right = new FormAttachment(props.getMiddlePct(), 0);
    cloneAsLabel.setLayoutData(fdCloneAsLabel);

    cloneAsText = new Text(shell, SWT.SINGLE | SWT.BORDER);
    FormData fdCloneAsText = new FormData();
    fdCloneAsText.top = new FormAttachment(urlLabel, 0, SWT.CENTER);
    fdCloneAsText.left = new FormAttachment(props.getMiddlePct(), props.getMargin());
    fdCloneAsText.right = new FormAttachment(100, 0);
    cloneAsText.setLayoutData(fdCloneAsText);
    lastControl = cloneAsText;

    urlText.addModifyListener(
        event -> {
          String url = ((Text) event.widget).getText();
          URIish uri;
          try {
            uri = new URIish(url);
            cloneAsText.setText(uri.getHumanishName());
          } catch (URISyntaxException e) {
            //        e.printStackTrace();
          }
        });

    return lastControl;
  }

  @Override
  protected void ok() {
    url = urlText.getText();
    cloneAs = cloneAsText.getText();
    super.ok();
  }

  public String getURL() {
    return url;
  }

  public String getCloneAs() {
    return cloneAs;
  }
}
