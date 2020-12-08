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

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class UsernamePasswordDialog extends Dialog {

  private Text usernameText;
  private Text passwordText;
  private String username;
  private String password;

  public UsernamePasswordDialog(Shell parentShell) {
    super(parentShell);
  }

  @Override
  protected Control createDialogArea(Composite parent) {
    Composite comp = (Composite) super.createDialogArea(parent);

    GridLayout layout = (GridLayout) comp.getLayout();
    layout.numColumns = 2;

    Label usernameLabel = new Label(comp, SWT.RIGHT);
    usernameLabel.setText("Username: ");
    usernameText = new Text(comp, SWT.SINGLE | SWT.BORDER);
    usernameText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

    Label passwordLabel = new Label(comp, SWT.RIGHT);
    passwordLabel.setText("Password: ");
    passwordText = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.PASSWORD);
    passwordText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

    return comp;
  }

  @Override
  protected void okPressed() {
    username = usernameText.getText();
    password = passwordText.getText();
    super.okPressed();
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
