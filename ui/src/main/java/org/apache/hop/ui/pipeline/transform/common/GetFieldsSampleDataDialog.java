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

package org.apache.hop.ui.pipeline.transform.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

/**
 * A dialog that allows the user to select the number of  data rows to sample, when fetching fields.
 */
public class GetFieldsSampleDataDialog extends EnterNumberDialog {

  private static final Class<?> PKG = GetFieldsSampleDataDialog.class;

  private static final int SAMPLE_SIZE = 100;

  private static final int SHELL_WIDTH = 700;

  private IGetFieldsCapableTransformDialog parentDialog;
  private boolean reloadAllFields;

  public GetFieldsSampleDataDialog( final Shell parentShell, final IGetFieldsCapableTransformDialog parentDialog,
                                    final boolean reloadAllFields ) {
    super( parentShell, SAMPLE_SIZE,
      BaseMessages.getString( PKG, "GetFieldsSampleSizeDialog.Title" ),
      BaseMessages.getString( PKG, "GetFieldsSampleSizeDialog.Message" ),
      BaseMessages.getString( PKG, "GetFieldsSampleSizeDialog.ShowSample.Message" ), SHELL_WIDTH );
    this.parentDialog = parentDialog;
    this.reloadAllFields = reloadAllFields;
  }

  @Override
  protected void ok() {
    try {
      samples = Integer.parseInt( wNumber.getText() );
      handleOk( samples );
      dispose();
    } catch ( Exception e ) {
      MessageBox mb = new MessageBox( getParent(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "Dialog.Error.EnterInteger" ) );
      mb.setText( BaseMessages.getString( PKG, "Dialog.Error.Header" ) );
      mb.open();
      wNumber.selectAll();
    }
  }

  protected void handleOk( final int samples ) {
    if ( samples >= 0 ) {
      String message = parentDialog.loadFields( parentDialog.getPopulatedMeta(), samples, reloadAllFields );
      if ( wCheckbox != null && wCheckbox.getSelection() ) {
        if ( StringUtils.isNotBlank( message ) ) {
          final EnterTextDialog etd =
            new EnterTextDialog( parentDialog.getShell(),
              BaseMessages.getString( PKG, "GetFieldsSampleDataDialog.ScanResults.DialogTitle" ),
              BaseMessages.getString( PKG, "GetFieldsSampleDataDialog.ScanResults.DialogMessage" ), message, true );
          etd.setReadOnly();
          etd.setModal();
          etd.open();
        } else {

          MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          box.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
          box.setMessage( BaseMessages.getString( PKG, "GetFieldsSampleDataDialog.ScanResults.Error.Message" ) );
          box.open();

        }
      }
    }
  }
}
