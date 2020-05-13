/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.concurrent.atomic.AtomicBoolean;

public class DirectoryDialogButtonListenerFactory {
  public static final SelectionAdapter getSelectionAdapter( final Shell shell, final TextVar destination ) {
    // Listen to the Browse... button
    return new SelectionAdapter() {
      public void widgetSelected( SelectionEvent event ) {
        DirectoryDialog directoryDialog = new DirectoryDialog( shell, SWT.OPEN );
        if ( destination.getText() != null ) {
          String filterPath = destination.getText();
          directoryDialog.setFilterPath( filterPath );
        }

        AtomicBoolean doIt = new AtomicBoolean( true );
        try {
          ExtensionPointHandler.callExtensionPoint( LogChannel.UI, HopGuiExtensionPoint.HopGuiFileDirectoryDialog.id,
            new HopGuiDirectoryDialogExtension( doIt, directoryDialog ) );
        } catch(Exception xe) {
          LogChannel.UI.logError( "Error handling extension point 'HopGuiFileDirectoryDialog'", xe );
        }

        // doIt false means: don't open the dialog, just get the value from it.
        // We assume the plugin changed it.
        //
        if ( !doIt.get() || directoryDialog.open() != null ) {
          String str = directoryDialog.getFilterPath();
          destination.setText( str );
        }
      }
    };
  }
}
