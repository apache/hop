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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopVariablesList;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Allows the user to edit the hop.config file.
 *
 * @author Matt
 */
public class HopConfigFileDialog extends Dialog {
  private static Class<?> PKG = HopConfigFileDialog.class; // for i18n purposes, needed by Translator!!

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private Shell shell;
  private PropsUi props;

  private Map<String, String> kettleProperties;

  private Set<String> previousHopPropertiesKeys;

  /**
   * Constructs a new dialog
   *
   * @param parent  The parent shell to link to
   * @param style   The style in which we want to draw this shell.
   */
  public HopConfigFileDialog( Shell parent, int style ) {
    super( parent, style );
    props = PropsUi.getInstance();
    kettleProperties = null;
  }

  public Map<String, String> open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX );
    shell.setImage( GuiResource.getInstance().getImagePipelineGraph() );
    props.setLook( shell );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "HopPropertiesFileDialog.Title" ) );

    int margin = props.getMargin();

    // Message line
    //
    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "HopPropertiesFileDialog.Message" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, margin );
    wlFields.setLayoutData( fdlFields );

    int FieldsRows = 0;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HopPropertiesFileDialog.Name.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HopPropertiesFileDialog.Value.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HopPropertiesFileDialog.Description.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };
    colinf[ 2 ].setDisabledListener( rowNr -> false );

    wFields =
      new TableView(
        Variables.getADefaultVariableSpace(), shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf,
        FieldsRows, null, props );

    wFields.setReadonly( false );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, 30 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wFields );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return kettleProperties;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    try {
      // These are the standard Hop variables...
      //
      HopVariablesList variablesList = HopVariablesList.getInstance();

      // Obtain and sort the list of keys...
      //
      List<String> keys = HopConfig.getSortedKeys();

      // TODO: populate dialog with GuiPlugin widgets from configuration plugins
      //

    } catch ( Exception e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "HopPropertiesFileDialog.Exception.ErrorLoadingData.Title" ),
        BaseMessages.getString( PKG, "HopPropertiesFileDialog.Exception.ErrorLoadingData.Message" ), e );
    }
  }

  private void cancel() {
    kettleProperties = null;
    dispose();
  }

  private void ok() {
    // TODO : get info using GuiCompositeWidgets
    dispose();
  }
}
