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

import org.apache.commons.lang.StringUtils;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Allows the user to edit the system settings of the hop.config file.
 *
 * @author Matt
 */
public class HopSystemPropertiesDialog extends Dialog {
  private static Class<?> PKG = HopSystemPropertiesDialog.class; // for i18n purposes, needed by Translator!!

  private TableView wFields;

  private Shell shell;
  private final PropsUi props;

  private Map<String, String> systemProperties;

  /**
   * Constructs a new dialog
   *
   * @param parent  The parent shell to link to
   */
  public HopSystemPropertiesDialog( Shell parent ) {
    super( parent, SWT.NONE );
    props = PropsUi.getInstance();
    systemProperties = null;
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

    // The buttons at the bottom
    //
    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wFields );

    // Message line at the top
    //
    Label wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "HopPropertiesFileDialog.Message" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, margin );
    wlFields.setLayoutData( fdlFields );

    int FieldsRows = 0;

    ColumnInfo[] columns = {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HopPropertiesFileDialog.Name.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HopPropertiesFileDialog.Value.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HopPropertiesFileDialog.Description.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };
    columns[ 2 ].setDisabledListener( rowNr -> false );

    // Fields between the label and the buttons
    //
    wFields = new TableView( Variables.getADefaultVariableSpace(), shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columns,
        FieldsRows, null, props );

    wFields.setReadonly( false );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, 2*margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2*margin );
    wFields.setLayoutData( fdFields );



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
    return systemProperties;
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

      // Here are the current properties...
      //
      systemProperties = HopConfig.readSystemProperties();

      // Obtain and sort the list of keys...
      //
      List<String> keys = new ArrayList<>(systemProperties.keySet() );
      Collections.sort(keys);

      for (String key : keys) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        int col=1;
        String value = systemProperties.get( key );
        String description = variablesList.getDescriptionMap().get( key );

        item.setText( col++, Const.NVL(key, "") );
        item.setText( col++, Const.NVL(value, "") );
        item.setText( col++, Const.NVL(description, "") );
      }

      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );

    } catch ( Exception e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "HopPropertiesFileDialog.Exception.ErrorLoadingData.Title" ),
        BaseMessages.getString( PKG, "HopPropertiesFileDialog.Exception.ErrorLoadingData.Message" ), e );
    }
  }

  private void cancel() {
    systemProperties = null;
    dispose();
  }

  private void ok() {
    for (int i=0;i<wFields.nrNonEmpty();i++) {
      TableItem item = wFields.getNonEmpty( i );
      String key = item.getText(1);
      String value = item.getText(2);
      if ( StringUtils.isNotEmpty(key)) {
        systemProperties.put( key, Const.NVL(value, "") );
      }
    }

    dispose();
  }
}
