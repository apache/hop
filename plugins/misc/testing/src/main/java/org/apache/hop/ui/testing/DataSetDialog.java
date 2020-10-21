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

package org.apache.hop.ui.testing;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetCsvUtil;
import org.apache.hop.testing.DataSetField;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metastore.IMetadataDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.File;
import java.util.List;

public class DataSetDialog extends Dialog implements IMetadataDialog {
  private static final Class<?> PKG = DataSetDialog.class; // for i18n purposes, needed by Translator2!!

  private DataSet dataSet;

  private Shell shell;

  private Text wName;
  private Text wDescription;
  private Text wBaseFilename;
  private TableView wFieldMapping;
  private TextVar wFolderName;

  private PropsUi props;

  private int middle;
  private int margin;

  private String returnValue;

  private IHopMetadataProvider metadataProvider;

  public DataSetDialog( Shell parent, IHopMetadataProvider metadataProvider, DataSet dataSet ) {
    super( parent, SWT.NONE );
    this.metadataProvider = metadataProvider;
    this.dataSet = dataSet;
    props = PropsUi.getInstance();
    returnValue = null;
  }

  public String open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageTable() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "DataSetDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // The name of the group...
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "DataSetDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( middle, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    // The description of the group...
    //
    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "DataSetDialog.Description.Label" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, 0 );
    fdlDescription.right = new FormAttachment( middle, -margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( lastControl, margin );
    fdDescription.left = new FormAttachment( middle, 0 );
    fdDescription.right = new FormAttachment( 100, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;

    // The folder containing the set...
    //
    Label wlFolderName = new Label( shell, SWT.RIGHT );
    props.setLook( wlFolderName );
    wlFolderName.setText( BaseMessages.getString( PKG, "DataSetDialog.FolderName.Label" ) );
    FormData fdlFolderName = new FormData();
    fdlFolderName.top = new FormAttachment( lastControl, margin );
    fdlFolderName.left = new FormAttachment( 0, 0 );
    fdlFolderName.right = new FormAttachment( middle, -margin );
    wlFolderName.setLayoutData( fdlFolderName );
    wFolderName = new TextVar( dataSet, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFolderName );
    FormData fdFolderName = new FormData();
    fdFolderName.top = new FormAttachment( lastControl, margin );
    fdFolderName.left = new FormAttachment( middle, 0 );
    fdFolderName.right = new FormAttachment( 100, 0 );
    wFolderName.setLayoutData( fdFolderName );
    lastControl = wFolderName;

    // The table storing the set...
    //
    Label wlBaseFilename = new Label( shell, SWT.RIGHT );
    props.setLook( wlBaseFilename );
    wlBaseFilename.setText( BaseMessages.getString( PKG, "DataSetDialog.BaseFilename.Label" ) );
    FormData fdlBaseFilename = new FormData();
    fdlBaseFilename.top = new FormAttachment( lastControl, margin );
    fdlBaseFilename.left = new FormAttachment( 0, 0 );
    fdlBaseFilename.right = new FormAttachment( middle, -margin );
    wlBaseFilename.setLayoutData( fdlBaseFilename );
    wBaseFilename = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBaseFilename );
    FormData fdBaseFilename = new FormData();
    fdBaseFilename.top = new FormAttachment( lastControl, margin );
    fdBaseFilename.left = new FormAttachment( middle, 0 );
    fdBaseFilename.right = new FormAttachment( 100, 0 );
    wBaseFilename.setLayoutData( fdBaseFilename );
    lastControl = wBaseFilename;
    

    // The field mapping from the input to the data set...
    //
    Label wlFieldMapping = new Label( shell, SWT.NONE );
    wlFieldMapping.setText( BaseMessages.getString( PKG, "DataSetDialog.FieldMapping.Label" ) );
    props.setLook( wlFieldMapping );
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment( 0, 0 );
    fdlUpIns.top = new FormAttachment( lastControl, margin * 2 );
    wlFieldMapping.setLayoutData( fdlUpIns );
    lastControl = wlFieldMapping;

    // Buttons at the bottom...
    //
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, e -> ok() );

    Button wEditData = new Button( shell, SWT.PUSH );
    wEditData.setText( BaseMessages.getString( PKG, "DataSetDialog.EditData.Button" ) );
    wEditData.addListener( SWT.Selection, e -> editData() );

    Button wViewData = new Button( shell, SWT.PUSH );
    wViewData.setText( BaseMessages.getString( PKG, "DataSetDialog.ViewData.Button" ) );
    wViewData.addListener( SWT.Selection, e -> viewData() );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    Button[] buttons = new Button[] { wOK, wEditData, wViewData, wCancel };
    BaseTransformDialog.positionBottomButtons( shell, buttons, margin, null );

    // the field mapping grid in between
    //
    ColumnInfo[] columns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldName" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldType" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldFormat" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldLength" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldPrecision" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.Comment" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };

    wFieldMapping = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      columns,
      dataSet.getFields().size(),
      null, props );

    FormData fdFieldMapping = new FormData();
    fdFieldMapping.left = new FormAttachment( 0, 0 );
    fdFieldMapping.top = new FormAttachment( lastControl, margin );
    fdFieldMapping.right = new FormAttachment( 100, 0 );
    fdFieldMapping.bottom = new FormAttachment( wOK, -2 * margin );
    wFieldMapping.setLayoutData( fdFieldMapping );


    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );
    wDescription.addSelectionListener( selAdapter );
    wBaseFilename.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return returnValue;
  }

  private void verifySettings() throws HopException {
    try {
      if ( StringUtil.isEmpty( wBaseFilename.getText() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "DataSetDialog.Error.NoTableSpecified" ) );
      }
    } catch(Exception e) {
      throw new HopException( "Error validating group and table values", e );
    }

  }

  protected void editData() {

    // If the row count is too high, we don't want to load it into memory...
    // Too high simply means: above the preview size...
    //
    int previewSize = props.getDefaultPreviewSize();
    try {

      verifySettings();

      DataSet set = new DataSet();
      set.initializeVariablesFrom( HopGui.getInstance().getVariables() );
      getInfo( set );

      // get rows from the data set...
      //
      List<Object[]> rows = set.getAllRows( LogChannel.UI );

      IRowMeta fieldsRowMeta = set.getSetRowMeta();

      boolean written = false;
      while (!written) {
        try {
          EditRowsDialog editRowsDialog = new EditRowsDialog( shell, SWT.NONE,
            BaseMessages.getString( PKG, "DataSetDialog.EditRows.Title" ),
            BaseMessages.getString( PKG, "DataSetDialog.EditRows.Message", set.getName() ),
            fieldsRowMeta,
            rows );
          List<Object[]> newList = editRowsDialog.open();
          if ( newList != null ) {
            File setFolder = new File( set.getActualDataSetFolder() );
            boolean folderExists = setFolder.exists();
            if ( !folderExists ) {
              MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION );
              box.setText( "Create data sets folder?" );
              box.setMessage( "The data sets folder does not exist. Do you want to create it?" + Const.CR + set.getActualDataSetFolder() );
              int answer = box.open();
              if ( ( answer & SWT.YES ) != 0 ) {
                setFolder.mkdirs();
                folderExists = true;
              } else if ( ( answer & SWT.CANCEL ) != 0 ) {
                break;
              }

            }
            // Write the rows back to the data set
            //
            if ( folderExists ) {
              DataSetCsvUtil.writeDataSetData( set, fieldsRowMeta, newList );
              written = true;
            }
          } else {
            // User hit cancel
            break;
          }
        } catch(Exception e) {
          new ErrorDialog( shell, "Error", "Error writing data to dataset file "+dataSet.getActualDataSetFilename(), e );
        }
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error previewing data from dataset table", e );
    }
  }

  protected void viewData() {
    try {
      DataSet set = new DataSet();
      set.initializeVariablesFrom( HopGui.getInstance().getVariables() );
      getInfo( set );
      verifySettings();

      List<Object[]> setRows = set.getAllRows( LogChannel.UI );
      IRowMeta setRowMeta = set.getSetRowMeta();

      PreviewRowsDialog previewRowsDialog = new PreviewRowsDialog( shell, new Variables(), SWT.NONE, set.getName(), setRowMeta, setRows );
      previewRowsDialog.open();

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error previewing data from dataset table", e );
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {

    wName.setText( Const.NVL( dataSet.getName(), "" ) );
    wDescription.setText( Const.NVL( dataSet.getDescription(), "" ) );
    wFolderName.setText( Const.NVL( dataSet.getFolderName(), "" ) );
    wBaseFilename.setText( Const.NVL( dataSet.getBaseFilename(), "" ) );
    for ( int i = 0; i < dataSet.getFields().size(); i++ ) {
      DataSetField field = dataSet.getFields().get( i );
      int colNr = 1;
      wFieldMapping.setText( Const.NVL( field.getFieldName(), "" ), colNr++, i );
      wFieldMapping.setText( ValueMetaFactory.getValueMetaName( field.getType() ), colNr++, i );
      wFieldMapping.setText( Const.NVL( field.getFormat(), "" ), colNr++, i );
      wFieldMapping.setText( field.getLength() >= 0 ? Integer.toString( field.getLength() ) : "", colNr++, i );
      wFieldMapping.setText( field.getPrecision() >= 0 ? Integer.toString( field.getPrecision() ) : "", colNr++, i );
      wFieldMapping.setText( Const.NVL( field.getComment(), "" ), colNr++, i );
    }
    wName.setFocus();
  }

  private void cancel() {
    returnValue = null;
    dispose();
  }

  /**
   * @param set The data set to load the dialog information into
   */
  public void getInfo( DataSet set ) {
    set.setName( wName.getText() );
    set.setDescription( wDescription.getText() );
    set.setFolderName( wFolderName.getText() );
    set.setBaseFilename( wBaseFilename.getText() );
    set.getFields().clear();
    int nrFields = wFieldMapping.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFieldMapping.getNonEmpty( i );
      int colnr = 1;
      String fieldName = item.getText( colnr++ );
      int type = ValueMetaFactory.getIdForValueMeta( item.getText( colnr++ ) );
      String format = item.getText( colnr++ );
      int length = Const.toInt( item.getText( colnr++ ), -1 );
      int precision = Const.toInt( item.getText( colnr++ ), -1 );
      String comment = item.getText( colnr++ );

      DataSetField field = new DataSetField( fieldName, type, length, precision, comment, format );
      set.getFields().add( field );
    }

  }

  public void ok() {

    try {
      verifySettings();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", BaseMessages.getString( PKG, "DataSetDialog.Error.ValidationError" ), e );
    }

    getInfo( dataSet );

    returnValue = dataSet.getName();
    dispose();

  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

}
