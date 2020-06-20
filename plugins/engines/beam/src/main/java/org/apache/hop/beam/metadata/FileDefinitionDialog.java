package org.apache.hop.beam.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metastore.IMetadataDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class FileDefinitionDialog implements IMetadataDialog {

  private static Class<?> PKG = FileDefinitionDialog.class; // for i18n purposes, needed by Translator2!!

  private FileDefinition fileDefinition;

  private Shell parent;
  private Shell shell;
  private IHopMetadataProvider metadataProvider;
  private IVariables variables;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private Text wSeparator;
  private Text wEnclosure;
  private TableView wFields;
  private Button wOk;
  private Button wCancel;

  Control lastControl;

  private PropsUi props;

  private int middle;
  private int margin;

  private String definitionName;

  public FileDefinitionDialog( Shell parent, IHopMetadataProvider metadataProvider, FileDefinition fileDefinition, IVariables variables ) {
    this.parent = parent;
    this.metadataProvider = metadataProvider;
    this.fileDefinition = fileDefinition;
    this.variables = variables;
    props = PropsUi.getInstance();
    definitionName = null;
  }

  public String open() {
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageServer() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Shell.Title" ) );
    shell.setLayout( formLayout );


    // Buttons at the bottom of the dialo...
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, null );

    // The rest of the dialog is for the widgets...
    //
    addFormWidgets();

    // Add listeners

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );
    wDescription.addSelectionListener( selAdapter );
    wSeparator.addSelectionListener( selAdapter );
    wEnclosure.addSelectionListener( selAdapter );

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
    return definitionName;
  }

  private void addFormWidgets() {

    // The name
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );
    lastControl = wName;

    // The description
    //
    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Description.Label" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlDescription.right = new FormAttachment( middle, -margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( wlDescription, 0, SWT.CENTER );
    fdDescription.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdDescription.right = new FormAttachment( 95, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;

    // Separator
    //
    Label wlSeparator = new Label( shell, SWT.RIGHT );
    props.setLook( wlSeparator );
    wlSeparator.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Separator.Label" ) );
    FormData fdlSeparator = new FormData();
    fdlSeparator.top = new FormAttachment( lastControl, margin );
    fdlSeparator.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlSeparator.right = new FormAttachment( middle, -margin );
    wlSeparator.setLayoutData( fdlSeparator );
    wSeparator = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeparator );
    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment( wlSeparator, 0, SWT.CENTER );
    fdSeparator.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSeparator.right = new FormAttachment( 95, 0 );
    wSeparator.setLayoutData( fdSeparator );
    lastControl = wSeparator;

    // Enclosure
    //
    Label wlEnclosure = new Label( shell, SWT.RIGHT );
    props.setLook( wlEnclosure );
    wlEnclosure.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Enclosure.Label" ) );
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.top = new FormAttachment( lastControl, margin );
    fdlEnclosure.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlEnclosure.right = new FormAttachment( middle, -margin );
    wlEnclosure.setLayoutData( fdlEnclosure );
    wEnclosure = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosure );
    FormData fdEnclosure = new FormData();
    fdEnclosure.top = new FormAttachment( wlEnclosure, 0, SWT.CENTER );
    fdEnclosure.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdEnclosure.right = new FormAttachment( 95, 0 );
    wEnclosure.setLayoutData( fdEnclosure );
    lastControl = wEnclosure;

    // Fields...
    //
    Label wlFields = new Label( shell, SWT.LEFT );
    props.setLook( wlFields );
    wlFields.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Label" ) );
    FormData fdlFields = new FormData();
    fdlFields.top = new FormAttachment( lastControl, margin );
    fdlFields.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlFields.right = new FormAttachment( 100, 0);
    wlFields.setLayoutData( fdlFields );

    ColumnInfo[] columnInfos = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldName" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), false ),
      new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldFormat" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldLength" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "FileDefinitionDialog.Fields.Column.FieldPrecision" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };

    wFields = new TableView( new Variables(), shell, SWT.BORDER, columnInfos, fileDefinition.getFieldDefinitions().size(), null, props );
    props.setLook( wFields );
    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdFields.right = new FormAttachment( 100, 0);
    fdFields.bottom = new FormAttachment( wOk, -margin*2);
    wFields.setLayoutData( fdFields );
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( fileDefinition.getName(), "" ) );
    wDescription.setText( Const.NVL( fileDefinition.getDescription(), "" ) );
    wSeparator.setText( Const.NVL( fileDefinition.getSeparator(), "" ) );
    wEnclosure.setText( Const.NVL( fileDefinition.getEnclosure(), "" ) );

    List<FieldDefinition> fields = fileDefinition.getFieldDefinitions();
    for (int i=0;i<fields.size();i++) {
      FieldDefinition field = fields.get( i );
      TableItem item = wFields.table.getItem( i );
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getHopType(), ""));
      item.setText(3, Const.NVL(field.getFormatMask(), ""));
      item.setText(4, field.getLength()<0 ? "" : Integer.toString(field.getLength()));
      item.setText(5, field.getPrecision()<0 ? "" : Integer.toString(field.getPrecision()));
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    definitionName = null;
    dispose();
  }

  public void ok() {
    if ( StringUtils.isEmpty( wName.getText() ) ) {
      MessageBox box = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      box.setText( BaseMessages.getString( PKG, "FileDefinitionDialog.NoNameDialog.Title" ) );
      box.setMessage( BaseMessages.getString( PKG, "FileDefinitionDialog.NoNameDialog.Message" ) );
      box.open();
      return;
    }
    getInfo( fileDefinition );
    definitionName = fileDefinition.getName();
    dispose();
  }

  // Get dialog info in securityService
  private void getInfo( FileDefinition def ) {
    def.setName( wName.getText() );
    def.setDescription( wDescription.getText() );
    def.setSeparator( wSeparator.getText() );
    def.setEnclosure( wEnclosure.getText() );
    def.getFieldDefinitions().clear();
    for (int i=0;i<wFields.nrNonEmpty();i++) {
      TableItem item = wFields.getNonEmpty( i );
      String name = item.getText(1);
      String hopType = item.getText(2);
      String formatMask = item.getText(3);
      int length = Const.toInt(item.getText(4), -1);
      int precision = Const.toInt(item.getText(5), -1);
      def.getFieldDefinitions().add(new FieldDefinition( name, hopType, length, precision, formatMask ));
    }
  }
}
