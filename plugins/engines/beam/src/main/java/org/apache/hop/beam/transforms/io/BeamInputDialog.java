
package org.apache.hop.beam.transforms.io;

import org.apache.hop.beam.metastore.FileDefinition;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.TextVar;
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
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.Collections;
import java.util.List;


public class BeamInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = BeamInput.class; // for i18n purposes, needed by Translator2!!
  private final BeamInputMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private TextVar wInputLocation;
  private Combo wFileDefinition;

  public BeamInputDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (BeamInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "BeamInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    String fileDefinitionNames[];
    try {
      List<String> fileDefinitionNameList = new MetaStoreFactory<FileDefinition>( FileDefinition.class, metaStore ).getElementNames();
      Collections.sort( fileDefinitionNameList );

      fileDefinitionNames = fileDefinitionNameList.toArray( new String[ 0 ] );
    } catch ( Exception e ) {
      log.logError( "Error getting file definitions list", e );
      fileDefinitionNames = new String[] {};
    }

    // Stepname line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( wlTransformName, 0, SWT.CENTER );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    Label wlInputLocation = new Label( shell, SWT.RIGHT );
    wlInputLocation.setText( BaseMessages.getString( PKG, "BeamInputDialog.InputLocation" ) );
    props.setLook( wlInputLocation );
    FormData fdlInputLocation = new FormData();
    fdlInputLocation.left = new FormAttachment( 0, 0 );
    fdlInputLocation.top = new FormAttachment( lastControl, margin );
    fdlInputLocation.right = new FormAttachment( middle, -margin );
    wlInputLocation.setLayoutData( fdlInputLocation );
    wInputLocation = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputLocation );
    FormData fdInputLocation = new FormData();
    fdInputLocation.left = new FormAttachment( middle, 0 );
    fdInputLocation.top = new FormAttachment( wlInputLocation, 0, SWT.CENTER );
    fdInputLocation.right = new FormAttachment( 100, 0 );
    wInputLocation.setLayoutData( fdInputLocation );
    lastControl = wInputLocation;

    Label wlFileDefinition = new Label( shell, SWT.RIGHT );
    wlFileDefinition.setText( BaseMessages.getString( PKG, "BeamInputDialog.FileDefinition" ) );
    props.setLook( wlFileDefinition );
    FormData fdlFileDefinition = new FormData();
    fdlFileDefinition.left = new FormAttachment( 0, 0 );
    fdlFileDefinition.top = new FormAttachment( lastControl, margin );
    fdlFileDefinition.right = new FormAttachment( middle, -margin );
    wlFileDefinition.setLayoutData( fdlFileDefinition );
    wFileDefinition = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileDefinition );
    wFileDefinition.setItems( fileDefinitionNames );
    FormData fdFileDefinition = new FormData();
    fdFileDefinition.left = new FormAttachment( middle, 0 );
    fdFileDefinition.top = new FormAttachment( wlFileDefinition, 0, SWT.CENTER );
    fdFileDefinition.right = new FormAttachment( 100, 0 );
    wFileDefinition.setLayoutData( fdFileDefinition );
    lastControl = wFileDefinition;

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );


    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wFileDefinition.addSelectionListener( lsDef );
    wInputLocation.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }


  /**
   * Populate the widgets.
   */
  public void getData() {
    wTransformName.setText( transformName );
    wFileDefinition.setText( Const.NVL( input.getFileDescriptionName(), "" ) );
    wInputLocation.setText( Const.NVL( input.getInputLocation(), "" ) );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    getInfo( input );

    dispose();
  }

  private void getInfo( BeamInputMeta in ) {
    transformName = wTransformName.getText(); // return value

    in.setFileDescriptionName( wFileDefinition.getText() );
    in.setInputLocation( wInputLocation.getText() );

    input.setChanged();
  }
}