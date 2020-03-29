/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.blockingstep;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@PluginDialog( id = "BlockingStep", image = "BLK.svg", pluginType = PluginDialog.PluginType.STEP,
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Blocking+step" )
public class BlockingStepDialog extends BaseStepDialog implements StepDialogInterface {
  private static final Class<?> PKG = BlockingStepDialog.class; // for i18n purposes, needed by Translator2!!

  private BlockingStepMeta input;

  private Label wlPassAllRows;
  private Button wPassAllRows;

  private Label wlSpoolDir;
  private Button wbSpoolDir;
  private TextVar wSpoolDir;
  private FormData fdlSpoolDir, fdbSpoolDir, fdSpoolDir;

  private Label wlPrefix;
  private Text wPrefix;
  private FormData fdlPrefix, fdPrefix;

  private Label wlCacheSize;
  private Text wCacheSize;
  private FormData fdlCacheSize, fdCacheSize;

  private Label wlCompress;
  private Button wCompress;
  private FormData fdlCompress, fdCompress;

  public BlockingStepDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BlockingStepMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "BlockingStepDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "BlockingStepDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Update the dimension?
    wlPassAllRows = new Label( shell, SWT.RIGHT );
    wlPassAllRows.setText( BaseMessages.getString( PKG, "BlockingStepDialog.PassAllRows.Label" ) );
    props.setLook( wlPassAllRows );
    FormData fdlUpdate = new FormData();
    fdlUpdate.left = new FormAttachment( 0, 0 );
    fdlUpdate.right = new FormAttachment( middle, -margin );
    fdlUpdate.top = new FormAttachment( wStepname, margin );
    wlPassAllRows.setLayoutData( fdlUpdate );
    wPassAllRows = new Button( shell, SWT.CHECK );
    props.setLook( wPassAllRows );
    FormData fdUpdate = new FormData();
    fdUpdate.left = new FormAttachment( middle, 0 );
    fdUpdate.top = new FormAttachment( wStepname, margin );
    fdUpdate.right = new FormAttachment( 100, 0 );
    wPassAllRows.setLayoutData( fdUpdate );

    // Clicking on update changes the options in the update combo boxes!
    wPassAllRows.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setEnableDialog();
      }
    } );

    // Temp directory for sorting
    wlSpoolDir = new Label( shell, SWT.RIGHT );
    wlSpoolDir.setText( BaseMessages.getString( PKG, "BlockingStepDialog.SpoolDir.Label" ) );
    props.setLook( wlSpoolDir );
    fdlSpoolDir = new FormData();
    fdlSpoolDir.left = new FormAttachment( 0, 0 );
    fdlSpoolDir.right = new FormAttachment( middle, -margin );
    fdlSpoolDir.top = new FormAttachment( wPassAllRows, margin );
    wlSpoolDir.setLayoutData( fdlSpoolDir );

    wbSpoolDir = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSpoolDir );
    wbSpoolDir.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbSpoolDir = new FormData();
    fdbSpoolDir.right = new FormAttachment( 100, 0 );
    fdbSpoolDir.top = new FormAttachment( wPassAllRows, margin );
    wbSpoolDir.setLayoutData( fdbSpoolDir );

    wSpoolDir = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSpoolDir );
    wSpoolDir.addModifyListener( lsMod );
    fdSpoolDir = new FormData();
    fdSpoolDir.left = new FormAttachment( middle, 0 );
    fdSpoolDir.top = new FormAttachment( wPassAllRows, margin );
    fdSpoolDir.right = new FormAttachment( wbSpoolDir, -margin );
    wSpoolDir.setLayoutData( fdSpoolDir );

    wbSpoolDir.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        DirectoryDialog dd = new DirectoryDialog( shell, SWT.NONE );
        dd.setFilterPath( wSpoolDir.getText() );
        String dir = dd.open();
        if ( dir != null ) {
          wSpoolDir.setText( dir );
        }
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version:
    wSpoolDir.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wSpoolDir.setToolTipText( transMeta.environmentSubstitute( wSpoolDir.getText() ) );
      }
    } );

    // Prefix of temporary file
    wlPrefix = new Label( shell, SWT.RIGHT );
    wlPrefix.setText( BaseMessages.getString( PKG, "BlockingStepDialog.Prefix.Label" ) );
    props.setLook( wlPrefix );
    fdlPrefix = new FormData();
    fdlPrefix.left = new FormAttachment( 0, 0 );
    fdlPrefix.right = new FormAttachment( middle, -margin );
    fdlPrefix.top = new FormAttachment( wbSpoolDir, margin * 2 );
    wlPrefix.setLayoutData( fdlPrefix );
    wPrefix = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPrefix );
    wPrefix.addModifyListener( lsMod );
    fdPrefix = new FormData();
    fdPrefix.left = new FormAttachment( middle, 0 );
    fdPrefix.top = new FormAttachment( wbSpoolDir, margin * 2 );
    fdPrefix.right = new FormAttachment( 100, 0 );
    wPrefix.setLayoutData( fdPrefix );

    // Maximum number of lines to keep in memory before using temporary files
    wlCacheSize = new Label( shell, SWT.RIGHT );
    wlCacheSize.setText( BaseMessages.getString( PKG, "BlockingStepDialog.CacheSize.Label" ) );
    props.setLook( wlCacheSize );
    fdlCacheSize = new FormData();
    fdlCacheSize.left = new FormAttachment( 0, 0 );
    fdlCacheSize.right = new FormAttachment( middle, -margin );
    fdlCacheSize.top = new FormAttachment( wPrefix, margin * 2 );
    wlCacheSize.setLayoutData( fdlCacheSize );
    wCacheSize = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCacheSize );
    wCacheSize.addModifyListener( lsMod );
    fdCacheSize = new FormData();
    fdCacheSize.left = new FormAttachment( middle, 0 );
    fdCacheSize.top = new FormAttachment( wPrefix, margin * 2 );
    fdCacheSize.right = new FormAttachment( 100, 0 );
    wCacheSize.setLayoutData( fdCacheSize );

    // Using compression for temporary files?
    wlCompress = new Label( shell, SWT.RIGHT );
    wlCompress.setText( BaseMessages.getString( PKG, "BlockingStepDialog.Compress.Label" ) );
    props.setLook( wlCompress );
    fdlCompress = new FormData();
    fdlCompress.left = new FormAttachment( 0, 0 );
    fdlCompress.right = new FormAttachment( middle, -margin );
    fdlCompress.top = new FormAttachment( wCacheSize, margin * 2 );
    wlCompress.setLayoutData( fdlCompress );
    wCompress = new Button( shell, SWT.CHECK );
    props.setLook( wCompress );
    fdCompress = new FormData();
    fdCompress.left = new FormAttachment( middle, 0 );
    fdCompress.top = new FormAttachment( wCacheSize, margin * 2 );
    fdCompress.right = new FormAttachment( 100, 0 );
    wCompress.setLayoutData( fdCompress );
    wCompress.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] {
      wOK, wCancel }, margin, wCompress );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    // Set the enablement of the dialog widgets
    setEnableDialog();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wPassAllRows.setSelection( input.isPassAllRows() );
    if ( input.getPrefix() != null ) {
      wPrefix.setText( input.getPrefix() );
    }
    if ( input.getDirectory() != null ) {
      wSpoolDir.setText( input.getDirectory() );
    }
    wCacheSize.setText( "" + input.getCacheSize() );
    wCompress.setSelection( input.getCompress() );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    input.setPrefix( wPrefix.getText() );
    input.setDirectory( wSpoolDir.getText() );
    input.setCacheSize( Const.toInt( wCacheSize.getText(), BlockingStepMeta.CACHE_SIZE ) );
    if ( isDetailed() ) {
      logDetailed( "Compression is set to " + wCompress.getSelection() );
    }
    input.setCompress( wCompress.getSelection() );
    input.setPassAllRows( wPassAllRows.getSelection() );

    dispose();
  }

  /**
   * Set the correct state "enabled or not" of the dialog widgets.
   */
  private void setEnableDialog() {
    wlSpoolDir.setEnabled( wPassAllRows.getSelection() );
    wbSpoolDir.setEnabled( wPassAllRows.getSelection() );
    wSpoolDir.setEnabled( wPassAllRows.getSelection() );
    wlPrefix.setEnabled( wPassAllRows.getSelection() );
    wPrefix.setEnabled( wPassAllRows.getSelection() );
    wlCacheSize.setEnabled( wPassAllRows.getSelection() );
    wCacheSize.setEnabled( wPassAllRows.getSelection() );
    wlCompress.setEnabled( wPassAllRows.getSelection() );
    wCompress.setEnabled( wPassAllRows.getSelection() );
  }
}
