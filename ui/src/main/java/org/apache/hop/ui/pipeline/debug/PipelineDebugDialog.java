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

package org.apache.hop.ui.pipeline.debug;

import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.debug.TransformDebugMeta;
import org.apache.hop.pipeline.debug.PipelineDebugMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ConditionEditor;
import org.apache.hop.ui.core.widget.LabelText;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.Hashtable;
import java.util.Map;

/**
 * Allows you to edit/enter the pipeline debugging information
 *
 * @author matt
 * @since version 3.0 RC1
 */
public class PipelineDebugDialog extends Dialog {
  private static final Class<?> PKG = PipelineDebugDialog.class; // For Translator

  public static final int DEBUG_CANCEL = 0;
  public static final int DEBUG_LAUNCH = 1;
  public static final int DEBUG_CONFIG = 2;

  private Display display;
  private Shell parent;
  private Shell shell;
  private PropsUi props;
  private int retval;

  private Button wOk, wCancel, wLaunch;

  private TableView wTransforms;

  private final IVariables variables;
  private PipelineDebugMeta pipelineDebugMeta;
  private Composite wComposite;
  private LabelText wRowCount;
  private int margin;
  private int middle;
  private Button wFirstRows;
  private Button wPauseBreakPoint;
  private Condition condition;
  private IRowMeta transformInputFields;
  private ConditionEditor wCondition;
  private Label wlCondition;
  private Map<TransformMeta, TransformDebugMeta> transformDebugMetaMap;
  private int previousIndex;

  public PipelineDebugDialog( Shell parent, IVariables variables, PipelineDebugMeta pipelineDebugMeta ) {
    super( parent );
    this.variables = variables;
    this.parent = parent;
    this.pipelineDebugMeta = pipelineDebugMeta;
    props = PropsUi.getInstance();

    // Keep our own map of transform debugging information...
    //
    transformDebugMetaMap = new Hashtable<>();
    transformDebugMetaMap.putAll( pipelineDebugMeta.getTransformDebugMetaMap() );

    previousIndex = -1;

    retval = DEBUG_CANCEL;
  }

  public int open() {

    display = parent.getDisplay();
    shell =
      new Shell( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImagePipeline() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.Shell.Title" ) );

    margin = props.getMargin();
    middle = props.getMiddlePct();

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.Configure.Label" ) );
    wOk.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        ok( true );
      }
    } );
    wLaunch = new Button( shell, SWT.PUSH );
    wLaunch.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.Launch.Label" ) );
    wLaunch.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        ok( false );
      }
    } );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        cancel();
      }
    } );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wLaunch, wOk, wCancel }, margin, null );

    wOk.setToolTipText( BaseMessages.getString( PKG, "PipelineDebugDialog.Configure.ToolTip" ) );
    wLaunch.setToolTipText( BaseMessages.getString( PKG, "PipelineDebugDialog.Launch.ToolTip" ) );

    // Add the list of transforms
    //
    ColumnInfo[] transformColumns =
      { new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDebugDialog.Column.TransformName" ), ColumnInfo.COLUMN_TYPE_TEXT, false,
        true ), // name,
        // non-numeric,
        // readonly
      };

    int nrTransforms = pipelineDebugMeta.getPipelineMeta().nrTransforms();
    wTransforms =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE, transformColumns,
        nrTransforms, true, null, props );
    FormData fdTransform = new FormData();
    fdTransform.left = new FormAttachment( 0, 0 );
    fdTransform.right = new FormAttachment( middle, -margin );
    fdTransform.top = new FormAttachment( 0, margin );
    fdTransform.bottom = new FormAttachment( wOk, -margin * 2 );
    wTransforms.setLayoutData( fdTransform );
    wTransforms.table.setHeaderVisible( false );

    // If someone clicks on a row, we want to refresh the right pane...
    //
    wTransforms.table.addSelectionListener( new SelectionAdapter() {

      @Override
      public void widgetSelected( SelectionEvent e ) {
        // Before we show anything, make sure to save the content of the screen...
        //
        getTransformDebugMeta();

        // Now show the information...
        //
        showTransformDebugInformation();
      }

    } );

    // If someone presses enter, launch the pipeline (this allows for "quick-preview")
    wTransforms.table.addKeyListener( new KeyAdapter() {

      @Override
      public void keyPressed( KeyEvent e ) {
        if ( e.character == SWT.CR ) {
          wLaunch.notifyListeners( SWT.Selection, new Event() );
        }
      }
    } );

    // Now add the composite on which we will dynamically place a number of widgets, based on the selected transform...
    //
    wComposite = new Composite( shell, SWT.BORDER );
    props.setLook( wComposite );

    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment( middle, 0 );
    fdComposite.right = new FormAttachment( 100, 0 );
    fdComposite.top = new FormAttachment( 0, margin );
    fdComposite.bottom = new FormAttachment( wOk, -margin * 2 );
    wComposite.setLayoutData( fdComposite );

    // Give the composite a layout...
    FormLayout compositeLayout = new FormLayout();
    compositeLayout.marginWidth = Const.FORM_MARGIN;
    compositeLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout( compositeLayout );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    // Set the focus on the OK button
    //
    wLaunch.setFocus();
    shell.setDefaultButton( wLaunch );

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return retval;
  }

  private void getData() {
    // Save the latest changes to the screen...
    //
    getTransformDebugMeta();

    // Add the transforms...
    //
    refreshTransformList();

  }

  private void refreshTransformList() {
    GuiResource resource = GuiResource.getInstance();

    // Add the list of transforms...
    //
    int maxIconSize = 0;
    int indexSelected = -1;
    wTransforms.table.removeAll();
    for ( int i = 0; i < pipelineDebugMeta.getPipelineMeta().getTransforms().size(); i++ ) {
      TransformMeta transformMeta = pipelineDebugMeta.getPipelineMeta().getTransform( i );
      TableItem item = new TableItem( wTransforms.table, SWT.NONE );
      Image image =
        resource.getImagesTransforms().get( transformMeta.getTransformPluginId() ).getAsBitmapForSize( display, ConstUi.ICON_SIZE,
          ConstUi.ICON_SIZE );
      item.setImage( 0, image );
      item.setText( 0, "" );
      item.setText( 1, transformMeta.getName() );

      if ( image.getBounds().width > maxIconSize ) {
        maxIconSize = image.getBounds().width;
      }

      TransformDebugMeta transformDebugMeta = transformDebugMetaMap.get( transformMeta );
      if ( transformDebugMeta != null ) {
        // We have debugging information so we mark the row
        //
        item.setBackground( resource.getColorLight() );
        if ( indexSelected < 0 ) {
          indexSelected = i;
        }
      }
    }

    wTransforms.removeEmptyRows();
    wTransforms.optWidth( false );
    wTransforms.table.getColumn( 0 ).setWidth( maxIconSize + 10 );
    wTransforms.table.getColumn( 0 ).setAlignment( SWT.CENTER );

    // OK, select the first used transform debug line...
    //
    if ( indexSelected >= 0 ) {
      wTransforms.table.setSelection( indexSelected );
      showTransformDebugInformation();
    }
  }

  /**
   * Grab the transform debugging information from the dialog. Store it in our private map
   */
  private void getTransformDebugMeta() {
    int index = wTransforms.getSelectionIndex();
    if ( previousIndex >= 0 ) {
      // Is there anything on the composite to save yet?
      //
      if ( wComposite.getChildren().length == 0 ) {
        return;
      }

      TransformMeta transformMeta = pipelineDebugMeta.getPipelineMeta().getTransform( previousIndex );
      TransformDebugMeta transformDebugMeta = new TransformDebugMeta( transformMeta );
      transformDebugMeta.setCondition( condition );
      transformDebugMeta.setPausingOnBreakPoint( wPauseBreakPoint.getSelection() );
      transformDebugMeta.setReadingFirstRows( wFirstRows.getSelection() );
      transformDebugMeta.setRowCount( Const.toInt( wRowCount.getText(), -1 ) );

      transformDebugMetaMap.put( transformMeta, transformDebugMeta );
    }
    previousIndex = index;
  }

  private void getInfo( PipelineDebugMeta meta ) {
    meta.getTransformDebugMetaMap().clear();
    meta.getTransformDebugMetaMap().putAll( transformDebugMetaMap );
  }

  private void ok( boolean config ) {
    if ( config ) {
      retval = DEBUG_CONFIG;
    } else {
      retval = DEBUG_LAUNCH;
    }
    getTransformDebugMeta();
    getInfo( pipelineDebugMeta );
    dispose();
  }

  private void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void cancel() {
    retval = DEBUG_CANCEL;
    dispose();
  }

  private void showTransformDebugInformation() {

    // Now that we have all the information to display, let's put some widgets on our composite.
    // Before we go there, let's clear everything that was on there...
    //
    for ( Control control : wComposite.getChildren() ) {
      control.dispose();
    }
    wComposite.layout( true, true );

    int[] selectionIndices = wTransforms.table.getSelectionIndices();
    if ( selectionIndices == null || selectionIndices.length != 1 ) {
      return;
    }

    previousIndex = selectionIndices[ 0 ];

    // What transform did we click on?
    //
    final TransformMeta transformMeta = pipelineDebugMeta.getPipelineMeta().getTransform( selectionIndices[ 0 ] );

    // What is the transform debugging metadata?
    // --> This can be null (most likely scenario)
    //
    final TransformDebugMeta transformDebugMeta = transformDebugMetaMap.get( transformMeta );

    // At the top we'll put a few common items like first[x], etc.
    //

    // The row count (e.g. number of rows to keep)
    //
    wRowCount =
      new LabelText( wComposite, BaseMessages.getString( PKG, "PipelineDebugDialog.RowCount.Label" ), BaseMessages
        .getString( PKG, "PipelineDebugDialog.RowCount.ToolTip" ) );
    FormData fdRowCount = new FormData();
    fdRowCount.left = new FormAttachment( 0, 0 );
    fdRowCount.right = new FormAttachment( 100, 0 );
    fdRowCount.top = new FormAttachment( 0, 0 );
    wRowCount.setLayoutData( fdRowCount );
    wRowCount.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent arg0 ) {
        ok( false );
      }
    } );

    // Do we retrieve the first rows passing?
    //
    wFirstRows = new Button( wComposite, SWT.CHECK );
    props.setLook( wFirstRows );
    wFirstRows.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.FirstRows.Label" ) );
    wFirstRows.setToolTipText( BaseMessages.getString( PKG, "PipelineDebugDialog.FirstRows.ToolTip" ) );
    FormData fdFirstRows = new FormData();
    fdFirstRows.left = new FormAttachment( middle, 0 );
    fdFirstRows.right = new FormAttachment( 100, 0 );
    fdFirstRows.top = new FormAttachment( wRowCount, margin );
    wFirstRows.setLayoutData( fdFirstRows );

    // Do we pause on break point, when the condition is met?
    //
    wPauseBreakPoint = new Button( wComposite, SWT.CHECK );
    props.setLook( wPauseBreakPoint );
    wPauseBreakPoint.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.PauseBreakPoint.Label" ) );
    wPauseBreakPoint.setToolTipText( BaseMessages.getString( PKG, "PipelineDebugDialog.PauseBreakPoint.ToolTip" ) );
    FormData fdPauseBreakPoint = new FormData();
    fdPauseBreakPoint.left = new FormAttachment( middle, 0 );
    fdPauseBreakPoint.right = new FormAttachment( 100, 0 );
    fdPauseBreakPoint.top = new FormAttachment( wFirstRows, margin );
    wPauseBreakPoint.setLayoutData( fdPauseBreakPoint );

    // The condition to pause for...
    //
    condition = null;
    if ( transformDebugMeta != null ) {
      condition = transformDebugMeta.getCondition();
    }
    if ( condition == null ) {
      condition = new Condition();
    }

    // The input fields...
    try {
      transformInputFields = pipelineDebugMeta.getPipelineMeta().getTransformFields( variables, transformMeta );
    } catch ( HopTransformException e ) {
      transformInputFields = new RowMeta();
    }

    wlCondition = new Label( wComposite, SWT.RIGHT );
    props.setLook( wlCondition );
    wlCondition.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.Condition.Label" ) );
    wlCondition.setToolTipText( BaseMessages.getString( PKG, "PipelineDebugDialog.Condition.ToolTip" ) );
    FormData fdlCondition = new FormData();
    fdlCondition.left = new FormAttachment( 0, 0 );
    fdlCondition.right = new FormAttachment( middle, -margin );
    fdlCondition.top = new FormAttachment( wPauseBreakPoint, margin );
    wlCondition.setLayoutData( fdlCondition );

    wCondition = new ConditionEditor( wComposite, SWT.BORDER, condition, transformInputFields );
    FormData fdCondition = new FormData();
    fdCondition.left = new FormAttachment( middle, 0 );
    fdCondition.right = new FormAttachment( 100, 0 );
    fdCondition.top = new FormAttachment( wPauseBreakPoint, margin );
    fdCondition.bottom = new FormAttachment( 100, 0 );
    wCondition.setLayoutData( fdCondition );

    getTransformDebugData( transformDebugMeta );

    // Add a "clear" button at the bottom on the left...
    //
    Button wClear = new Button( wComposite, SWT.PUSH );
    props.setLook( wClear );
    wClear.setText( BaseMessages.getString( PKG, "PipelineDebugDialog.Clear.Label" ) );
    wClear.setToolTipText( BaseMessages.getString( PKG, "PipelineDebugDialog.Clear.ToolTip" ) );
    FormData fdClear = new FormData();
    fdClear.left = new FormAttachment( 0, 0 );
    fdClear.bottom = new FormAttachment( 100, 0 );
    wClear.setLayoutData( fdClear );

    wClear.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        // Clear the preview transform information for this transform...
        //
        transformDebugMetaMap.remove( transformMeta );
        wTransforms.table.setSelection( new int[] {} );
        previousIndex = -1;

        // refresh the transforms list...
        //
        refreshTransformList();

        showTransformDebugInformation();
      }
    } );

    wComposite.layout( true, true );
  }

  private void getTransformDebugData( TransformDebugMeta transformDebugMeta ) {
    if ( transformDebugMeta == null ) {
      return;
    }

    if ( transformDebugMeta.getRowCount() > 0 ) {
      wRowCount.setText( Integer.toString( transformDebugMeta.getRowCount() ) );
    } else {
      wRowCount.setText( "" );
    }

    wFirstRows.setSelection( transformDebugMeta.isReadingFirstRows() );
    wPauseBreakPoint.setSelection( transformDebugMeta.isPausingOnBreakPoint() );
  }
}
