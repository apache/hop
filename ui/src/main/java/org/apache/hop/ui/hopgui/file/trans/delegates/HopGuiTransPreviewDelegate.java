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

package org.apache.hop.ui.hopgui.file.trans.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.ExecutionAdapter;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.engine.EngineMetric;
import org.apache.hop.trans.engine.EngineMetrics;
import org.apache.hop.trans.engine.IPipelineEngine;
import org.apache.hop.trans.engine.IEngineComponent;
import org.apache.hop.trans.step.RowAdapter;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.trans.HopGuiTransGraph;
import org.apache.hop.ui.hopgui.file.trans.extension.HopGuiTransPreviewExtension;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@GuiPlugin(
  description = "The transformation preview tab in the execution pane"
)
public class HopGuiTransPreviewDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiTransPreviewDelegate-ToolBar";

  private HopGui hopUi;
  private HopGuiTransGraph transGraph;

  private CTabItem transPreviewTab;

  private ToolBar toolbar;
  private Composite transPreviewComposite;

  protected Map<String, RowBuffer> previewDataMap;
  protected Map<String, String> previewLogMap;
  private Composite previewComposite;

  private Text logText;
  private TableView tableView;

  private StepMeta selectedStep;
  protected StepMeta lastSelectedStep;

  /**
   * @param hopUi
   * @param transGraph
   */
  public HopGuiTransPreviewDelegate( HopGui hopUi, HopGuiTransGraph transGraph ) {
    this.hopUi = hopUi;
    this.transGraph = transGraph;

    previewDataMap = new HashMap<>();
    previewLogMap = new HashMap<>();
  }

  public void showPreviewView() {

    if ( transPreviewTab == null || transPreviewTab.isDisposed() ) {
      addTransPreview();
    } else {
      transPreviewTab.dispose();

      transGraph.checkEmptyExtraView();
    }
  }

  /**
   * Add a grid with the execution metrics per step in a table view
   */
  public void addTransPreview() {

    // First, see if we need to add the extra view...
    //
    if ( transGraph.extraViewComposite == null || transGraph.extraViewComposite.isDisposed() ) {
      transGraph.addExtraView();
    } else {
      if ( transPreviewTab != null && !transPreviewTab.isDisposed() ) {
        // just set this one active and get out...
        //
        transGraph.extraViewTabFolder.setSelection( transPreviewTab );
        return;
      }
    }

    transPreviewTab = new CTabItem( transGraph.extraViewTabFolder, SWT.NONE );
    transPreviewTab.setImage( GUIResource.getInstance().getImagePreview() );
    transPreviewTab.setText( BaseMessages.getString( PKG, "HopGui.TransGraph.PreviewTab.Name" ) );

    transPreviewComposite = new Composite( transGraph.extraViewTabFolder, SWT.NONE );
    transPreviewComposite.setLayout( new FormLayout() );
    PropsUI.getInstance().setLook( transPreviewComposite, Props.WIDGET_STYLE_TOOLBAR );

    addToolBar();

    previewComposite = new Composite( transPreviewComposite, SWT.NONE );
    previewComposite.setLayout( new FillLayout() );
    FormData fdPreview = new FormData();
    fdPreview.left = new FormAttachment( 0, 0 );
    fdPreview.right = new FormAttachment( 100, 0 );
    fdPreview.top = new FormAttachment( toolbar, 0 );
    fdPreview.bottom = new FormAttachment( 100, 0 );
    previewComposite.setLayoutData( fdPreview );

    transPreviewTab.setControl( transPreviewComposite );

    transGraph.extraViewTabFolder.setSelection( transPreviewTab );

    transGraph.extraViewTabFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        refreshView();
      }
    } );
    HopGuiTransPreviewExtension extension = new HopGuiTransPreviewExtension(
      transPreviewComposite, toolbar, previewComposite );
    try {
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), "TransPreviewCreated", extension );
    } catch ( HopException ex ) {
      hopUi.getLog().logError( "Extension point call failed.", ex );
    }
  }

  private void addToolBar() {
    toolbar = new ToolBar( transPreviewComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.RIGHT | SWT.HORIZONTAL );
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fdToolBar );
    hopUi.getProps().setLook( toolbar, Props.WIDGET_STYLE_TOOLBAR );

    GuiCompositeWidgets widgets = new GuiCompositeWidgets( hopUi.getVariableSpace() );
    widgets.createCompositeWidgets( this, null, toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID, null );
    toolbar.pack();
  }


  /**
   * This refresh is driven by outside influenced using listeners and so on.
   */
  public synchronized void refreshView() {
    if ( transGraph != null && transGraph.extraViewTabFolder != null ) {
      if ( transGraph.extraViewTabFolder.getSelection() != transPreviewTab ) {
        return;
      }
    }

    if ( previewComposite == null || previewComposite.isDisposed() ) {
      return;
    }

    // Which step do we preview...
    //
    StepMeta stepMeta = selectedStep; // copy to prevent race conditions and so on.
    if ( stepMeta == null ) {
      hidePreviewGrid();
      return;
    } else {
      lastSelectedStep = selectedStep;
    }

    // Do we have a log for this selected step?
    // This means the preview work is still running or it error-ed out.
    //
    boolean errorStep = false;
    if ( transGraph.trans != null ) {

      // Get the engine metrics for the step but only copy 0
      //
      IPipelineEngine<TransMeta> engine = transGraph.trans;
      EngineMetrics engineMetrics = engine.getEngineMetrics( stepMeta.getName(), 0 );
      IEngineComponent component = engine.findComponent( stepMeta.getName(), 0 );
      Long errors = engineMetrics.getComponentMetric( component, Trans.METRIC_ERROR );

      if ( errors != null && errors > 0 ) {
        errorStep = true;
      }
    }

    String logText = previewLogMap.get( stepMeta.getName() );
    if ( errorStep && logText != null && logText.length() > 0 ) {
      showLogText( stepMeta, logText.toString() );
      return;
    }

    // If the preview work is done we have row meta-data and data for each step.
    //
    RowBuffer rowBuffer = previewDataMap.get( stepMeta.getName() );
    if ( rowBuffer != null ) {
      try {
        showPreviewGrid( transGraph.getManagedObject(), stepMeta, rowBuffer );
      } catch ( Exception e ) {
        e.printStackTrace();
        logText += Const.getStackTracker( e );
        showLogText( stepMeta, logText.toString() );
      }
    }
  }

  protected void hidePreviewGrid() {
    if ( tableView != null && !tableView.isDisposed() ) {
      tableView.dispose();
    }
  }

  protected void showPreviewGrid( TransMeta transMeta, StepMeta stepMeta, RowBuffer rowBuffer) throws HopException {
    clearPreviewComposite();

    RowMetaInterface rowMeta = rowBuffer.getRowMeta();
    List<Object[]> rowsData = rowBuffer.getBuffer();

    ColumnInfo[] columnInfo = new ColumnInfo[ rowMeta.size() ];
    for ( int i = 0; i < columnInfo.length; i++ ) {
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
      columnInfo[ i ] = new ColumnInfo( valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, false, true );
      columnInfo[ i ].setValueMeta( valueMeta );
    }

    tableView =
      new TableView( transMeta, previewComposite, SWT.NONE, columnInfo, rowsData.size(), null, PropsUI.getInstance() );

    // Put data on it...
    //
    for ( int rowNr = 0; rowNr < rowsData.size(); rowNr++ ) {
      Object[] rowData = rowsData.get( rowNr );
      TableItem item;
      if ( rowNr < tableView.table.getItemCount() ) {
        item = tableView.table.getItem( rowNr );
      } else {
        item = new TableItem( tableView.table, SWT.NONE );
      }
      for ( int colNr = 0; colNr < rowMeta.size(); colNr++ ) {
        int dataIndex = rowMeta.indexOfValue( rowMeta.getValueMeta( colNr ).getName() );
        String string;
        ValueMetaInterface valueMetaInterface;
        try {
          valueMetaInterface = rowMeta.getValueMeta( dataIndex );
          if ( valueMetaInterface.isStorageBinaryString() ) {
            Object nativeType = valueMetaInterface.convertBinaryStringToNativeType( (byte[]) rowData[ dataIndex ] );
            string = valueMetaInterface.getStorageMetadata().getString( nativeType );
          } else {
            string = rowMeta.getString( rowData, dataIndex );
          }
        } catch ( Exception e ) {
          string = "Conversion error: " + e.getMessage();
        }
        if ( string == null ) {
          item.setText( colNr + 1, "<null>" );
          item.setForeground( colNr + 1, GUIResource.getInstance().getColorBlue() );
        } else {
          item.setText( colNr + 1, string );
        }
      }
    }

    tableView.setRowNums();
    tableView.setShowingConversionErrorsInline( true );
    tableView.optWidth( true );

    previewComposite.layout( true, true );
  }

  protected void showLogText( StepMeta stepMeta, String loggingText ) {
    clearPreviewComposite();

    logText = new Text( previewComposite, SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL );
    logText.setText( loggingText );

    previewComposite.layout( true, true );
  }

  private void clearPreviewComposite() {
    // First clear out the preview composite, then put in a text field showing the log text
    //
    //
    for ( Control control : previewComposite.getChildren() ) {
      control.dispose();
    }
  }

  public CTabItem getTransGridTab() {
    return transPreviewTab;
  }

  public void capturePreviewData( final IPipelineEngine<TransMeta> pipelineEngine, List<StepMeta> stepMetas ) {

    // First clean out previous preview data. Otherwise this method leaks memory like crazy.
    //
    previewLogMap.clear();
    previewDataMap.clear();

    try {
      for ( final StepMeta stepMeta : stepMetas ) {
        pipelineEngine.retrieveComponentOutput( stepMeta.getName(), 0, PropsUI.getInstance().getDefaultPreviewSize(),
          (pipeline, rowBuffer)-> previewDataMap.put(stepMeta.getName(), rowBuffer)
        );
      }
    } catch ( Exception e ) {
      pipelineEngine.getLogChannel().logError( "Error capturing preview data: ", e );
    }

    // In case there were errors during preview...
    //
    try {
      pipelineEngine.addFinishedListener( ( pe ) -> {
        // Copy over the data from the previewDelegate...
        //
        pe.getEngineMetrics();
        if ( pe.getErrors() != 0 ) {
          // capture logging and store it...
          //
          for ( IEngineComponent component : pipelineEngine.getComponents() ) {
            if ( component.getCopyNr() == 0 ) {
              previewLogMap.put( component.getName(), component.getLogText() );
            }
          }
        }
      } );
    } catch(HopException e) {
      pipelineEngine.getLogChannel().logError( "Error getting logging from execution: ", e );
    }
  }

  public void addPreviewData( StepMeta stepMeta, RowMetaInterface rowMeta, List<Object[]> rowsData,
                              StringBuffer buffer ) {
    previewLogMap.put( stepMeta.getName(), buffer.toString() );
    previewDataMap.put( stepMeta.getName(), new RowBuffer(rowMeta, rowsData) );
  }

  /**
   * @return the selectedStep
   */
  public StepMeta getSelectedStep() {
    return selectedStep;
  }

  /**
   * @param selectedStep the selectedStep to set
   */
  public void setSelectedStep( StepMeta selectedStep ) {
    this.selectedStep = selectedStep;
  }
}
