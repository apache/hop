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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelinePreviewExtension;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@GuiPlugin(
  description = "The pipeline preview tab in the execution pane"
)
public class HopGuiPipelinePreviewDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiPipelinePreviewDelegate-ToolBar";

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;

  private CTabItem pipelinePreviewTab;

  private ToolBar toolbar;
  private Composite pipelinePreviewComposite;

  protected Map<String, RowBuffer> previewDataMap;
  protected Map<String, String> previewLogMap;
  private Composite previewComposite;

  private Text logText;
  private TableView tableView;

  private TransformMeta selectedTransform;
  protected TransformMeta lastSelectedTransform;

  /**
   * @param hopGui
   * @param pipelineGraph
   */
  public HopGuiPipelinePreviewDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;

    previewDataMap = new HashMap<>();
    previewLogMap = new HashMap<>();
  }

  public void showPreviewView() {

    if ( pipelinePreviewTab == null || pipelinePreviewTab.isDisposed() ) {
      addPipelinePreview();
    } else {
      pipelinePreviewTab.dispose();

      pipelineGraph.checkEmptyExtraView();
    }
  }

  /**
   * Add a grid with the execution metrics per transform in a table view
   */
  public void addPipelinePreview() {

    // First, see if we need to add the extra view...
    //
    if ( pipelineGraph.extraViewComposite == null || pipelineGraph.extraViewComposite.isDisposed() ) {
      pipelineGraph.addExtraView();
    } else {
      if ( pipelinePreviewTab != null && !pipelinePreviewTab.isDisposed() ) {
        // just set this one active and get out...
        //
        pipelineGraph.extraViewTabFolder.setSelection( pipelinePreviewTab );
        return;
      }
    }

    pipelinePreviewTab = new CTabItem( pipelineGraph.extraViewTabFolder, SWT.NONE );
    pipelinePreviewTab.setImage( GuiResource.getInstance().getImagePreview() );
    pipelinePreviewTab.setText( BaseMessages.getString( PKG, "HopGui.PipelineGraph.PreviewTab.Name" ) );

    pipelinePreviewComposite = new Composite( pipelineGraph.extraViewTabFolder, SWT.NONE );
    pipelinePreviewComposite.setLayout( new FormLayout() );
    PropsUi.getInstance().setLook( pipelinePreviewComposite, Props.WIDGET_STYLE_TOOLBAR );

    addToolBar();

    previewComposite = new Composite( pipelinePreviewComposite, SWT.NONE );
    previewComposite.setLayout( new FillLayout() );
    FormData fdPreview = new FormData();
    fdPreview.left = new FormAttachment( 0, 0 );
    fdPreview.right = new FormAttachment( 100, 0 );
    fdPreview.top = new FormAttachment( toolbar, 0 );
    fdPreview.bottom = new FormAttachment( 100, 0 );
    previewComposite.setLayoutData( fdPreview );

    pipelinePreviewTab.setControl( pipelinePreviewComposite );

    pipelineGraph.extraViewTabFolder.setSelection( pipelinePreviewTab );

    pipelineGraph.extraViewTabFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        refreshView();
      }
    } );
    HopGuiPipelinePreviewExtension extension = new HopGuiPipelinePreviewExtension(
      pipelinePreviewComposite, toolbar, previewComposite );
    try {
      ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), "PipelinePreviewCreated", extension );
    } catch ( HopException ex ) {
      hopGui.getLog().logError( "Extension point call failed.", ex );
    }
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiPipelinePreviewDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiPipelineGraph ) {
      HopGuiPipelineGraph graph = (HopGuiPipelineGraph) fileTypeHandler;
      return graph.pipelinePreviewDelegate;
    }
    return null;
  }

  private void addToolBar() {
    toolbar = new ToolBar( pipelinePreviewComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.RIGHT | SWT.HORIZONTAL );
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fdToolBar );
    hopGui.getProps().setLook( toolbar, Props.WIDGET_STYLE_TOOLBAR );

    GuiToolbarWidgets widgets = new GuiToolbarWidgets( );
    widgets.createToolbarWidgets( toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID );
    toolbar.pack();
  }


  /**
   * This refresh is driven by outside influenced using listeners and so on.
   */
  public synchronized void refreshView() {
    if ( pipelineGraph != null && pipelineGraph.extraViewTabFolder != null ) {
      if ( pipelineGraph.extraViewTabFolder.getSelection() != pipelinePreviewTab ) {
        return;
      }
    }

    if ( previewComposite == null || previewComposite.isDisposed() ) {
      return;
    }

    // Which transform do we preview...
    //
    TransformMeta transformMeta = selectedTransform; // copy to prevent race conditions and so on.
    if ( transformMeta == null ) {
      hidePreviewGrid();
      return;
    } else {
      lastSelectedTransform = selectedTransform;
    }

    // Do we have a log for this selected transform?
    // This means the preview work is still running or it error-ed out.
    //
    boolean errorTransform = false;
    if ( pipelineGraph.pipeline != null ) {

      // Get the engine metrics for the transform but only copy 0
      //
      IPipelineEngine<PipelineMeta> engine = pipelineGraph.pipeline;
      EngineMetrics engineMetrics = engine.getEngineMetrics( transformMeta.getName(), 0 );
      IEngineComponent component = engine.findComponent( transformMeta.getName(), 0 );
      Long errors = engineMetrics.getComponentMetric( component, Pipeline.METRIC_ERROR );

      if ( errors != null && errors > 0 ) {
        errorTransform = true;
      }
    }

    String logText = previewLogMap.get( transformMeta.getName() );
    if ( errorTransform && logText != null && logText.length() > 0 ) {
      showLogText( transformMeta, logText.toString() );
      return;
    }

    // If the preview work is done we have row meta-data and data for each transform.
    //
    RowBuffer rowBuffer = previewDataMap.get( transformMeta.getName() );
    if ( rowBuffer != null ) {
      try {
        showPreviewGrid( pipelineGraph.getManagedObject(), transformMeta, rowBuffer );
      } catch ( Exception e ) {
        e.printStackTrace();
        logText += Const.getStackTracker( e );
        showLogText( transformMeta, logText.toString() );
      }
    }
  }

  protected void hidePreviewGrid() {
    if ( tableView != null && !tableView.isDisposed() ) {
      tableView.dispose();
    }
  }

  protected void showPreviewGrid( PipelineMeta pipelineMeta, TransformMeta transformMeta, RowBuffer rowBuffer) throws HopException {
    clearPreviewComposite();

    IRowMeta rowMeta = rowBuffer.getRowMeta();
    List<Object[]> rowsData = rowBuffer.getBuffer();

    ColumnInfo[] columnInfo = new ColumnInfo[ rowMeta.size() ];
    for ( int i = 0; i < columnInfo.length; i++ ) {
      IValueMeta valueMeta = rowMeta.getValueMeta( i );
      columnInfo[ i ] = new ColumnInfo( valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, false, true );
      columnInfo[ i ].setValueMeta( valueMeta );
    }

    tableView =
      new TableView( pipelineMeta, previewComposite, SWT.NONE, columnInfo, rowsData.size(), null, PropsUi.getInstance() );

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
        IValueMeta iValueMeta;
        try {
          iValueMeta = rowMeta.getValueMeta( dataIndex );
          if ( iValueMeta.isStorageBinaryString() ) {
            Object nativeType = iValueMeta.convertBinaryStringToNativeType( (byte[]) rowData[ dataIndex ] );
            string = iValueMeta.getStorageMetadata().getString( nativeType );
          } else {
            string = rowMeta.getString( rowData, dataIndex );
          }
        } catch ( Exception e ) {
          string = "Conversion error: " + e.getMessage();
        }
        if ( string == null ) {
          item.setText( colNr + 1, "<null>" );
          item.setForeground( colNr + 1, GuiResource.getInstance().getColorBlue() );
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

  protected void showLogText( TransformMeta transformMeta, String loggingText ) {
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

  public CTabItem getPipelineGridTab() {
    return pipelinePreviewTab;
  }

  public void capturePreviewData( final IPipelineEngine<PipelineMeta> pipelineEngine, List<TransformMeta> transformMetas ) {

    // First clean out previous preview data. Otherwise this method leaks memory like crazy.
    //
    previewLogMap.clear();
    previewDataMap.clear();

    if (pipelineEngine.getEngineCapabilities().isSupportingPreview()) {
      try {
        for ( final TransformMeta transformMeta : transformMetas ) {
          pipelineEngine.retrieveComponentOutput( transformMeta.getName(), 0, PropsUi.getInstance().getDefaultPreviewSize(),
            ( pipeline, rowBuffer ) -> previewDataMap.put( transformMeta.getName(), rowBuffer )
          );
        }
      } catch ( Exception e ) {
        pipelineEngine.getLogChannel().logError( "Error capturing preview data: ", e );
      }

      // In case there were errors during preview...
      //
      try {
        pipelineEngine.addExecutionFinishedListener( pipeline -> {
          // Copy over the data from the previewDelegate...
          //
          pipeline.getEngineMetrics();
          if ( pipeline.getErrors() != 0 ) {
            // capture logging and store it...
            //
            for ( IEngineComponent component : pipelineEngine.getComponents() ) {
              if ( component.getCopyNr() == 0 ) {
                previewLogMap.put( component.getName(), component.getLogText() );
              }
            }
          }
        } );
      } catch ( HopException e ) {
        pipelineEngine.getLogChannel().logError( "Error getting logging from execution: ", e );
      }
    }
  }

  public void addPreviewData( TransformMeta transformMeta, IRowMeta rowMeta, List<Object[]> rowsData,
                              StringBuffer buffer ) {
    previewLogMap.put( transformMeta.getName(), buffer.toString() );
    previewDataMap.put( transformMeta.getName(), new RowBuffer(rowMeta, rowsData) );
  }

  /**
   * @return the selectedTransform
   */
  public TransformMeta getSelectedTransform() {
    return selectedTransform;
  }

  /**
   * @param selectedTransform the selectedTransform to set
   */
  public void setSelectedTransform( TransformMeta selectedTransform ) {
    this.selectedTransform = selectedTransform;
  }
}
