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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiLogBrowser;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.util.ArrayList;
import java.util.Map;

@GuiPlugin
public class HopGuiPipelineLogDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiPipelineLogDelegate-ToolBar";
  public static final String TOOLBAR_ICON_CLEAR_LOG_VIEW = "ToolbarIcon-10000-ClearLog";
  public static final String TOOLBAR_ICON_SHOW_ERROR_LINES = "ToolbarIcon-10010-ShowErrorLines";
  public static final String TOOLBAR_ICON_LOG_SETTINGS = "ToolbarIcon-10020-LogSettings";
  public static final String TOOLBAR_ICON_LOG_PAUSE_RESUME = "ToolbarIcon-10030-LogPauseResume";

  private HopGuiPipelineGraph pipelineGraph;

  private HopGui hopUi;
  private CTabItem pipelineLogTab;

  private StyledText pipelineLogText;

  private ToolBar toolbar;
  private GuiCompositeWidgets toolBarWidgets;

  private Composite pipelineLogComposite;

  private HopGuiLogBrowser logBrowser;

  /**
   * @param hopUi
   */
  public HopGuiPipelineLogDelegate( HopGui hopUi, HopGuiPipelineGraph pipelineGraph ) {
    this.hopUi = hopUi;
    this.pipelineGraph = pipelineGraph;
  }

  public void addPipelineLog() {
    // First, see if we need to add the extra view...
    //
    if ( pipelineGraph.extraViewComposite == null || pipelineGraph.extraViewComposite.isDisposed() ) {
      pipelineGraph.addExtraView();
    } else {
      if ( pipelineLogTab != null && !pipelineLogTab.isDisposed() ) {
        // just set this one active and get out...
        //
        pipelineGraph.extraViewTabFolder.setSelection( pipelineLogTab );
        return;
      }
    }

    // Add a pipelineLogTab : display the logging...
    //
    pipelineLogTab = new CTabItem( pipelineGraph.extraViewTabFolder, SWT.NONE );
    pipelineLogTab.setImage( GuiResource.getInstance().getImageShowLog() );
    pipelineLogTab.setText( BaseMessages.getString( PKG, "HopGui.PipelineGraph.LogTab.Name" ) );

    pipelineLogComposite = new Composite( pipelineGraph.extraViewTabFolder, SWT.NO_BACKGROUND | SWT.NO_FOCUS );
    pipelineLogComposite.setLayout( new FormLayout() );

    addToolBar();

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fd );

    pipelineLogText = new StyledText( pipelineLogComposite, SWT.READ_ONLY | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL );
    hopUi.getProps().setLook( pipelineLogText );
    FormData fdText = new FormData();
    fdText.left = new FormAttachment( 0, 0 );
    fdText.right = new FormAttachment( 100, 0 );
    fdText.top = new FormAttachment( toolbar, 0 );
    fdText.bottom = new FormAttachment( 100, 0 );
    pipelineLogText.setLayoutData( fdText );

    logBrowser = new HopGuiLogBrowser( pipelineLogText, pipelineGraph );
    logBrowser.installLogSniffer();

    // If the pipeline is closed, we should dispose of all the logging information in the buffer and registry for
    // this pipeline
    //
    pipelineGraph.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent event ) {
        if ( pipelineGraph.pipeline != null ) {
          HopLogStore.discardLines( pipelineGraph.pipeline.getLogChannelId(), true );
        }
      }
    } );

    pipelineLogTab.setControl( pipelineLogComposite );

    pipelineGraph.extraViewTabFolder.setSelection( pipelineLogTab );
  }

  private void addToolBar() {
    toolbar = new ToolBar( pipelineLogComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL );
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fdToolBar );
    hopUi.getProps().setLook( toolbar, Props.WIDGET_STYLE_TOOLBAR );

    toolBarWidgets = new GuiCompositeWidgets( hopUi.getVariables() );
    toolBarWidgets.createCompositeWidgets( this, null, toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID, null );
    toolbar.pack();
  }

  public void showLogView() {
    if ( pipelineLogTab == null || pipelineLogTab.isDisposed() ) {
      addPipelineLog();
    } else {
      pipelineLogTab.dispose();

      pipelineGraph.checkEmptyExtraView();
    }

    // spoon.addPipelineLog(pipelineMeta);
  }

  @GuiToolbarElement(
    id = TOOLBAR_ICON_CLEAR_LOG_VIEW,
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "PipelineLog.Button.ClearLog",
    toolTip = "PipelineLog.Button.ClearLog",
    i18nPackageClass = HopGui.class,
    image = "ui/images/trash.svg"
  )
  public void clearLog() {
    if ( pipelineLogText != null && !pipelineLogText.isDisposed() ) {
      pipelineLogText.setText( "" );
    }
    Map<String, String> transformLogMap = pipelineGraph.getTransformLogMap();
    if ( transformLogMap != null ) {
      transformLogMap.clear();
      pipelineGraph.getDisplay().asyncExec( new Runnable() {
        public void run() {
          pipelineGraph.redraw();
        }
      } );
    }
  }

  @GuiToolbarElement(
    id = TOOLBAR_ICON_LOG_SETTINGS,
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "PipelineLog.Button.LogSettings",
    toolTip = "PipelineLog.Button.LogSettings",
    i18nPackageClass = HopGui.class,
    image = "ui/images/log-settings.svg"
  )
  public void showLogSettings() {
    // TODO: implement or rethink
  }

  @GuiToolbarElement(
    id = TOOLBAR_ICON_SHOW_ERROR_LINES,
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "PipelineLog.Button.ShowErrorLines",
    toolTip = "PipelineLog.Button.ShowErrorLines",
    i18nPackageClass = HopGui.class,
    image = "ui/images/show-error-lines.svg"
  )
  public void showErrors() {
    String all = pipelineLogText.getText();
    ArrayList<String> err = new ArrayList<>();

    int i = 0;
    int startpos = 0;
    int crlen = Const.CR.length();

    while ( i < all.length() - crlen ) {
      if ( all.substring( i, i + crlen ).equalsIgnoreCase( Const.CR ) ) {
        String line = all.substring( startpos, i );
        String uLine = line.toUpperCase();
        if ( uLine.indexOf( BaseMessages.getString( PKG, "PipelineLog.System.ERROR" ) ) >= 0
          || uLine.indexOf( BaseMessages.getString( PKG, "PipelineLog.System.EXCEPTION" ) ) >= 0
          || uLine.indexOf( "ERROR" ) >= 0 || // i18n for compatibilty to non translated transforms a.s.o.
          uLine.indexOf( "EXCEPTION" ) >= 0 // i18n for compatibilty to non translated transforms a.s.o.
        ) {
          err.add( line );
        }
        // New start of line
        startpos = i + crlen;
      }

      i++;
    }
    String line = all.substring( startpos );
    String uLine = line.toUpperCase();
    if ( uLine.indexOf( BaseMessages.getString( PKG, "PipelineLog.System.ERROR2" ) ) >= 0
      || uLine.indexOf( BaseMessages.getString( PKG, "PipelineLog.System.EXCEPTION2" ) ) >= 0
      || uLine.indexOf( "ERROR" ) >= 0 || // i18n for compatibilty to non translated transforms a.s.o.
      uLine.indexOf( "EXCEPTION" ) >= 0 // i18n for compatibilty to non translated transforms a.s.o.
    ) {
      err.add( line );
    }

    if ( err.size() > 0 ) {
      String[] err_lines = new String[ err.size() ];
      for ( i = 0; i < err_lines.length; i++ ) {
        err_lines[ i ] = err.get( i );
      }

      EnterSelectionDialog esd = new EnterSelectionDialog( pipelineGraph.getShell(), err_lines,
        BaseMessages.getString( PKG, "PipelineLog.Dialog.ErrorLines.Title" ),
        BaseMessages.getString( PKG, "PipelineLog.Dialog.ErrorLines.Message" ) );
      line = esd.open();
      if ( line != null ) {
        PipelineMeta pipelineMeta = pipelineGraph.getManagedObject();
        for ( i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
          TransformMeta transformMeta = pipelineMeta.getTransform( i );
          if ( line.indexOf( transformMeta.getName() ) >= 0 ) {
            pipelineGraph.editTransform( pipelineMeta, transformMeta );
          }
        }
      }
    }
  }

  /**
   * @return the pipelineLogTab
   */
  public CTabItem getPipelineLogTab() {
    return pipelineLogTab;
  }

  public String getLoggingText() {
    if ( pipelineLogText != null && !pipelineLogText.isDisposed() ) {
      return pipelineLogText.getText();
    } else {
      return null;
    }

  }

  @GuiToolbarElement(
    id = TOOLBAR_ICON_LOG_PAUSE_RESUME,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "JobLog.Button.Pause",
    toolTip = "JobLog.Button.Pause",
    i18nPackageClass = HopGui.class,
    image = "ui/images/pause-log.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    separator = true
  )
  public void pauseLog() {
    ToolItem item = toolBarWidgets.findToolItem( TOOLBAR_ICON_LOG_PAUSE_RESUME );
    if ( logBrowser.isPaused() ) {
      logBrowser.setPaused( false );
      item.setImage( GuiResource.getInstance().getImageContinueLog() );
    } else {
      logBrowser.setPaused( true );
      item.setImage( GuiResource.getInstance().getImagePauseLog() );
    }
  }

  public HopGuiLogBrowser getLogBrowser() {
    return logBrowser;
  }

  public boolean hasSelectedText() {
    return pipelineLogText != null && !pipelineLogText.isDisposed() && StringUtils.isNotEmpty( pipelineLogText.getSelectionText() );
  }

  public void copySelected() {
    if ( hasSelectedText() ) {
      pipelineGraph.pipelineClipboardDelegate.toClipboard( pipelineLogText.getSelectionText() );
    }
  }
}
