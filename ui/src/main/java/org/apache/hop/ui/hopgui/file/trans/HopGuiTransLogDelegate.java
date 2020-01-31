/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopgui.file.trans;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopui.HopUi;
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

import java.util.ArrayList;
import java.util.Map;

@GuiPlugin(
  id = "HopGuiTransLogDelegate"
)public class HopGuiTransLogDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiTransLogDelegate-ToolBar";

  private HopGuiTransGraph transGraph;

  private HopGui hopUi;
  private CTabItem transLogTab;

  private StyledText transLogText;

  private ToolBar toolbar;

  private Composite transLogComposite;

  private HopGuiLogBrowser logBrowser;

  /**
   * @param hopUi
   */
  public HopGuiTransLogDelegate( HopGui hopUi, HopGuiTransGraph transGraph ) {
    this.hopUi = hopUi;
    this.transGraph = transGraph;
  }

  public void addTransLog() {
    // First, see if we need to add the extra view...
    //
    if ( transGraph.extraViewComposite == null || transGraph.extraViewComposite.isDisposed() ) {
      transGraph.addExtraView();
    } else {
      if ( transLogTab != null && !transLogTab.isDisposed() ) {
        // just set this one active and get out...
        //
        transGraph.extraViewTabFolder.setSelection( transLogTab );
        return;
      }
    }

    // Add a transLogTab : display the logging...
    //
    transLogTab = new CTabItem( transGraph.extraViewTabFolder, SWT.NONE );
    transLogTab.setImage( GUIResource.getInstance().getImageShowLog() );
    transLogTab.setText( BaseMessages.getString( PKG, "Spoon.TransGraph.LogTab.Name" ) );

    transLogComposite = new Composite( transGraph.extraViewTabFolder, SWT.NO_BACKGROUND | SWT.NO_FOCUS );
    transLogComposite.setLayout( new FormLayout() );

    addToolBar();

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fd );

    transLogText = new StyledText( transLogComposite, SWT.READ_ONLY | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL );
    hopUi.getProps().setLook( transLogText );
    FormData fdText = new FormData();
    fdText.left = new FormAttachment( 0, 0 );
    fdText.right = new FormAttachment( 100, 0 );
    fdText.top = new FormAttachment( toolbar, 0 );
    fdText.bottom = new FormAttachment( 100, 0 );
    transLogText.setLayoutData( fdText );

    logBrowser = new HopGuiLogBrowser( transLogText, transGraph );
    logBrowser.installLogSniffer();

    // If the transformation is closed, we should dispose of all the logging information in the buffer and registry for
    // this transformation
    //
    transGraph.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent event ) {
        if ( transGraph.trans != null ) {
          HopLogStore.discardLines( transGraph.trans.getLogChannelId(), true );
        }
      }
    } );

    transLogTab.setControl( transLogComposite );

    transGraph.extraViewTabFolder.setSelection( transLogTab );
  }

  private void addToolBar() {
    toolbar = new ToolBar( transLogComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.RIGHT | SWT.VERTICAL );
    FormData fdToolBar = new FormData(  );
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fdToolBar );
    hopUi.getProps().setLook( toolbar, Props.WIDGET_STYLE_TOOLBAR );

    GuiCompositeWidgets widgets = new GuiCompositeWidgets( hopUi.getVariableSpace() );
    widgets.createCompositeWidgets( this, null, toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID, null );
    toolbar.pack();
  }

  public void showLogView() {

    // What button?
    //
    // XulToolbarButton showLogXulButton = toolbar.getButtonById("trans-show-log");
    // ToolItem toolBarButton = (ToolItem) showLogXulButton.getNativeObject();

    if ( transLogTab == null || transLogTab.isDisposed() ) {
      addTransLog();
    } else {
      transLogTab.dispose();

      transGraph.checkEmptyExtraView();
    }

    // spoon.addTransLog(transMeta);
  }

  public void showLogSettings() {
    // TODO: hopUi.setLog();
  }

  public void clearLog() {
    if ( transLogText != null && !transLogText.isDisposed() ) {
      transLogText.setText( "" );
    }
    Map<StepMeta, String> stepLogMap = transGraph.getStepLogMap();
    if ( stepLogMap != null ) {
      stepLogMap.clear();
      transGraph.getDisplay().asyncExec( new Runnable() {
        public void run() {
          transGraph.redraw();
        }
      } );
    }
  }

  public void showErrors() {
    String all = transLogText.getText();
    ArrayList<String> err = new ArrayList<>();

    int i = 0;
    int startpos = 0;
    int crlen = Const.CR.length();

    while ( i < all.length() - crlen ) {
      if ( all.substring( i, i + crlen ).equalsIgnoreCase( Const.CR ) ) {
        String line = all.substring( startpos, i );
        String uLine = line.toUpperCase();
        if ( uLine.indexOf( BaseMessages.getString( PKG, "TransLog.System.ERROR" ) ) >= 0
          || uLine.indexOf( BaseMessages.getString( PKG, "TransLog.System.EXCEPTION" ) ) >= 0
          || uLine.indexOf( "ERROR" ) >= 0 || // i18n for compatibilty to non translated steps a.s.o.
          uLine.indexOf( "EXCEPTION" ) >= 0 // i18n for compatibilty to non translated steps a.s.o.
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
    if ( uLine.indexOf( BaseMessages.getString( PKG, "TransLog.System.ERROR2" ) ) >= 0
      || uLine.indexOf( BaseMessages.getString( PKG, "TransLog.System.EXCEPTION2" ) ) >= 0
      || uLine.indexOf( "ERROR" ) >= 0 || // i18n for compatibilty to non translated steps a.s.o.
      uLine.indexOf( "EXCEPTION" ) >= 0 // i18n for compatibilty to non translated steps a.s.o.
    ) {
      err.add( line );
    }

    if ( err.size() > 0 ) {
      String[] err_lines = new String[ err.size() ];
      for ( i = 0; i < err_lines.length; i++ ) {
        err_lines[ i ] = err.get( i );
      }

      EnterSelectionDialog esd = new EnterSelectionDialog( transGraph.getShell(), err_lines,
        BaseMessages.getString( PKG, "TransLog.Dialog.ErrorLines.Title" ),
        BaseMessages.getString( PKG, "TransLog.Dialog.ErrorLines.Message" ) );
      line = esd.open();
      if ( line != null ) {
        TransMeta transMeta = transGraph.getManagedObject();
        for ( i = 0; i < transMeta.nrSteps(); i++ ) {
          StepMeta stepMeta = transMeta.getStep( i );
          if ( line.indexOf( stepMeta.getName() ) >= 0 ) {
            transGraph.editStep(transMeta, stepMeta);
          }
        }
      }
    }
  }

  /**
   * @return the transLogTab
   */
  public CTabItem getTransLogTab() {
    return transLogTab;
  }

  public String getLoggingText() {
    if ( transLogText != null && !transLogText.isDisposed() ) {
      return transLogText.getText();
    } else {
      return null;
    }

  }

  public void pauseLog() {
    /** TODO: Implement through GuiElement
     *
    XulToolbarbutton pauseContinueButton = (XulToolbarbutton) toolbar.getElementById( "log-pause" );
    ToolItem swtToolItem = (ToolItem) pauseContinueButton.getManagedObject();

    if ( logBrowser.isPaused() ) {
      logBrowser.setPaused( false );
      if ( pauseContinueButton != null ) {
        swtToolItem.setImage( GUIResource.getInstance().getImagePauseLog() );
      }
    } else {
      logBrowser.setPaused( true );
      if ( pauseContinueButton != null ) {
        swtToolItem.setImage( GUIResource.getInstance().getImageContinueLog() );
      }
    }
     */
  }

  public HopGuiLogBrowser getLogBrowser() {
    return logBrowser;
  }

  public boolean hasSelectedText() {
    return transLogText!=null && !transLogText.isDisposed() && StringUtils.isNotEmpty(transLogText.getSelectionText());
  }

  public void copySelected() {
    if (hasSelectedText()) {
      transGraph.transClipboardDelegate.toClipboard( transLogText.getSelectionText() );
    }
  }
}
