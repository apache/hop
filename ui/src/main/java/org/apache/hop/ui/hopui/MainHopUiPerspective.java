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

package org.apache.hop.ui.hopui;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.gui.HopUiFactory;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.hopui.trans.TransGraph;
import org.apache.hop.ui.util.ImageUtil;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.apache.xul.swt.tab.TabItem;
import org.apache.xul.swt.tab.TabSet;

public class MainHopUiPerspective implements HopUiPerspectiveImageProvider {

  public static final String ID = "001-spoon-jobs";

  private Composite ui;
  private List<HopUiPerspectiveListener> listeners = new ArrayList<HopUiPerspectiveListener>();
  private TabSet tabfolder;
  private static final Class<?> PKG = HopUi.class;

  public MainHopUiPerspective( Composite ui, TabSet tabfolder ) {
    this.ui = ui;
    this.tabfolder = tabfolder;
  }

  // Default perspective to support Jobs and Transformations
  @Override
  public String getId() {
    return ID;
  }

  @Override
  public String getDisplayName( Locale l ) {
    return BaseMessages.getString( PKG, "Spoon.Perspectives.DI" );
  }

  @Override
  public InputStream getPerspectiveIcon() {
    return ImageUtil.getImageInputStream( Display.getCurrent(), "ui/images/transformation.png" );
  }

  @Override
  public String getPerspectiveIconPath() {
    return "ui/images/transformation.svg";
  }

  @Override
  public Composite getUI() {
    return ui;
  }

  @Override
  public void setActive( boolean active ) {
    for ( HopUiPerspectiveListener l : listeners ) {
      if ( active ) {
        l.onActivation();
        HopUi.getInstance().enableMenus();
      } else {
        l.onDeactication();
      }
    }
  }

  @Override
  public List<XulEventHandler> getEventHandlers() {
    return null;
  }

  @Override
  public List<XulOverlay> getOverlays() {
    return Collections.singletonList( (XulOverlay) new DefaultXulOverlay( "ui/main_perspective_overlay.xul" ) );
  }

  @Override
  public void addPerspectiveListener( HopUiPerspectiveListener listener ) {
    if ( listeners.contains( listener ) == false ) {
      listeners.add( listener );
    }
  }

  @Override
  public EngineMetaInterface getActiveMeta() {

    if ( tabfolder == null ) {
      return null;
    }

    TabItem tabItem = tabfolder.getSelected();
    if ( tabItem == null ) {
      return null;
    }

    // What transformation is in the active tab?
    // TransLog, TransGraph & TransHist contain the same transformation
    //
    TabMapEntry mapEntry = ( (HopUi) HopUiFactory.getInstance() ).delegates.tabs.getTab( tabfolder.getSelected() );
    EngineMetaInterface meta = null;
    if ( mapEntry != null ) {
      if ( mapEntry.getObject() instanceof TransGraph ) {
        meta = ( mapEntry.getObject() ).getMeta();
      }
      if ( mapEntry.getObject() instanceof JobGraph ) {
        meta = ( mapEntry.getObject() ).getMeta();
      }
    }

    return meta;

  }

  void setTabset( TabSet tabs ) {
    this.tabfolder = tabs;
  }

}
