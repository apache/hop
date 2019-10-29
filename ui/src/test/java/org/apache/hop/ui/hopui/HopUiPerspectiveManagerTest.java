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

import org.eclipse.swt.widgets.Composite;
import org.junit.Before;
import org.junit.Test;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulToolbar;
import org.pentaho.ui.xul.containers.XulVbox;
import org.pentaho.ui.xul.impl.XulEventHandler;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.Mockito.*;


public class HopUiPerspectiveManagerTest {
  private static final String PERSPECTIVE_ID = "perspective-id";
  private static final String PERSPECTIVE_NAME = "perspective-name";

  private Map<HopUiPerspective, HopUiPerspectiveManager.PerspectiveManager> perspectiveManagerMap;
  private static HopUiPerspectiveManager hopUiPerspectiveManager;
  private HopUiPerspective perspective;

  @Before
  public void setUp() throws Exception {
    hopUiPerspectiveManager = HopUiPerspectiveManager.getInstance();
    hopUiPerspectiveManager = spy( hopUiPerspectiveManager );

    perspective = new DummyPerspective();
    hopUiPerspectiveManager.addPerspective( perspective );

    // emulate we have one perspective, that is not inited yet.
    perspectiveManagerMap = emulatePerspectiveManagerMap( perspective );

    doReturn( perspectiveManagerMap ).when( hopUiPerspectiveManager ).getPerspectiveManagerMap();
    doReturn( mock( HopUi.class ) ).when( hopUiPerspectiveManager ).getSpoon();
    hopUiPerspectiveManager.setDeck( mock( XulDeck.class ) );
    doReturn( mock( LogChannelInterface.class ) ).when( hopUiPerspectiveManager ).getLogger();
  }


  @Test
  public void perspectiveIsInitializedOnlyOnce() throws HopException {
    HopUiPerspectiveManager.PerspectiveManager perspectiveManager = perspectiveManagerMap.get( perspective );

    hopUiPerspectiveManager.activatePerspective( perspective.getClass() );
    // it's the first time this perspective gets active, so it should be initialized after this call
    verify( perspectiveManager ).performInit();

    hopUiPerspectiveManager.activatePerspective( perspective.getClass() );
    // make sure that perspective was inited only after first activation
    verify( perspectiveManager ).performInit();
  }


  @Test
  public void hidePerspective() {
    HopUiPerspectiveManager.PerspectiveManager perspectiveManager = perspectiveManagerMap.get( perspective );
    hopUiPerspectiveManager.hidePerspective( perspective.getId() );

    verify( perspectiveManager ).setPerspectiveHidden( PERSPECTIVE_NAME, true );
  }

  @Test
  public void showPerspective() {
    HopUiPerspectiveManager.PerspectiveManager perspectiveManager = perspectiveManagerMap.get( perspective );
    hopUiPerspectiveManager.showPerspective( perspective.getId() );

    verify( perspectiveManager ).setPerspectiveHidden( PERSPECTIVE_NAME, false );
  }



  private Map<HopUiPerspective, HopUiPerspectiveManager.PerspectiveManager> emulatePerspectiveManagerMap(
    HopUiPerspective... perspectives ) {
    Map<HopUiPerspective, HopUiPerspectiveManager.PerspectiveManager> spoonPerspectiveManagerMap = new HashMap<>();

    for ( HopUiPerspective perspective : perspectives ) {
      spoonPerspectiveManagerMap.put( perspective, createPerspectiveManager( perspective ) );
    }
    return spoonPerspectiveManagerMap;
  }

  private HopUiPerspectiveManager.PerspectiveManager createPerspectiveManager( HopUiPerspective perspective ) {
    List<HopUiPerspectiveManager.PerspectiveData> perspectiveDatas = new ArrayList<HopUiPerspectiveManager.PerspectiveData>();
    perspectiveDatas.add( new HopUiPerspectiveManager.PerspectiveData( PERSPECTIVE_NAME, PERSPECTIVE_ID ) );
    HopUiPerspectiveManager.PerspectiveManager perspectiveManager =
      new HopUiPerspectiveManager.PerspectiveManager( perspective, mock( XulVbox.class ), mock( XulToolbar.class ),
        perspectiveDatas, perspective.getDisplayName( Locale.getDefault() ) );

    perspectiveManager = spy( perspectiveManager );
    doNothing().when( perspectiveManager ).performInit();

    return perspectiveManager;
  }


  private class DummyPerspective implements HopUiPerspective {

    @Override public String getId() {
      return PERSPECTIVE_ID;
    }

    @Override public Composite getUI() {
      return null;
    }

    @Override public String getDisplayName( Locale l ) {
      return PERSPECTIVE_NAME;
    }

    @Override public InputStream getPerspectiveIcon() {
      return null;
    }

    @Override public void setActive( boolean active ) {

    }

    @Override public List<XulOverlay> getOverlays() {
      return null;
    }

    @Override public List<XulEventHandler> getEventHandlers() {
      return null;
    }

    @Override public void addPerspectiveListener( HopUiPerspectiveListener listener ) {

    }

    @Override public EngineMetaInterface getActiveMeta() {
      return null;
    }
  }

}
