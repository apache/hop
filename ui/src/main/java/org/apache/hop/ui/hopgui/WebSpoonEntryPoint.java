/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 * Copyright (C) 2016-2018 by Hitachi America, Ltd., R&D : http://www.hitachi-america.us/rd/
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

package org.apache.hop.ui.hopgui;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.WebSpoonUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.application.AbstractEntryPoint;
import org.eclipse.rap.rwt.client.service.ExitConfirmation;
import org.eclipse.rap.rwt.client.service.StartupParameters;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;

public class WebSpoonEntryPoint extends AbstractEntryPoint {

  @Override
  protected void createContents( Composite parent ) {
    // Set UISession so that any child thread of UIThread can access it
    WebSpoonUtils.setUISession( RWT.getUISession() );
    WebSpoonUtils.setUISession( WebSpoonUtils.getConnectionId(), RWT.getUISession() );
    WebSpoonUtils.setUser( WebSpoonUtils.getConnectionId(), RWT.getRequest().getRemoteUser() );
    // Transferring Widget Data for client-side canvas drawing instructions
    WidgetUtil.registerDataKeys( "props" );
    WidgetUtil.registerDataKeys( "mode" );
    WidgetUtil.registerDataKeys( "nodes" );
    WidgetUtil.registerDataKeys( "hops" );
    WidgetUtil.registerDataKeys( "notes" );
    /*
     *  The following lines were migrated from Spoon.main
     *  because they are session specific.
     */
    PropsUi.init( parent.getDisplay() );

    // Options
    StartupParameters serviceParams = RWT.getClient().getService( StartupParameters.class );
    List<String> args = new ArrayList<String>();
    String[] options = { "rep", "user", "pass", "trans", "job", "dir", "file" };
    for ( String option : options ) {
      if ( serviceParams.getParameter( option ) != null ) {
        args.add( "-" + option + "=" + serviceParams.getParameter( option ) );
      }
    }

    // Execute Spoon.createContents
    HopGui.getInstance().setCommandLineArguments( args );
    HopGui.getInstance().setShell( parent.getShell() );
    try {
      HopGuiEnvironment.init();
    } catch (HopException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    HopGui.getInstance().open();

    /*
     *  The following lines are webSpoon additional functions
     */
    // In webSpoon, SWT.Close is not triggered on closing a browser (tab).
    parent.getDisplay().addListener( SWT.Dispose, ( event ) -> {
      try {
        /**
         *  UISession at WebSpoonUtils.uiSession will be GCed when UIThread dies.
         *  But the one at WebSpoonUtils.uiSessionMap should be explicitly removed.
         */
        WebSpoonUtils.removeUISession( WebSpoonUtils.getConnectionId() );
        WebSpoonUtils.removeUser( WebSpoonUtils.getConnectionId() );
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error closing Spoon", e );
      }
    });
  }

}
