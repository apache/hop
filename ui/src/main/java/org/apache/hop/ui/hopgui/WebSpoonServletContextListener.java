/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 * Copyright (C) 2016 by Hitachi America, Ltd., R&D : http://www.hitachi-america.us/rd/
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.www.HopServerSingleton;
import org.apache.hop.www.SlaveServerConfig;
import org.eclipse.rap.rwt.engine.RWTServletContextListener;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class WebSpoonServletContextListener extends RWTServletContextListener {
  private final static Logger logger = Logger.getLogger( WebSpoonServletContextListener.class.getName() );

  public void contextInitialized( ServletContextEvent event ) {
    logger.info( "Current directory: " + ( new File( "." ) ).getCanonicalPath() );

    System.setProperty( "KETTLE_CONTEXT_PATH", event.getServletContext().getContextPath() );
    /*
     *  The following lines are from Spoon.main
     *  because they are application-wide context.
     */
    ExecutorService executor = Executors.newCachedThreadPool();
    Future<HopException> pluginRegistryFuture = executor.submit( new Callable<HopException>() {

      @Override
      public HopException call() throws Exception {
        HopGui.registerUIPluginObjectTypes();

        HopClientEnvironment.getInstance().setClient( HopClientEnvironment.ClientType.HOP_GUI );
        try {
          HopEnvironment.init( false );
        } catch ( HopException e ) {
          return e;
        }

        return null;
      }
    } );
    HopException registryException;
    try {
      registryException = pluginRegistryFuture.get();
      if ( registryException != null ) {
        throw registryException;
      }
      HopGui.initLogging( HopGui.getInstance().getCommandLineArguments() );
    } catch ( Throwable t ) {
      // avoid calls to Messages i18n method getString() in this block
      // We do this to (hopefully) also catch Out of Memory Exceptions
      //
      t.printStackTrace();
    }
    super.contextInitialized( event );
  }
  public void contextDestroyed( ServletContextEvent event ) {
    super.contextDestroyed( event );
    // Kill all remaining things in this VM!
    System.exit( 0 );
  }
}
