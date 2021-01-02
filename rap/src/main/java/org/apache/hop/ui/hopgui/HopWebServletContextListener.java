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
import org.eclipse.rap.rwt.engine.RWTServletContextListener;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HopWebServletContextListener extends RWTServletContextListener {
  private final static Logger logger = Logger.getLogger( HopWebServletContextListener.class.getName() );

  public void contextInitialized( ServletContextEvent event ) {
    /*
     *  The following lines are from HopGui.main
     *  because they are application-wide context.
     */
    try {
      HopEnvironment.init();
      HopGuiEnvironment.init();
    } catch ( HopException e ) {
      e.printStackTrace();
    }
    super.contextInitialized( event );
  }
  public void contextDestroyed( ServletContextEvent event ) {
    super.contextDestroyed( event );
    // Kill all remaining things in this VM!
    System.exit( 0 );
  }
}
