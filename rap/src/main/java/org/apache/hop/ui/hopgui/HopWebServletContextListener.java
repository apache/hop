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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.eclipse.rap.rwt.engine.RWTServletContextListener;

import javax.servlet.ServletContextEvent;
import java.util.logging.Logger;

public class HopWebServletContextListener extends RWTServletContextListener {
  private static final Logger logger =
      Logger.getLogger(HopWebServletContextListener.class.getName());

  @Override
  public void contextInitialized(ServletContextEvent event) {
    /*
     *  The following lines are from HopGui.main
     *  because they are application-wide context.
     */
    try {
      HopEnvironment.init();
      HopGuiEnvironment.init();
    } catch (HopException e) {
      e.printStackTrace();
    }
    super.contextInitialized(event);
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
    super.contextDestroyed(event);
    // Kill all remaining things in this VM!
    System.exit(0);
  }
}
