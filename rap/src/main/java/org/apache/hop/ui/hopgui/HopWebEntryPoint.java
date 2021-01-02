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

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.application.AbstractEntryPoint;
import org.eclipse.rap.rwt.client.service.StartupParameters;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.widgets.Composite;

public class HopWebEntryPoint extends AbstractEntryPoint {

  @Override
  protected void createContents( Composite parent ) {
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
    HopGui.getInstance().setProps( PropsUi.getInstance() );
    try {
      ExtensionPointHandler.callExtensionPoint(
        HopGui.getInstance().getLog(),
        HopGui.getInstance().getVariables(),
        HopExtensionPoint.HopGuiInit.id,
        HopGui.getInstance()
      );
    } catch(Exception e) {
      HopGui.getInstance().getLog().logError( "Error calling extension point plugin(s) for HopGuiInit", e);
    }

    HopGui.getInstance().open();
  }

}
