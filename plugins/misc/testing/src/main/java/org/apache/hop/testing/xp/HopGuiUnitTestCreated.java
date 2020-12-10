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

package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

@ExtensionPoint(
  id = "HopGuiUnitTestCreated",
  extensionPointId = "HopGuiMetadataObjectCreated",
  description = "When HopGui create a new metadata object somewhere"
)
public class HopGuiUnitTestCreated extends HopGuiUnitTestChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, Object object ) throws HopException {

    // Refresh the tests list
    //
    super.callExtensionPoint( log, variables, object );

    // Ignore all other metadata object changes
    //
    if (!(object instanceof PipelineUnitTest)) {
      return;
    }
    PipelineUnitTest test = (PipelineUnitTest) object;

    HopGui hopGui = HopGui.getInstance();
    TestingGuiPlugin testingGuiPlugin = TestingGuiPlugin.getInstance();

    // Create this for the active pipeline...
    //
    PipelineMeta pipelineMeta = testingGuiPlugin.getActivePipelineMeta();
    if ( pipelineMeta == null ) {
      return;
    }

    MessageBox messageBox = new MessageBox( hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
    messageBox.setText( "Attach?" );
    messageBox.setMessage( "Do you want to use this unit test for the active pipeline '" + pipelineMeta.getName() + "'?" );
    int answer = messageBox.open();
    if ( ( answer & SWT.YES ) == 0 ) {
      return;
    }

    // Attach it to the active pipeline
    // TODO: calculate relative filename?
    //
    test.setPipelineFilename( pipelineMeta.getFilename() );


    // Also switch to this unit test
    //
    TestingGuiPlugin.selectUnitTest( pipelineMeta, test );

    // Refresh
    //
    hopGui.getActiveFileTypeHandler().updateGui();

  }
}
