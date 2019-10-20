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

package org.apache.hop.ui.spoon.delegates;

import org.eclipse.swt.widgets.Shell;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointInterface;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.ClassLoadingPluginInterface;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.ui.spoon.Spoon;

import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpoonStepsDelegateTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public interface PluginMockInterface extends ClassLoadingPluginInterface, PluginInterface {
  }

  @Test
  public void testDelStepsExtensionPointCancelDelete() throws Exception {
    PluginMockInterface pluginInterface = mock( PluginMockInterface.class );
    when( pluginInterface.getName() ).thenReturn( HopExtensionPoint.TransBeforeDeleteSteps.id );
    when( pluginInterface.getMainType() ).thenReturn( (Class) ExtensionPointInterface.class );
    when( pluginInterface.getIds() ).thenReturn( new String[] { HopExtensionPoint.TransBeforeDeleteSteps.id } );

    ExtensionPointInterface extensionPoint = mock( ExtensionPointInterface.class );
    when( pluginInterface.loadClass( ExtensionPointInterface.class ) ).thenReturn( extensionPoint );
    doThrow( HopException.class ).when( extensionPoint )
        .callExtensionPoint( any( LogChannelInterface.class ), any( StepMeta[].class ) );

    PluginRegistry.addPluginType( ExtensionPointPluginType.getInstance() );
    PluginRegistry.getInstance().registerPlugin( ExtensionPointPluginType.class, pluginInterface );

    SpoonStepsDelegate delegate = mock( SpoonStepsDelegate.class );
    delegate.spoon = mock( Spoon.class );
    doCallRealMethod().when( delegate ).delSteps( any( TransMeta.class ), any( StepMeta[].class ) );

    TransMeta trans = mock( TransMeta.class );
    StepMeta[] steps = new StepMeta[] { mock( StepMeta.class ) };
    delegate.delSteps( trans, steps );

    verify( extensionPoint, times( 1 ) ).callExtensionPoint( any(), eq( steps ) );
  }

  @Test
  public void testGetStepDialogClass() throws Exception {
    PluginMockInterface plugin = mock( PluginMockInterface.class );
    when( plugin.getIds() ).thenReturn( new String[] { "mockPlugin"} );
    when( plugin.matches( "mockPlugin" ) ).thenReturn( true );
    when( plugin.getName() ).thenReturn( "mockPlugin" );

    StepMetaInterface meta = mock( StepMetaInterface.class );
    when( meta.getDialogClassName() ).thenReturn( String.class.getName() );
    when( plugin.getClassMap() ).thenReturn( new HashMap<Class<?>, String>() {{
        put( StepMetaInterface.class, meta.getClass().getName() );
        put( StepDialogInterface.class, StepDialogInterface.class.getName() );
      }} );

    PluginRegistry.getInstance().registerPlugin( StepPluginType.class, plugin );

    SpoonStepsDelegate delegate = mock( SpoonStepsDelegate.class );
    Spoon spoon = mock( Spoon.class );
    delegate.spoon = spoon;
    delegate.log = mock( LogChannelInterface.class );
    when( spoon.getShell() ).thenReturn( mock( Shell.class ) );
    doCallRealMethod().when( delegate ).getStepDialog( any( StepMetaInterface.class ), any( TransMeta.class ), any( String.class ) );

    TransMeta trans = mock( TransMeta.class );

    // verify that dialog class is requested from plugin
    try {
      delegate.getStepDialog( meta, trans, "" ); // exception is expected here
    } catch ( Throwable ignore ) {
      verify( meta, never() ).getDialogClassName();
    }

    // verify that the deprecated way is still valid
    when( plugin.getClassMap() ).thenReturn( new HashMap<Class<?>, String>() {{
        put( StepMetaInterface.class, meta.getClass().getName() );
      }} );
    try {
      delegate.getStepDialog( meta, trans, "" ); // exception is expected here
    } catch ( Throwable ignore ) {
      verify( meta, times( 1 ) ).getDialogClassName();
    }

    // cleanup
    PluginRegistry.getInstance().removePlugin( StepPluginType.class, plugin );
  }
}
