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

package org.apache.hop.ui.hopui.delegates;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointInterface;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.ClassLoadingPluginInterface;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.hopui.HopUi;
import org.junit.ClassRule;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HopUiStepsDelegateTest {
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

    HopUiStepsDelegate delegate = mock( HopUiStepsDelegate.class );
    delegate.hopUi = mock( HopUi.class );
    doCallRealMethod().when( delegate ).delSteps( any( TransMeta.class ), any( StepMeta[].class ) );

    TransMeta trans = mock( TransMeta.class );
    StepMeta[] steps = new StepMeta[] { mock( StepMeta.class ) };
    delegate.delSteps( trans, steps );

    verify( extensionPoint, times( 1 ) ).callExtensionPoint( any(), eq( steps ) );
  }
}
