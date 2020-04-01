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

package org.apache.hop.pipeline.steps.selectvalues;

import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStep;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.mock.StepMockHelper;
import org.mockito.ArgumentMatchers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.mockito.Mockito.when;

/**
 * <p>
 * Util class to handle StepMock creation in generic way.
 * </p>
 * <p>
 * Usage example:
 * <pre>
 * Mapping step = StepMockUtil.getStep( Mapping.class, MappingMeta.class, "junit" );
 * </pre>
 *
 *
 * </p>
 */
public class StepMockUtil {

  public static <T extends StepMetaInterface, V extends BaseStep> StepMockHelper<T, StepDataInterface> getStepMockHelper( Class<T> meta, String name ) {
    StepMockHelper<T, StepDataInterface> stepMockHelper = new StepMockHelper<T, StepDataInterface>( name, meta, StepDataInterface.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( ArgumentMatchers.any(), ArgumentMatchers.any( LoggingObjectInterface.class ) ) ).thenReturn( stepMockHelper.logChannelInterface );
    when( stepMockHelper.logChannelInterfaceFactory.create( ArgumentMatchers.any() ) ).thenReturn( stepMockHelper.logChannelInterface );
    when( stepMockHelper.pipeline.isRunning() ).thenReturn( true );
    return stepMockHelper;
  }

  public static <T extends BaseStep, K extends StepMetaInterface, V extends StepDataInterface> T getStep( Class<T> klass, StepMockHelper<K, V> mock )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Constructor<T> kons = klass.getConstructor( StepMeta.class, StepDataInterface.class, int.class, PipelineMeta.class, Pipeline.class );
    T step = kons.newInstance( mock.stepMeta, mock.stepDataInterface, 0, mock.pipelineMeta, mock.pipeline );
    return step;
  }

  public static <T extends BaseStep, K extends StepMetaInterface> T getStep( Class<T> stepClass, Class<K> stepMetaClass, String stepName )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    return StepMockUtil.getStep( stepClass, StepMockUtil.getStepMockHelper( stepMetaClass, stepName ) );
  }

}
