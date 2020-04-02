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

package org.apache.hop.pipeline.transforms;

import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * <p>
 * Util class to handle TransformMock creation in generic way.
 * </p>
 * <p>
 * Usage example:
 * <pre>
 * Mapping transform = TransformMockUtil.getTransform( Mapping.class, MappingMeta.class, "junit" );
 * </pre>
 *
 *
 * </p>
 */
public class TransformMockUtil {

  public static <T extends TransformMetaInterface, V extends BaseTransform> TransformMockHelper<T, TransformDataInterface> getTransformMockHelper( Class<T> meta, String name ) {
    TransformMockHelper<T, TransformDataInterface> transformMockHelper = new TransformMockHelper<T, TransformDataInterface>( name, meta, TransformDataInterface.class );
    when( transformMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.logChannelInterfaceFactory.create( any() ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    return transformMockHelper;
  }

  public static <T extends BaseTransform, K extends TransformMetaInterface, V extends TransformDataInterface> T getTransform( Class<T> klass, TransformMockHelper<K, V> mock )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Constructor<T> kons = klass.getConstructor( TransformMeta.class, TransformDataInterface.class, int.class, PipelineMeta.class, Pipeline.class );
    T transform = kons.newInstance( mock.transformMeta, mock.transformDataInterface, 0, mock.pipelineMeta, mock.pipeline );
    return transform;
  }

  public static <T extends BaseTransform, K extends TransformMetaInterface> T getTransform( Class<T> transformClass, Class<K> transformMetaClass, String transformName )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    return TransformMockUtil.getTransform( transformClass, TransformMockUtil.getTransformMockHelper( transformMetaClass, transformName ) );
  }

}
