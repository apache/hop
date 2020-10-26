/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.mockito.ArgumentMatchers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

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

  public static <Main extends BaseTransform, Meta extends ITransformMeta, Data extends ITransformData> TransformMockHelper<Meta, Data> getTransformMockHelper( Class<Meta> metaClass, Class<Data> dataClass, String name ) {

    TransformMockHelper<Meta, Data> transformMockHelper = new TransformMockHelper<Meta, Data>( name, metaClass, dataClass );

    when( transformMockHelper.logChannelFactory.create( ArgumentMatchers.any(), ArgumentMatchers.any( ILoggingObject.class ) ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.logChannelFactory.create( ArgumentMatchers.any() ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    return transformMockHelper;
  }

  public static <T extends BaseTransform, K extends ITransformMeta, V extends ITransformData> T getTransform( Class<T> klass, TransformMockHelper<K, V> mock )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Constructor<T> kons = klass.getConstructor( TransformMeta.class, ITransformData.class, int.class, PipelineMeta.class, Pipeline.class );
    T transform = kons.newInstance( mock.transformMeta, mock.iTransformData, 0, mock.pipelineMeta, mock.pipeline );
    return transform;
  }

  public static <T extends BaseTransform, K extends ITransformMeta, Data extends ITransformData> T getTransform( Class<T> transformClass, Class<K> transformMetaClass, Class<Data> dataClass, String transformName )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    return TransformMockUtil.getTransform( transformClass, TransformMockUtil.getTransformMockHelper( transformMetaClass, dataClass, transformName ) );
  }

}
