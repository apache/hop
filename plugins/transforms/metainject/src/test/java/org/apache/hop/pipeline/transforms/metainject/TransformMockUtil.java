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

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
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

  public static TransformMockHelper getTransformMockHelper( Class<MetaInjectMeta> metaClass, Class<MetaInjectData> dataClass, String name ) {

    TransformMockHelper<MetaInjectMeta, MetaInjectData> transformMockHelper = new TransformMockHelper<>( name, metaClass, dataClass );

    when( transformMockHelper.logChannelFactory.create( ArgumentMatchers.any(), ArgumentMatchers.any( ILoggingObject.class ) ) ).thenReturn( transformMockHelper.iLogChannel );
    when( transformMockHelper.logChannelFactory.create( ArgumentMatchers.any() ) ).thenReturn( transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    return transformMockHelper;
  }

  public static MetaInject getTransform( Class<MetaInject> klass, TransformMockHelper mock )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Constructor<MetaInject> kons = klass.getConstructor(
      TransformMeta.class,
      MetaInjectMeta.class,
      MetaInjectData.class,
      int.class,
      PipelineMeta.class,
      Pipeline.class
    );
    MetaInject transform = kons.newInstance( mock.transformMeta, mock.iTransformMeta, mock.iTransformData, 0, mock.pipelineMeta, mock.pipeline );
    return transform;
  }

  public static MetaInject getTransform( Class<MetaInject> transformClass, Class<MetaInjectMeta> transformMetaClass, Class<MetaInjectData> dataClass, String transformName )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    return TransformMockUtil.getTransform( transformClass, TransformMockUtil.getTransformMockHelper( transformMetaClass, dataClass, transformName ) );
  }

}
