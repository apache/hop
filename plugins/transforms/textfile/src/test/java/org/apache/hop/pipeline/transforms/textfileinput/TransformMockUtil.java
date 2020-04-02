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

package org.apache.hop.pipeline.transforms.textfileinput;

import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
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

  public static <Meta extends TransformMetaInterface, Main extends BaseTransform, Data extends TransformDataInterface> TransformMockHelper<Meta, Data> getTransformMockHelper( Class<Meta> metaClass, Class<Data> dataClass, String name ) {
    TransformMockHelper<Meta, Data> transformMockHelper = new TransformMockHelper<>( name, metaClass, dataClass );
    when( transformMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.logChannelInterfaceFactory.create( any() ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    return transformMockHelper;
  }

  public static <Main extends BaseTransform, Meta extends TransformMetaInterface, Data extends TransformDataInterface> Main getTransform( Class<Main> mainClass, Class<Data> dataClass, TransformMockHelper<Meta, Data> mock )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Constructor<Main> kons = mainClass.getConstructor( TransformMeta.class, dataClass, int.class, PipelineMeta.class, Pipeline.class );
    Main transform = kons.newInstance( mock.transformMeta, mock.transformDataInterface, 0, mock.pipelineMeta, mock.pipeline );
    return transform;
  }

  public static <Main extends BaseTransform, Meta extends TransformMetaInterface, Data extends TransformDataInterface> Main getTransform( Class<Main> mainClass, Class<Meta> metaClass, Class<Data> dataClass, String transformName )
    throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    return TransformMockUtil.getTransform( mainClass, dataClass, TransformMockUtil.getTransformMockHelper( metaClass, dataClass, transformName ) );
  }

}
