/*!
 * Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.lifecycle.IHopLifecycleListener;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.*;


import static org.mockito.Mockito.*;

/**
 * Created by Vasilina_Terehova on 3/31/2017.
 */
public class OpenMappingExtensionTest{

  public static final String PIPELINE_META_NAME = "Test name";
  private static ILogChannel logChannelInterface;
  private PipelineMeta pipelineMeta;
  private TransformMeta transformMeta;
  private Object[] metaData;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setupClass() throws HopException{
    HopEnvironment.init();
  }

  @Before
  public void setup() {
//    setup();
    PipelineMeta pipelineMeta = spy( new PipelineMeta() );
    transformMeta = mock( TransformMeta.class );
    metaData = new Object[] { transformMeta, pipelineMeta };
    setKettleLogFactoryWithMock();
  }

  @Test
  @Ignore
  public void testLocalizedMessage() throws HopException {
    OpenMappingExtension openMappingExtension = new OpenMappingExtension();
    Class PKG = IHopLifecycleListener.class;
    String afterInjectionMessageAdded = BaseMessages.getString( PKG, "PipelineGraph.AfterInjection" );
    PipelineMeta pipelineMeta = spy( new PipelineMeta() );
    pipelineMeta.setName(PIPELINE_META_NAME);
    doReturn( mock( MetaInjectMeta.class ) ).when(transformMeta).getTransform();
    openMappingExtension.callExtensionPoint( logChannelInterface, metaData );
    assert ( pipelineMeta.getName().contains( afterInjectionMessageAdded ) );
  }

  private void setKettleLogFactoryWithMock() {
    ILogChannelFactory logChannelInterfaceFactory = mock( ILogChannelFactory.class );
    logChannelInterface = mock( ILogChannel.class );
    when( logChannelInterfaceFactory.create( any() ) ).thenReturn( logChannelInterface );
    HopLogStore.setLogChannelFactory( logChannelInterfaceFactory );
  }
}
