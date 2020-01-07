/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.cluster;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogChannelInterfaceFactory;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransMetaFactory;
import org.apache.hop.trans.TransMetaFactoryImpl;
import org.w3c.dom.Node;

public class TransSplitterTest {
  private LogChannelInterfaceFactory oldLogChannelInterfaceFactory;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() throws Exception {
    LogChannelInterfaceFactory logChannelInterfaceFactory = mock( LogChannelInterfaceFactory.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    oldLogChannelInterfaceFactory = HopLogStore.getLogChannelInterfaceFactory();
    HopLogStore.setLogChannelInterfaceFactory( logChannelInterfaceFactory );
    when( logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
        logChannelInterface );
  }

  @After
  public void tearDown() {
    HopLogStore.setLogChannelInterfaceFactory( oldLogChannelInterfaceFactory );
  }

  @Test
   public void testTransSplitterRowsetSize() throws HopException {
    TransMeta originalMeta = new TransMeta();
    originalMeta.setSizeRowset( 0 );
    TransMetaFactory factory = new TransMetaFactoryImpl();

    try {
      TransSplitter transSplitter = new TransSplitter( originalMeta, factory );
      transSplitter.splitOriginalTransformation();
      assertEquals( originalMeta.getSizeRowset(), transSplitter.getMaster().getSizeRowset() );
    } catch ( Exception e ) {
      //ignore
    }

  }
}
