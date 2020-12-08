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

package org.apache.hop.pipeline.transforms.salesforceinput;

import com.sforce.ws.util.Base64;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class SalesforceInputTest {

  @Test
  public void doConversions() throws Exception {
    TransformMeta transformMeta = new TransformMeta();
    String name = "test";
    transformMeta.setName( name );
    int copyNr = 0;
    PipelineMeta pipelineMeta = Mockito.mock( PipelineMeta.class );
    Pipeline pipeline = Mockito.spy( new LocalPipelineEngine() );
    Mockito.when( pipelineMeta.findTransform( Mockito.eq( name ) ) ).thenReturn( transformMeta );

    SalesforceInputMeta meta = new SalesforceInputMeta();
    SalesforceInputData data = new SalesforceInputData();

    SalesforceInput salesforceInput = new SalesforceInput( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    data.outputRowMeta = Mockito.mock( RowMeta.class );
    Mockito.when( data.outputRowMeta.getValueMeta( Mockito.eq( 0 ) ) ).thenReturn( new ValueMetaBinary() );

    data.convertRowMeta = Mockito.mock( RowMeta.class );
    Mockito.when( data.convertRowMeta.getValueMeta( Mockito.eq( 0 ) ) ).thenReturn( new ValueMetaString() );

    Object[] outputRowData = new Object[ 1 ];
    byte[] binary = { 0, 1, 0, 1, 1, 1 };
    salesforceInput.doConversions( outputRowData, 0, new String( Base64.encode( binary ) ) );
    Assert.assertArrayEquals( binary, (byte[]) outputRowData[ 0 ] );

    binary = new byte[ 0 ];
    salesforceInput.doConversions( outputRowData, 0, new String( Base64.encode( binary ) ) );
    Assert.assertArrayEquals( binary, (byte[]) outputRowData[ 0 ] );
  }

}
