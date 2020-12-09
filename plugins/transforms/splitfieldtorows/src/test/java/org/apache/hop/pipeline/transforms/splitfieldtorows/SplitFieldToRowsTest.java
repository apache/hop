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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrey Khayrutdinov
 */
public class SplitFieldToRowsTest {

	  private TransformMockHelper<SplitFieldToRowsMeta, SplitFieldToRowsData> transformMockHelper;

	  @Before
	  public void setup() {
	    transformMockHelper =
				new TransformMockHelper<>( "Test SplitFieldToRows", SplitFieldToRowsMeta.class, SplitFieldToRowsData.class );
	    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) )
	      .thenReturn( transformMockHelper.iLogChannel );
	    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
	  }

	  @After
	  public void tearDown() {
	    transformMockHelper.cleanUp();
	  }
	
  @Test
  public void interpretsNullDelimiterAsEmpty() throws Exception {
    SplitFieldToRows transform =
    	      new SplitFieldToRows( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
    	        transformMockHelper.pipeline );
    
	transform.init();    
    
    SplitFieldToRowsMeta meta = new SplitFieldToRowsMeta();
    meta.setDelimiter( null );
    meta.setDelimiterRegex( false );

    transform.init();
    
    // empty string should be quoted --> \Q\E
    assertEquals( "\\Q\\E", transform.getData().delimiterPattern.pattern() );
  }
}
