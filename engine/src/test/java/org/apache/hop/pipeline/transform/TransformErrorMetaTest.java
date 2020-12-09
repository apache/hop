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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TransformErrorMetaTest {

  @Test
  public void testGetErrorRowMeta() {
    IVariables vars = new Variables();
    vars.setVariable( "VarNumberErrors", "nbrErrors" );
    vars.setVariable( "VarErrorDescription", "errorDescription" );
    vars.setVariable( "VarErrorFields", "errorFields" );
    vars.setVariable( "VarErrorCodes", "errorCodes" );
    TransformErrorMeta testObject = new TransformErrorMeta( new TransformMeta(), new TransformMeta(),
      "${VarNumberErrors}", "${VarErrorDescription}", "${VarErrorFields}", "${VarErrorCodes}" );
    IRowMeta result = testObject.getErrorRowMeta(vars); // 10, "some data was bad", "factId", "BAD131" );

    assertNotNull( result );
    assertEquals( 4, result.size() );
    assertEquals( IValueMeta.TYPE_INTEGER, result.getValueMeta( 0 ).getType() );
    assertEquals( "nbrErrors", result.getValueMeta( 0 ).getName() );
    assertEquals( IValueMeta.TYPE_STRING, result.getValueMeta( 1 ).getType() );
    assertEquals( "errorDescription", result.getValueMeta( 1 ).getName() );
    assertEquals( IValueMeta.TYPE_STRING, result.getValueMeta( 2 ).getType() );
    assertEquals( "errorFields", result.getValueMeta( 2 ).getName() );
    assertEquals( IValueMeta.TYPE_STRING, result.getValueMeta( 3 ).getType() );
    assertEquals( "errorCodes", result.getValueMeta( 3 ).getName() );
  }
}
