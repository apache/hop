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

package org.apache.hop.core.variables;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Variables tests.
 *
 * @author Yury Bakhmutski
 * @see Variables
 */
public class VariablesTest {

  private Variables variables = new Variables();

  /**
   * Test for PDI-12893 issue.  Checks if an ConcurrentModificationException while iterating over the System properties
   * is occurred.
   */
  @Test
  public void testinItializeVariablesFrom() {
    final Variables variablesMock = mock( Variables.class );
    doCallRealMethod().when( variablesMock ).initializeFrom( any( IVariables.class ) );

    @SuppressWarnings( "unchecked" ) final Map<String, String> propertiesMock = mock( Map.class );
    when( variablesMock.getProperties() ).thenReturn( propertiesMock );

    doAnswer( new Answer<Map<String, String>>() {
      final String keyStub = "key";

      @Override
      public Map<String, String> answer( InvocationOnMock invocation ) throws Throwable {
        if ( System.getProperty( keyStub ) == null ) {
          modifySystemproperties();
        }

        if ( invocation.getArguments()[ 1 ] != null ) {
          propertiesMock.put( (String) invocation.getArguments()[ 0 ], System.getProperties().getProperty(
            (String) invocation.getArguments()[ 1 ] ) );
        }
        return propertiesMock;
      }
    } ).when( propertiesMock ).put( anyString(), anyString() );

    variablesMock.initializeFrom( null );
  }

  private void modifySystemproperties() {
    final String keyStub = "key";
    final String valueStub = "value";

    Thread thread = new Thread( () -> System.setProperty( keyStub, valueStub ) );
    thread.start();
  }

  /**
   * Spawns 20 threads that modify variables to test concurrent modification error fix.
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentModification() throws Exception {

    int threads = 20;
    List<Callable<Boolean>> callables = new ArrayList<>();
    for ( int i = 0; i < threads; i++ ) {
      callables.add( newCallable() );
    }

    // Assert threads ran successfully.
    for ( Future<Boolean> result : Executors.newFixedThreadPool( 5 ).invokeAll( callables ) ) {
      assertTrue( result.get() );
    }
  }

  // Note:  Not using lambda so this can be ported to older version compatible with 1.7
  private Callable<Boolean> newCallable() {
    return () -> {
      for ( int i = 0; i < 300; i++ ) {
        String key = "key" + i;
        variables.setVariable( key, "value" );
        assertEquals( variables.resolve( "${" + key + "}" ), "value" );
      }
      return true;
    };
  }

  @Test
  public void testFieldSubstitution() throws HopValueException {
    Object[] rowData = new Object[] { "DataOne", "DataTwo" };
    RowMeta rm = new RowMeta();
    rm.addValueMeta( new ValueMetaString( "FieldOne" ) );
    rm.addValueMeta( new ValueMetaString( "FieldTwo" ) );

    Variables vars = new Variables();
    assertNull( vars.resolve( null, rm, rowData ) );
    assertEquals( "", vars.resolve( "", rm, rowData ) );
    assertEquals( "DataOne", vars.resolve( "?{FieldOne}", rm, rowData ) );
    assertEquals( "TheDataOne", vars.resolve( "The?{FieldOne}", rm, rowData ) );
  }

  @Test
  public void testEnvironmentSubstitute() {
    Variables vars = new Variables();
    vars.setVariable( "VarOne", "DataOne" );
    vars.setVariable( "VarTwo", "DataTwo" );

    assertNull( vars.resolve( (String) null ) );
    assertEquals( "", vars.resolve( "" ) );
    assertEquals( "DataTwo", vars.resolve( "${VarTwo}" ) );
    assertEquals( "DataTwoEnd", vars.resolve( "${VarTwo}End" ) );

    assertEquals( 0, vars.resolve( new String[ 0 ] ).length );
    assertArrayEquals( new String[] { "DataOne", "TheDataOne" },
      vars.resolve( new String[] { "${VarOne}", "The${VarOne}" } ) );
  }
}
