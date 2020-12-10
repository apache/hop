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

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hop.i18n.BaseMessages.getString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class TransformOptionTest {
  @Mock TransformMeta transformMeta;
  @Mock IVariables variables;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setup() {
    when( variables.resolve( anyString() ) ).thenAnswer( incovacationMock -> {
      Object[] arguments = incovacationMock.getArguments();
      return (String) arguments[ 0 ];
    } );
  }

  @Test
  public void testCheckPass() {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformOption.checkInteger( remarks, transformMeta, variables, "IDENTIFIER", "9" );
    TransformOption.checkLong( remarks, transformMeta, variables, "IDENTIFIER", "9" );
    TransformOption.checkBoolean( remarks, transformMeta, variables, "IDENTIFIER", "true" );
    TransformOption.checkBoolean( remarks, transformMeta, variables, "IDENTIFIER", "false" );
    assertEquals( 0, remarks.size() );
  }

  @Test
  public void testCheckPassEmpty() {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformOption.checkInteger( remarks, transformMeta, variables, "IDENTIFIER", "" );
    TransformOption.checkLong( remarks, transformMeta, variables, "IDENTIFIER", "" );
    TransformOption.checkBoolean( remarks, transformMeta, variables, "IDENTIFIER", "" );
    TransformOption.checkInteger( remarks, transformMeta, variables, "IDENTIFIER", null );
    TransformOption.checkLong( remarks, transformMeta, variables, "IDENTIFIER", null );
    TransformOption.checkBoolean( remarks, transformMeta, variables, "IDENTIFIER", null );
    assertEquals( 0, remarks.size() );
  }

  @Test
  public void testCheckFailInteger() {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformOption.checkInteger( remarks, transformMeta, variables, "IDENTIFIER", "asdf" );
    assertEquals( 1, remarks.size() );
    assertEquals( remarks.get( 0 ).getText(),
      getString( TransformOption.class, "TransformOption.CheckResult.NotAInteger", "IDENTIFIER" ) );
  }

  @Test
  public void testCheckFailLong() {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformOption.checkLong( remarks, transformMeta, variables, "IDENTIFIER", "asdf" );
    assertEquals( 1, remarks.size() );
    assertEquals( remarks.get( 0 ).getText(),
      getString( TransformOption.class, "TransformOption.CheckResult.NotAInteger", "IDENTIFIER" ) );
  }

  @Test
  public void testCheckFailBoolean() {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformOption.checkBoolean( remarks, transformMeta, variables, "IDENTIFIER", "asdf" );
    assertEquals( 1, remarks.size() );
    assertEquals( remarks.get( 0 ).getText(),
      getString( TransformOption.class, "TransformOption.CheckResult.NotABoolean", "IDENTIFIER" ) );
  }
}
