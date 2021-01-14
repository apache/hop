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
package org.apache.hop.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckResultTest {
  @Test
  public void testClass() {
    final int type = ICheckResult.TYPE_RESULT_ERROR;
    final String text = "some text";
    final String sourceMetaName = "meta name";
    final ICheckResultSource sourceMeta = mock( ICheckResultSource.class );
    final String errorCode = "error code";

    CheckResult cr = new CheckResult();
    assertEquals( ICheckResult.TYPE_RESULT_NONE, cr.getType() );
    assertTrue( cr.getTypeDesc() != null && cr.getTypeDesc().isEmpty() );
    cr.setType( type );
    assertEquals( type, cr.getType() );

    assertTrue( cr.getText().isEmpty() );
    cr.setText( text );
    assertSame( text, cr.getText() );

    assertNull( null, cr.getSourceInfo() );

    assertNull( cr.getErrorCode() );
    cr.setErrorCode( errorCode );
    assertSame( errorCode, cr.getErrorCode() );

    when( sourceMeta.getName() ).thenReturn( sourceMetaName );
    cr = new CheckResult( type, text, sourceMeta );
    assertSame( sourceMeta, cr.getSourceInfo() );
    assertTrue( cr.getTypeDesc() != null && !cr.getTypeDesc().isEmpty() );
    final String stringValue = String.format( "%s: %s (%s)", cr.getTypeDesc(), text, sourceMetaName );
    assertEquals( stringValue, cr.toString() );

    cr = new CheckResult( type, errorCode, text, sourceMeta );
    assertSame( errorCode, cr.getErrorCode() );
  }
}
