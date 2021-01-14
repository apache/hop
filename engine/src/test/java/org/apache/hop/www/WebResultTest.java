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

package org.apache.hop.www;

import org.apache.hop.core.exception.HopXmlException;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class WebResultTest {

  @Test
  public void testStatics() {
    assertEquals( "webresult", WebResult.XML_TAG );
    assertEquals( "OK", WebResult.STRING_OK );
    assertEquals( "ERROR", WebResult.STRING_ERROR );
    assertNotNull( WebResult.OK );
    assertEquals( "OK", WebResult.OK.getResult() );
    assertNull( WebResult.OK.getMessage() );
    assertNull( WebResult.OK.getId() );
  }

  @Test
  public void testConstructors() {
    String expectedResult = UUID.randomUUID().toString();
    WebResult result = new WebResult( expectedResult );
    assertEquals( expectedResult, result.getResult() );

    String expectedMessage = UUID.randomUUID().toString();
    result = new WebResult( expectedResult, expectedMessage );
    assertEquals( expectedResult, result.getResult() );
    assertEquals( expectedMessage, result.getMessage() );

    String expectedId = UUID.randomUUID().toString();
    result = new WebResult( expectedResult, expectedMessage, expectedId );
    assertEquals( expectedResult, result.getResult() );
    assertEquals( expectedMessage, result.getMessage() );
    assertEquals( expectedId, result.getId() );
  }

  @Test
  public void testSerialization() throws HopXmlException {
    WebResult original = new WebResult( UUID.randomUUID().toString(), UUID.randomUUID().toString(),
      UUID.randomUUID().toString() );

    String xml = original.getXml();
    WebResult copy = WebResult.fromXmlString( xml );

    assertNotNull( copy );
    assertNotSame( original, copy );
    assertEquals( original.getResult(), copy.getResult() );
    assertEquals( original.getMessage(), copy.getMessage() );
    assertEquals( original.getId(), copy.getId() );
  }

  @Test
  public void testSetters() {
    WebResult result = new WebResult( "" );
    assertEquals( "", result.getResult() );

    result.setMessage( "fakeMessage" );
    assertEquals( "fakeMessage", result.getMessage() );
    result.setResult( "fakeResult" );
    assertEquals( "fakeResult", result.getResult() );
    result.setId( "fakeId" );
    assertEquals( "fakeId", result.getId() );
  }
}
