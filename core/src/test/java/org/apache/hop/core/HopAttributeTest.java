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
import static org.mockito.Mockito.mock;

public class HopAttributeTest {
  @Test
  public void testClass() {
    final String key = "key";
    final String xmlCode = "xmlCode";
    final String description = "description";
    final String tooltip = "tooltip";
    final int type = 6;
    final IHopAttribute parent = mock( IHopAttribute.class );
    HopAttribute attribute = new HopAttribute( key, xmlCode, description, tooltip, type, parent );
    assertSame( key, attribute.getKey() );
    assertSame( xmlCode, attribute.getXmlCode() );
    assertSame( description, attribute.getDescription() );
    assertSame( tooltip, attribute.getTooltip() );
    assertEquals( type, attribute.getType() );
    assertSame( parent, attribute.getParent() );

    attribute.setKey( null );
    assertNull( attribute.getKey() );
    attribute.setXmlCode( null );
    assertNull( attribute.getXmlCode() );
    attribute.setDescription( null );
    assertNull( attribute.getDescription() );
    attribute.setTooltip( null );
    assertNull( attribute.getTooltip() );
    attribute.setType( -6 );
    assertEquals( -6, attribute.getType() );
    attribute.setParent( null );
    assertNull( attribute.getParent() );
  }
}
