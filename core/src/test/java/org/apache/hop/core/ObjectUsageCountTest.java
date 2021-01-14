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

public class ObjectUsageCountTest {
  @Test
  public void testClass() {
    final String name = "Object name";
    final int nrUses = 9;
    ObjectUsageCount count = new ObjectUsageCount( name, nrUses );
    assertSame( name, count.getObjectName() );
    assertEquals( nrUses, count.getNrUses() );
    assertEquals( name + ";" + nrUses, count.toString() );
    assertEquals( nrUses + 1, count.increment() );
    count.reset();
    assertEquals( 0, count.getNrUses() );
    count.setObjectName( null );
    assertNull( count.getObjectName() );
    count.setNrUses( nrUses );
    assertEquals( nrUses, count.getNrUses() );

    assertEquals( -1, count.compare( ObjectUsageCount.fromString( "Obj1;2" ), ObjectUsageCount.fromString( "Obj2" ) ) );
  }
}
