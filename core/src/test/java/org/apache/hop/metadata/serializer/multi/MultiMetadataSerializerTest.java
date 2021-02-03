/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.metadata.serializer.multi;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

import java.util.List;

public class MultiMetadataSerializerTest extends MetadataTestBase {

  protected MetadataType1 object1;
  protected MetadataType2 object2;
  protected MetadataType1 object3;
  protected MetadataType2 object4;
  protected MetadataType1 object5;

  protected void setUp() throws Exception {
    super.setUp();

    // Throw a few objects in providers 1, 2 and 3
    //
    object1 = new MetadataType1("t1o1", "d1", "v1");
    object2 = new MetadataType2("t2o2", "d2", "v2");
    object3 = new MetadataType1("t1o3", "d3", "v3");
    object4 = new MetadataType2("t2o4", "d4", "v4");
    object5 = new MetadataType1("t1o1", "d5", "v5");

    Class<MetadataType1> type1 = MetadataType1.class;
    Class<MetadataType2> type2 = MetadataType2.class;

    provider1.getSerializer( type1 ).save(object1);
    provider2.getSerializer( type2 ).save(object2);
    provider3.getSerializer( type1 ).save(object3);
    provider3.getSerializer( type2 ).save(object4);
    provider3.getSerializer( type1 ).save(object5); // same name as in provider 1
  }

  public void testLoad() throws HopException {
    IHopMetadataSerializer<MetadataType1> s1 = multiMetadataProvider.getSerializer( MetadataType1.class );
    IHopMetadataSerializer<MetadataType2> s2 = multiMetadataProvider.getSerializer( MetadataType2.class );

    // This objects is loaded from provider 3, not provider 1
    //
    MetadataType1 m1 = s1.load( "t1o1" );
    assertEquals( "t1o1", m1.getName() );
    assertEquals( "d5", m1.getDescription() );
    assertEquals( "v5", m1.getValue() );

    MetadataType2 m2 = s2.load( "t2o2" );
    assertEquals( "t2o2", m2.getName() );
    assertEquals( "d2", m2.getDescription() );
    assertEquals( "v2", m2.getValue() );

    MetadataType1 m3 = s1.load( "t1o3" );
    assertEquals( "t1o3", m3.getName() );
    assertEquals( "d3", m3.getDescription() );
    assertEquals( "v3", m3.getValue() );

    MetadataType2 m4 = s2.load( "t2o4" );
    assertEquals( "t2o4", m4 .getName() );
    assertEquals( "d4", m4 .getDescription() );
    assertEquals( "v4", m4 .getValue() );
  }

  public void testSave() throws HopException {

    MetadataType1 m6 = new MetadataType1( "t1o6", "d6", "v6" );
    multiMetadataProvider.getSerializer( MetadataType1.class ).save( m6 );

    // See if it ended up in provider3 as expected...
    //
    assertEquals( provider3.getDescription(), m6.getMetadataProviderName());

    // Load the object back from provider3
    //
    MetadataType1 t1o6 = provider3.getSerializer( MetadataType1.class ).load( "t1o6" );
    assertEquals( m6, t1o6 );

    // Now we want to save in provider1...
    // We do this by specifying the metadata source
    //
    MetadataType1 m7 = new MetadataType1( "t1o7", "d7", "v7" );
    m7.setMetadataProviderName( "Provider1" );
    multiMetadataProvider.getSerializer( MetadataType1.class ).save( m7 );

    // See if it ended up in provider3 as expected...
    //
    assertEquals( provider1.getDescription(), m7.getMetadataProviderName());

    // Load the object back from provider3
    //
    MetadataType1 t1o7 = provider1.getSerializer( MetadataType1.class ).load( "t1o7" );
    assertEquals( m7, t1o7 );
  }

  public void testDelete() {

  }

  public void testListObjectNames() throws HopException {
    IHopMetadataSerializer<MetadataType1> s1 = multiMetadataProvider.getSerializer( MetadataType1.class );
    IHopMetadataSerializer<MetadataType2> s2 = multiMetadataProvider.getSerializer( MetadataType2.class );

    List<String> names1  = s1.listObjectNames();
    assertEquals(2, names1.size());
    assertTrue(names1.contains("t1o1"));
    assertTrue(names1.contains("t1o3"));
    List<String> names2  = s2.listObjectNames();
    assertEquals(2, names2.size());
    assertTrue(names2.contains("t2o2"));
    assertTrue(names2.contains("t2o4"));
  }

  public void testExists() throws HopException {
    IHopMetadataSerializer<MetadataType1> s1 = multiMetadataProvider.getSerializer( MetadataType1.class );
    IHopMetadataSerializer<MetadataType2> s2 = multiMetadataProvider.getSerializer( MetadataType2.class );

    assertTrue(s1.exists( "t1o1" ));
    assertTrue(s2.exists( "t2o2" ));
    assertTrue(s1.exists( "t1o3" ));
    assertTrue(s2.exists( "t2o4" ));

    assertFalse(s2.exists( "t1o1" ));
    assertFalse(s1.exists( "t2o2" ));
    assertFalse(s2.exists( "t1o3" ));
    assertFalse(s1.exists( "t2o4" ));

    assertFalse(s1.exists( "t8o8" ));
    assertFalse(s2.exists( "t9o9" ));
  }

  public void testLoadAll() {}

}
