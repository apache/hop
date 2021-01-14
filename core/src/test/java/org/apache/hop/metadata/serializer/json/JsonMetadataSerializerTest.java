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

package org.apache.hop.metadata.serializer.json;

import junit.framework.TestCase;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.occupation.Occupation;
import org.apache.hop.metadata.serializer.json.person.Address;
import org.apache.hop.metadata.serializer.json.person.City;
import org.apache.hop.metadata.serializer.json.person.Person;
import org.apache.hop.metadata.serializer.json.person.interest.Cooking;
import org.apache.hop.metadata.serializer.json.person.interest.Music;
import org.apache.hop.metadata.serializer.json.person.interest.Running;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JsonMetadataSerializerTest extends TestCase {

  private JsonMetadataProvider metadataProvider;


  @Override protected void setUp() throws Exception {
    String baseFolder = System.getProperty( "java.io.tmpdir" ) + Const.FILE_SEPARATOR + "metadata"; // UUID.randomUUID();
    metadataProvider = new JsonMetadataProvider( new HopTwoWayPasswordEncoder(), baseFolder, Variables.getADefaultVariableSpace() );
  }

  @Override protected void tearDown() throws Exception {
    // TODO: Remove the temp folder
    //

  }

  @Test
  public void testPersonSerialization() throws HopException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("attribute1", "value1");
    attributes.put("attribute2", "value2");
    attributes.put("attribute3", "value3");

    Occupation occupation1 = new Occupation( "Occupation1", "Description of occupation1", 2001 );

    Person person = new Person( "Person", "51", new Address( "Street", "123", new City( "12345", "McCity" ) ),
      new Music("Music", "Love Music"),
      Arrays.asList(new Cooking("Cooking", "Cooking is great"), new Running("Running", "Keep on running") ),
      attributes,
      occupation1
      );

    assertNull( person.getSideInterest() );

    IHopMetadataSerializer<Occupation> occupationSerializer = metadataProvider.getSerializer( Occupation.class );
    IHopMetadataSerializer<Person> personSerializer = metadataProvider.getSerializer( Person.class );

    occupationSerializer.save( occupation1 );
    personSerializer.save( person );

    Person anotherPerson = personSerializer.load( "Person" );

    assertEquals( person, anotherPerson );
  }

}
