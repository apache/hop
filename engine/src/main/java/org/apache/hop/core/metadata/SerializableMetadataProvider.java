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

package org.apache.hop.core.metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.ByteArrayInputStream;

/**
 * This metadata implementation is an in-memory metadata which serializes using JSON.
 * In other words, JSON is read into memory as a MetaStore and then you can ask to serialize that information to and from JSON.
 */
public class SerializableMetadataProvider extends MemoryMetadataProvider implements IHopMetadataProvider {
  public SerializableMetadataProvider() {
    super();
  }

  /**
   * Create a copy of all elements in an existing metadata.
   *
   * @param source the source store to copy over
   */
  public SerializableMetadataProvider( IHopMetadataProvider source) throws HopException {

    // What is the list of available classes?
    //
    for (Class<IHopMetadata> metadataClass : source.getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> sourceSerializer = source.getSerializer( metadataClass );
      IHopMetadataSerializer<IHopMetadata> targetSerializer = getSerializer( metadataClass );

      // Loop over the available objects of the class and copy the information over.
      //
      for (String name : sourceSerializer.listObjectNames()) {
        targetSerializer.save( sourceSerializer.load( name ) );
      }
    }
  }

  public String toJson() throws HopException {

    JSONObject jStore = new JSONObject();

    // What is the list of available classes?
    //
    for (Class<IHopMetadata> metadataClass : getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> serializer = getSerializer( metadataClass );
      HopMetadata hopMetadata = metadataClass.getAnnotation( HopMetadata.class );
      if (hopMetadata==null) {
        throw new HopException("Error: class "+metadataClass+" is not annotated with "+HopMetadata.class.getName());
      }
      String classKey = hopMetadata.key();

      JSONArray jClass = new JSONArray();

      JsonMetadataParser parser = new JsonMetadataParser( metadataClass, this );

      // Loop over the available objects of the class and copy the information over to the JSON store.
      // They are stored under plugin IDs...
      //
      for (String name : serializer.listObjectNames()) {
        Object object = serializer.load( name );
        JSONObject jObject = parser.getJsonObject( (IHopMetadata) object );
        jClass.add(jObject);
      }

      jStore.put( classKey, jClass );

    }
    return jStore.toJSONString();
  }

  public SerializableMetadataProvider( String storeJson) throws ParseException, HopException {
    this();

    try {

      ByteArrayInputStream inputStream = null;
      try {
        inputStream = new ByteArrayInputStream( storeJson.getBytes( Const.XML_ENCODING ) );

        JsonFactory jsonFactory = new JsonFactory();
        com.fasterxml.jackson.core.JsonParser jsonParser = jsonFactory.createParser( inputStream );

        // Loop over the classes until there's no more left
        //
        jsonParser.nextToken(); // skip {
        while ( jsonParser.nextToken() != JsonToken.END_OBJECT ) {

          String classKey = jsonParser.getText();
          Class<IHopMetadata> managedClass = getMetadataClassForKey( classKey );

          JsonMetadataParser<IHopMetadata> metadataParser = new JsonMetadataParser<>( managedClass, this );

          IHopMetadataSerializer<IHopMetadata> serializer = getSerializer( managedClass );

          // Loop over the metadata objects in the JSON for the given class...
          //
          jsonParser.nextToken(); // skip {
          while ( jsonParser.nextToken() != JsonToken.END_ARRAY ) {
            IHopMetadata object = metadataParser.loadJsonObject( managedClass, jsonParser );
            serializer.save( object );
          }
        }
      } finally {
        if (inputStream!=null) {
          inputStream.close();
        }
      }
    } catch(Exception e) {
      throw new HopException("Error reading metadata from JSON", e);
    }
  }
  

}
