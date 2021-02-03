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

import com.fasterxml.jackson.core.JsonFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @param <T>
 */
public class JsonMetadataSerializer<T extends IHopMetadata> implements IHopMetadataSerializer<T> {

  protected IHopMetadataProvider metadataProvider;
  protected String baseFolder;
  protected Class<T> managedClass;
  protected JsonMetadataParser<T> parser;
  protected IVariables variables;
  protected String description;

  public JsonMetadataSerializer( IHopMetadataProvider metadataProvider, String baseFolder, Class<T> managedClass, IVariables variables, String description ) {
    this.metadataProvider = metadataProvider;
    this.baseFolder = baseFolder;
    this.managedClass = managedClass;
    this.parser = new JsonMetadataParser<>( managedClass, metadataProvider );
    this.variables = variables;
    this.description = description;
  }

  @Override public String getDescription() {
    return null;
  }

  @Override public List<T> loadAll() throws HopException {
    List<String> names = listObjectNames();
    Collections.sort( names );
    List<T> list = new ArrayList<>();
    for ( String name : names ) {
      list.add( load( name ) );
    }
    return list;
  }

  @Override public T load( String name ) throws HopException {
    if ( name == null ) {
      throw new HopException( "Error: you need to specify the name of the metadata object to load" );
    }
    if ( !exists( name ) ) {
      return null;
    }

    String filename = calculateFilename( name );

    try {
      // Load the JSON in a streaming fashion so we can parse the properties one by one...
      //
      FileInputStream fileInputStream = null;
      try {
        fileInputStream = new FileInputStream( filename );
        JsonFactory jsonFactory = new JsonFactory();
        com.fasterxml.jackson.core.JsonParser jsonParser = jsonFactory.createParser( fileInputStream );

        jsonParser.nextToken(); // skip {

        T t = parser.loadJsonObject( managedClass, jsonParser );
        inheritVariables(t);
        t.setMetadataProviderName( metadataProvider.getDescription() );
        return t;
      } finally {
        fileInputStream.close();
      }
    } catch ( Exception e ) {
      throw new HopException( "Error loading metadata object '" + name + "' from file '" + filename + "'", e );
    }
  }

  /**
   * If the loaded object implements variables we can inherit from it.
   *
   * @param t
   */
  private void inheritVariables( T t ) {
    if (t instanceof IVariables) {
      ((IVariables)t).initializeFrom( variables );
    }
  }


  @Override public void save( T t ) throws HopException {
    if ( StringUtils.isEmpty(t.getName())) {
      throw new HopException( "Error: To save a metadata object it needs to have a name" );
    }

    String filename = calculateFilename( t.getName() );
    try {

      JSONObject jObject = parser.getJsonObject( t );

      try ( FileWriter file = new FileWriter( filename ) ) {
        String jsonString = jObject.toJSONString();
        Gson gson = ( new GsonBuilder() ).setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse( jsonString );

        file.write( gson.toJson( je ) );
        file.flush();

        // Remember where we saved this...
        //
        t.setMetadataProviderName( getMetadataProvider().getDescription() );
      } catch ( IOException e ) {
        throw new HopException( "Error serializing JSON to file '" + filename + "'", e );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to save object '" + t.getName() + "' to JSON file '" + filename + "'", e );
    }
  }

  private String calculateFilename( String name ) {
    return baseFolder + Const.FILE_SEPARATOR + name + ".json";
  }


  @Override public T delete( String name ) throws HopException {
    if ( name == null ) {
      throw new HopException( "Error: you need to specify the name of the metadata object to delete" );
    }
    if ( !exists( name ) ) {
      throw new HopException( "Error: Object '" + name + "' doesn't exist" );
    }
    T t = load( name );
    String filename = calculateFilename( name );
    boolean deleted = new File( filename ).delete();
    if ( !deleted ) {
      throw new HopException( "Error: Object '" + name + "' could not be deleted, filename : " + filename );
    }
    return t;
  }

  @Override public List<String> listObjectNames() throws HopException {
    File[] files = new File( baseFolder ).listFiles( ( dir, name ) -> name.endsWith( ".json" ) );
    List<String> names = new ArrayList<>();
    for ( File file : files ) {
      names.add( FilenameUtils.removeExtension( file.getName() ) );
    }
    return names;
  }

  @Override public boolean exists( String name ) throws HopException {
    return new File( calculateFilename( name ) ).exists();
  }

  /**
   * Gets managedClass
   *
   * @return value of managedClass
   */
  @Override public Class<T> getManagedClass() {
    return managedClass;
  }

  /**
   * Gets baseFolder
   *
   * @return value of baseFolder
   */
  public String getBaseFolder() {
    return baseFolder;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * @param baseFolder The baseFolder to set
   */
  public void setBaseFolder( String baseFolder ) {
    this.baseFolder = baseFolder;
  }

  /**
   * @param managedClass The managedClass to set
   */
  public void setManagedClass( Class<T> managedClass ) {
    this.managedClass = managedClass;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }
}
