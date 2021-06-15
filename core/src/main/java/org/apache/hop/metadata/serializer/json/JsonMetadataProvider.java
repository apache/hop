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

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.BaseMetadataProvider;

import java.io.File;

public class JsonMetadataProvider extends BaseMetadataProvider implements IHopMetadataProvider {

  public static final String DEFAULT_DESCRIPTION = "JSON metadata";
  private ITwoWayPasswordEncoder twoWayPasswordEncoder;
  private String baseFolder;

  public JsonMetadataProvider() {
    super( Variables.getADefaultVariableSpace(), DEFAULT_DESCRIPTION );
    twoWayPasswordEncoder = Encr.getEncoder();
    if (twoWayPasswordEncoder==null) {
      twoWayPasswordEncoder = new HopTwoWayPasswordEncoder();
    }
    baseFolder="metadata";
  }

  public JsonMetadataProvider( ITwoWayPasswordEncoder twoWayPasswordEncoder, String baseFolder, IVariables variables ) {
    super(variables, DEFAULT_DESCRIPTION+" in folder "+baseFolder);
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
    this.baseFolder = baseFolder;
  }

  @Override public String getDescription() {
    return calculateDescription();
  }

  private String calculateDescription() {
    return "JSON metadata in folder "+baseFolder;
  }

  @Override public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer( Class<T> managedClass ) throws HopException {
    if (managedClass==null) {
      throw new HopException("You need to specify the class to serialize");
    }

    // Is this a metadata class?
    //
    HopMetadata hopMetadata = managedClass.getAnnotation( HopMetadata.class );
    if (hopMetadata==null) {
      throw new HopException("To serialize class "+managedClass.getClass().getName()+" it needs to have annotation "+HopMetadata.class.getName());
    }
    String classFolder = Const.NVL(hopMetadata.key(), hopMetadata.name());
    String serializerBaseFolderName = baseFolder + (baseFolder.endsWith( Const.FILE_SEPARATOR ) ? "" : Const.FILE_SEPARATOR) + classFolder;

    // Check if the folder exists...
    //
    File serializerBaseFolder = new File(serializerBaseFolderName);
    if (!serializerBaseFolder.exists()) {
      if (!serializerBaseFolder.mkdirs()) {
        throw new HopException("Unable to create folder '"+serializerBaseFolderName+"'to store JSON serialized objects in from class "+managedClass.getName());
      }
    }

    return new JsonMetadataSerializer<>( this, serializerBaseFolderName, managedClass, variables, hopMetadata.name() );
  }

  /**
   * Gets twoWayPasswordEncoder
   *
   * @return value of twoWayPasswordEncoder
   */
  @Override public ITwoWayPasswordEncoder getTwoWayPasswordEncoder() {
    return twoWayPasswordEncoder;
  }

  /**
   * @param twoWayPasswordEncoder The twoWayPasswordEncoder to set
   */
  public void setTwoWayPasswordEncoder( ITwoWayPasswordEncoder twoWayPasswordEncoder ) {
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
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
   * @param baseFolder The baseFolder to set
   */
  public void setBaseFolder( String baseFolder ) {
    this.baseFolder = baseFolder;
    setDescription( calculateDescription() );
  }
}
