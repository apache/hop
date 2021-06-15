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

package org.apache.hop.metadata.serializer.memory;

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

import java.util.HashMap;
import java.util.Map;

public class MemoryMetadataProvider extends BaseMetadataProvider implements IHopMetadataProvider {

  public static final String DEFAULT_DESCRIPTION = "In memory metadata";

  private Map<String, IHopMetadataSerializer<IHopMetadata>> serializerMap;
  private ITwoWayPasswordEncoder twoWayPasswordEncoder;

  public MemoryMetadataProvider() {
    super( Variables.getADefaultVariableSpace(), DEFAULT_DESCRIPTION );
    this.serializerMap = new HashMap<>();
    twoWayPasswordEncoder = Encr.getEncoder();
    if (twoWayPasswordEncoder==null) {
      twoWayPasswordEncoder = new HopTwoWayPasswordEncoder();
    }
  }

  public MemoryMetadataProvider( ITwoWayPasswordEncoder twoWayPasswordEncoder, IVariables variables ) {
    super(variables, DEFAULT_DESCRIPTION);
    this.serializerMap = new HashMap<>();
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
  }


  @Override public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer( Class<T> managedClass ) throws HopException {
    IHopMetadataSerializer<IHopMetadata> serializer = serializerMap.get( managedClass.getName() );
    if (serializer==null) {
      HopMetadata hopMetadata = managedClass.getAnnotation( HopMetadata.class );
      String description = managedClass.getSimpleName();
      if (hopMetadata!=null) {
        description = hopMetadata.name();
      }
      serializer = (IHopMetadataSerializer<IHopMetadata>) new MemoryMetadataSerializer<>( this, managedClass, variables, description );
      serializerMap.put( managedClass.getName(), serializer);
    }

    return (IHopMetadataSerializer<T>) serializer;
  }

  @Override public ITwoWayPasswordEncoder getTwoWayPasswordEncoder() {
    return twoWayPasswordEncoder;
  }

  /**
   * Gets serializerMap
   *
   * @return value of serializerMap
   */
  public Map<String, IHopMetadataSerializer<IHopMetadata>> getSerializerMap() {
    return serializerMap;
  }

  /**
   * @param serializerMap The serializerMap to set
   */
  public void setSerializerMap(
    Map<String, IHopMetadataSerializer<IHopMetadata>> serializerMap ) {
    this.serializerMap = serializerMap;
  }

  /**
   * @param twoWayPasswordEncoder The twoWayPasswordEncoder to set
   */
  public void setTwoWayPasswordEncoder( ITwoWayPasswordEncoder twoWayPasswordEncoder ) {
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
  }
}
