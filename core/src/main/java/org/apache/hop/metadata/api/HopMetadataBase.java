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

package org.apache.hop.metadata.api;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

public class HopMetadataBase implements IHopMetadata {

  /** All metadata objects have a name to uniquely identify it. */
  @HopMetadataProperty protected String name;

  /**
   * The metadata provider name is optionally used at runtime to figure out where the metadata came from.
   * Optionally used by plugins. It's volatile because it's never persisted.
   */
  @JsonIgnore
  protected volatile String metadataProviderName;

  public HopMetadataBase() {
  }

  public HopMetadataBase( String name ) {
    this();
    this.name = name;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    HopMetadataBase that = (HopMetadataBase) o;
    return name.equals( that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name );
  }


  /**
   * Get the name of the metadata object.
   *
   * @return The name uniquely identifying the metadata object
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name.
   *
   * @param name The name uniquely identifying the metadata object
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get the source of the metadata object. Plugins can use this to mix metadata from various
   * sources. It helps to figure out where this object originated.
   *
   * @return The source of metadata or null if it's not specified.
   */
  public String getMetadataProviderName() {
    return metadataProviderName;
  }

  /**
   * Set the source of the metadata. Plugins can use this to mix metadata from various sources.
   * helps to figure out where this object originated.
   *
   * @param metadataProviderName The source of metadata or null if it's not specified
   */
  public void setMetadataProviderName( String metadataProviderName ) {
    this.metadataProviderName = metadataProviderName;
  }
}
