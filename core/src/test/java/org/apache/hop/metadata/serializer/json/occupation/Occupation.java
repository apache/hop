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

package org.apache.hop.metadata.serializer.json.occupation;

import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

import java.util.Objects;

@HopMetadata(
  name = "Occupation",
  key = "occupation"
)
public class Occupation extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty
  private String description;

  @HopMetadataProperty
  private int startYear;

  public Occupation() {
  }

  public Occupation( String name, String description, int startYear ) {
    this.name = name;
    this.description = description;
    this.startYear = startYear;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    Occupation that = (Occupation) o;
    return startYear == that.startYear &&
      Objects.equals( name, that.name ) &&
      Objects.equals( description, that.description );
  }

  @Override public int hashCode() {
    return Objects.hash( name, description, startYear );
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets startYear
   *
   * @return value of startYear
   */
  public int getStartYear() {
    return startYear;
  }

  /**
   * @param startYear The startYear to set
   */
  public void setStartYear( int startYear ) {
    this.startYear = startYear;
  }
}
