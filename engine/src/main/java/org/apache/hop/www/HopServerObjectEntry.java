/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.www;

import java.util.Comparator;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A carte object entry in the transformation or job maps
 *
 * @author matt
 *
 */
@XmlRootElement
public class HopServerObjectEntry implements Comparator<HopServerObjectEntry>, Comparable<HopServerObjectEntry> {
  private String name;
  private String id;

  public HopServerObjectEntry() {
  }

  public HopServerObjectEntry(String name, String id ) {
    this.name = name;
    this.id = id;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( !( obj instanceof HopServerObjectEntry) ) {
      return false;
    }
    if ( obj == this ) {
      return true;
    }

    HopServerObjectEntry entry = (HopServerObjectEntry) obj;

    return entry.getId().equals( id );
  }

  public int hashCode() {
    return id.hashCode();
  }

  public int compare(HopServerObjectEntry o1, HopServerObjectEntry o2 ) {
    int cmpName = o1.getName().compareTo( o2.getName() );
    if ( cmpName != 0 ) {
      return cmpName;
    }

    return o1.getId().compareTo( o2.getId() );
  }

  public int compareTo( HopServerObjectEntry o ) {
    return compare( this, o );
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name
   *          the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id
   *          the id to set
   */
  public void setId( String id ) {
    this.id = id;
  }
}
