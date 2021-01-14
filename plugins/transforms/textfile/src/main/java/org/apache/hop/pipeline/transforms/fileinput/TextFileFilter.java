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

package org.apache.hop.pipeline.transforms.fileinput;

/**
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
@Deprecated
public class TextFileFilter implements Cloneable {
  /**
   * The position of the occurrence of the filter string to check at
   */
  private int filterPosition;

  /**
   * The string to filter on
   */
  private String filterString;

  /**
   * True if we want to stop when we reach a filter line
   */
  private boolean filterLastLine;

  /**
   * True if we want to match only this lines
   */
  private boolean filterPositive;

  /**
   * @param filterPosition The position of the occurrence of the filter string to check at
   * @param filterString   The string to filter on
   * @param filterLastLine True if we want to stop when we reach a filter string on the specified position False if you just want to
   *                       skip the line.
   * @param filterPositive True if we want to get only lines that match this string
   */
  public TextFileFilter( int filterPosition, String filterString, boolean filterLastLine, boolean filterPositive ) {
    this.filterPosition = filterPosition;
    this.filterString = filterString;
    this.filterLastLine = filterLastLine;
    this.filterPositive = filterPositive;
  }

  public TextFileFilter() {
  }

  public Object clone() {
    try {
      Object retval = super.clone();
      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * @return Returns the filterLastLine.
   */
  public boolean isFilterLastLine() {
    return filterLastLine;
  }

  /**
   * @param filterLastLine The filterLastLine to set.
   */
  public void setFilterLastLine( boolean filterLastLine ) {
    this.filterLastLine = filterLastLine;
  }

  /**
   * @return Returns the filterPositive.
   */
  public boolean isFilterPositive() {
    return filterPositive;
  }

  /**
   * @param filterPositive The filterPositive to set.
   */
  public void setFilterPositive( boolean filterPositive ) {
    this.filterPositive = filterPositive;
  }

  /**
   * @return Returns the filterPosition.
   */
  public int getFilterPosition() {
    return filterPosition;
  }

  /**
   * @param filterPosition The filterPosition to set.
   */
  public void setFilterPosition( int filterPosition ) {
    this.filterPosition = filterPosition;
  }

  /**
   * @return Returns the filterString.
   */
  public String getFilterString() {
    return filterString;
  }

  /**
   * @param filterString The filterString to set.
   */
  public void setFilterString( String filterString ) {
    this.filterString = filterString;
  }
}
