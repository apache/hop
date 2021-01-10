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

package org.apache.hop.pipeline.engine;

import java.util.Objects;

public class EngineMetric implements IEngineMetric {
  private String code;
  private String header;
  private String tooltip;
  private String displayPriority;
  private boolean numeric;

  public EngineMetric() {
  }

  public EngineMetric( String code, String header, String tooltip, String displayPriority, boolean numeric ) {
    this.code = code;
    this.header = header;
    this.tooltip = tooltip;
    this.displayPriority = displayPriority;
    this.numeric = numeric;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    EngineMetric that = (EngineMetric) o;
    return Objects.equals( code, that.code );
  }

  @Override public int hashCode() {
    return Objects.hash( code );
  }

  /**
   * Gets code
   *
   * @return value of code
   */
  @Override public String getCode() {
    return code;
  }

  /**
   * @param code The code to set
   */
  public void setCode( String code ) {
    this.code = code;
  }

  /**
   * Gets header
   *
   * @return value of header
   */
  @Override public String getHeader() {
    return header;
  }

  /**
   * @param header The header to set
   */
  public void setHeader( String header ) {
    this.header = header;
  }

  /**
   * Gets tooltip
   *
   * @return value of tooltip
   */
  @Override public String getTooltip() {
    return tooltip;
  }

  /**
   * @param tooltip The tooltip to set
   */
  public void setTooltip( String tooltip ) {
    this.tooltip = tooltip;
  }

  /**
   * Gets displayPriority
   *
   * @return value of displayPriority
   */
  @Override public String getDisplayPriority() {
    return displayPriority;
  }

  /**
   * @param displayPriority The displayPriority to set
   */
  public void setDisplayPriority( String displayPriority ) {
    this.displayPriority = displayPriority;
  }

  /**
   * Gets numeric
   *
   * @return value of numeric
   */
  @Override public boolean isNumeric() {
    return numeric;
  }

  /**
   * @param numeric The numeric to set
   */
  public void setNumeric( boolean numeric ) {
    this.numeric = numeric;
  }
}
