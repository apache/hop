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

package org.apache.hop.beam.core.shared;

import java.io.Serializable;

public class VariableValue implements Serializable {

  private String variable;
  private String value;

  public VariableValue() {
  }

  public VariableValue( String variable, String value ) {
    this.variable = variable;
    this.value = value;
  }

  /**
   * Gets variable
   *
   * @return value of variable
   */
  public String getVariable() {
    return variable;
  }

  /**
   * @param variable The variable to set
   */
  public void setVariable( String variable ) {
    this.variable = variable;
  }

  /**
   * Gets value
   *
   * @return value of value
   */
  public String getValue() {
    return value;
  }

  /**
   * @param value The value to set
   */
  public void setValue( String value ) {
    this.value = value;
  }
}
