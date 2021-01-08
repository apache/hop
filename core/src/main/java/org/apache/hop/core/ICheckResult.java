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

package org.apache.hop.core;

public interface ICheckResult {

  int TYPE_RESULT_NONE = 0;

  int TYPE_RESULT_OK = 1;

  int TYPE_RESULT_COMMENT = 2;

  int TYPE_RESULT_WARNING = 3;

  int TYPE_RESULT_ERROR = 4;

  /**
   * @return The type of the Check Result (0-4)
   */
  int getType();

  /**
   * @return The internationalized type description
   */
  String getTypeDesc();

  /**
   * @return The text of the check result.
   */
  String getText();

  /**
   * @return The source of the check result
   */
  ICheckResultSource getSourceInfo();

  /**
   * @return String description of the check result
   */
  @Override String toString();

  /**
   * @return The component-specific result code.
   */
  String getErrorCode();

  /**
   * Sets the component-specific result/error code.
   *
   * @param errorCode Unchecked string that can be used for validation
   */
  void setErrorCode( String errorCode );

  /**
   * Sets the check-result type
   *
   * @param value The type from 0-4
   */
  void setType( int value );

  /**
   * Sets the text for the check-result
   *
   * @param value
   */
  void setText( String value );

}
