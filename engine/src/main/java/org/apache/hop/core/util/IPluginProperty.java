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

package org.apache.hop.core.util;

import org.w3c.dom.Node;

import java.util.prefs.Preferences;

public interface IPluginProperty {

  /**
   * The default string value.
   */
  String DEFAULT_STRING_VALUE = "";

  /**
   * The default value.
   */
  Boolean DEFAULT_BOOLEAN_VALUE = Boolean.FALSE;

  /**
   * The default integer value.
   */
  Integer DEFAULT_INTEGER_VALUE = 0;

  /**
   * The default double value.
   */
  Double DEFAULT_DOUBLE_VALUE = 0.0;

  /**
   * The true value.
   */
  String BOOLEAN_STRING_TRUE = "Y";

  /**
   * @return true if value not null or 'false'.
   */
  boolean evaluate();

  /**
   * @param node preferences node
   */
  void saveToPreferences( final Preferences node );

  /**
   * @param node preferences node.
   */
  void readFromPreferences( final Preferences node );

  /**
   * @param builder builder to append to.
   */
  void appendXml( final StringBuilder builder );

  /**
   * @param node the node.
   */
  void loadXml( final Node node );

}
