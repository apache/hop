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

package org.apache.hop.ui.core.gui;

import org.apache.hop.core.util.Utils;

/**
 * This keeps track of the currently active namespace for all the current. It makes it easy to see
 * which namespace is active. A namespace is used for plugins like Environment to set the active
 * environment. The standard for HopGUI is hop-gui and for Translator is is hop-translator
 */
public class HopNamespace {

  private static HopNamespace instance;

  private String namespace;

  private HopNamespace() {}

  public static final HopNamespace getInstance() {
    if (instance == null) {
      instance = new HopNamespace();
    }
    return instance;
  }

  /**
   * Gets namespace
   *
   * @return value of namespace
   */
  public static final String getNamespace() {
    String namespace = getInstance().namespace;
    if (Utils.isEmpty(namespace)) {
      throw new RuntimeException("Please set a namespace before using one");
    }
    return getInstance().namespace;
  }

  /** @param namespace The namespace to set */
  public static final void setNamespace(String namespace) {
    getInstance().namespace = namespace;
  }
}
