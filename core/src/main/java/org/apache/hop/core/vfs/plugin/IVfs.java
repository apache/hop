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

package org.apache.hop.core.vfs.plugin;

import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.variables.IVariables;

public interface IVfs {
  /**
   * The fixed URL schemes served by {@link #getProvider()}, empty for a plugin serving named
   * connections from the metadata only.
   *
   * @return the fixed URL schemes of this plugin
   */
  String[] getUrlSchemes();

  /**
   * The provider serving the fixed {@link #getUrlSchemes()}. Return {@code null} when there are no
   * fixed schemes: a provider registered under no scheme at all can never be reached, and the file
   * system manager can't close what it doesn't know a scheme for.
   *
   * @return the provider for the fixed schemes, or null if this plugin has none
   */
  FileProvider getProvider();

  /**
   * The providers of the named connections in the metadata, keyed by connection name : the name is
   * the scheme these are registered under.
   *
   * @param variables the variables pointing at the metadata to load the connections from
   * @return a provider per named connection, empty or null if there are none
   */
  Map<String, FileProvider> getProviders(IVariables variables);
}
