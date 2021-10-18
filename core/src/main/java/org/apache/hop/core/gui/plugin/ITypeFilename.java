/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.gui.plugin;

import org.apache.hop.metadata.api.IHopMetadata;

/** Provide information which allows browsing capabilities for the Filename widget sub-type */
public interface ITypeFilename {

  /**
   * For WidgetSubType.Filename
   *
   * <p>Returns the default file extension in lowercase prefixed with dot (.xxx) for this file type.
   *
   * @return The default file extension
   */
  String getDefaultFileExtension();

  /**
   * For WidgetSubType.Filename
   *
   * @return The file type extensions.
   */
  String[] getFilterExtensions();

  /**
   * For WidgetSubType.Filename
   *
   * @return The file names (matching the extensions)
   */
  String[] getFilterNames();
}
