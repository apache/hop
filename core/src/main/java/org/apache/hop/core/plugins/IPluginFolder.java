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

package org.apache.hop.core.plugins;

import org.apache.hop.core.exception.HopFileException;

import java.io.File;

/**
 * Describes a possible location for a plugin
 *
 * @author matt
 */
public interface IPluginFolder {

  /**
   * @return The folder location
   */
  String getFolder();

  /**
   * @return true if the folder needs to be searched for plugin.xml appearances
   */
  boolean isPluginXmlFolder();

  /**
   * @return true if the folder needs to be searched for jar files with plugin annotations
   */
  boolean isPluginAnnotationsFolder();

  /**
   * Find all the jar files in this plugin folder
   *
   * @return The jar files
   * @throws HopFileException In case there is a problem reading
   */
  File[] findJarFiles() throws HopFileException;
}
