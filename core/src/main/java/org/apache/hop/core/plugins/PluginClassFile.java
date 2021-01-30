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

import java.net.URL;

public class PluginClassFile {
  private URL jarFile;
  private URL pluginFolder;
  private String className;

  /**
   * @param className
   * @param jarFile
   * @param folder
   */
  public PluginClassFile( String className, URL jarFile, URL folder ) {
    this.className = className;
    this.jarFile = jarFile;
    this.pluginFolder = folder;
  }

  @Override
  public String toString() {
    return jarFile.toString();
  }

  /**
   * @return the jarFile
   */
  public URL getJarFile() {
    return jarFile;
  }

  public URL getFolder() {
    return pluginFolder;
  }

  public String getClassName() {
    return className;
  }

}
