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

public class JarFilePlugin {
  private URL jarFile;
  private URL pluginFolder;
  private String className;

  /**
   * @param className
   * @param jarFile
   * @param pluginFolder
   */
  public JarFilePlugin( String className, URL jarFile, URL pluginFolder ) {
    this.className = className;
    this.jarFile = jarFile;
    this.pluginFolder = pluginFolder;
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

  /**
   * @param jarFile the jarFile to set
   */
  public void setJarFile( URL jarFile ) {
    this.jarFile = jarFile;
  }

  public URL getPluginFolder() {
    return pluginFolder;
  }

  public void setPluginFolder( URL pluginFolder ) {
    this.pluginFolder = pluginFolder;
  }

  public String getClassName() {
    return className;
  }

}
