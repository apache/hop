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

package org.apache.hop.git.model.repository;

import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "git",
    name = "Git Repository",
    description = "This defines a Git repository",
    image = "git_icon.svg")
public class GitRepository extends HopMetadataBase implements Cloneable, IHopMetadata {

  public static final String EXTENSION_POINT_ID_GIT_REPOSITORY_CREATION = "GitRepositoryCreate";

  @HopMetadataProperty(key = "description")
  private String description;

  @HopMetadataProperty(key = "directory")
  private String directory;

  public GitRepository() {
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, null, EXTENSION_POINT_ID_GIT_REPOSITORY_CREATION, this );
    } catch (Exception e) {
      throw new RuntimeException(
          "Error calling extension point " + EXTENSION_POINT_ID_GIT_REPOSITORY_CREATION, e);
    }
  }

  /**
   * Get a directory path that can contain variables.
   *
   * @return directory path
   */
  public String getDirectory() {
    return directory;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Get a directory path in the current environment. Unlike {@link #getDirectory()}, all variables
   * are resolved.
   *
   * @return directory path
   */
  public String getPhysicalDirectory(IVariables variables) {
    return variables.resolve(directory);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
