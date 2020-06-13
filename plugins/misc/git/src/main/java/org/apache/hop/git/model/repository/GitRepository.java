/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.model.repository;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
  key = "git",
  name = "Git Repository",
  description = "This defines a Git repository",
  iconImage = "git_icon.svg"
)
public class GitRepository extends Variables implements Cloneable, IVariables, IHopMetadata {

  @HopMetadataProperty( key = "name" )
  private String name;

  @HopMetadataProperty( key = "description" )
  private String description;

  @HopMetadataProperty( key = "directory" )
  private String directory;

  public GitRepository() {
  }

  /**
   * Get a directory path that can contain variables.
   *
   * @return directory path
   */
  public String getDirectory() {
    return directory;
  }

  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Get a directory path in the current environment.
   * Unlike {@link #getDirectory()}, all variables are resolved.
   *
   * @return directory path
   */
  public String getPhysicalDirectory() {
    return environmentSubstitute( directory );
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }
}
