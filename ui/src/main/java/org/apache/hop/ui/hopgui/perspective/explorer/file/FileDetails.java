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
 *
 */

package org.apache.hop.ui.hopgui.perspective.explorer.file;

import org.apache.hop.ui.hopgui.file.IHopFileType;

public class FileDetails {
  private String path;
  private IHopFileType hopFileType;

  public FileDetails( String path, IHopFileType hopFileType ) {
    this.path = path;
    this.hopFileType = hopFileType;
  }

  /**
   * Gets path
   *
   * @return value of path
   */
  public String getPath() {
    return path;
  }

  /**
   * @param path The path to set
   */
  public void setPath( String path ) {
    this.path = path;
  }

  /**
   * Gets hopFileType
   *
   * @return value of hopFileType
   */
  public IHopFileType getHopFileType() {
    return hopFileType;
  }

  /**
   * @param hopFileType The hopFileType to set
   */
  public void setHopFileType( IHopFileType hopFileType ) {
    this.hopFileType = hopFileType;
  }
}
