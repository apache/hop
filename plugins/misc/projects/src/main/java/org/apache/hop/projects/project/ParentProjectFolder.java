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

package org.apache.hop.projects.project;

import lombok.Getter;
import lombok.Setter;

/**
 * A folder in a parent project to copy into a child project home. Stored in {@code
 * project-config.json} as entries of {@code parentProjectFolders}.
 */
@Getter
@Setter
public class ParentProjectFolder {

  /** Path relative to the parent project home. Empty or {@code .} means the parent root. */
  private String folder;

  /**
   * Copy when the destination folder is missing or empty. Subsequent enables skip this mapping
   * unless {@link #copyOnEnable} is also true.
   */
  private boolean copyOnce;

  /** Copy every time the child project is enabled. */
  private boolean copyOnEnable;

  /** Replace existing destination files when this mapping runs. */
  private boolean overwrite;

  /**
   * Java regular expression (Hop wildcard) matched against the file base name and the path relative
   * to the copied folder. Empty means no extra exclusions.
   */
  private String exclusionWildcard;
}
