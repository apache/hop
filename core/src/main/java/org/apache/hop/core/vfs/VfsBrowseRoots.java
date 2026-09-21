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

package org.apache.hop.core.vfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.variables.IVariables;

/** Builds the {@code name:///} URI used as the root of a named VFS connection. */
public final class VfsBrowseRoots {

  private VfsBrowseRoots() {}

  /**
   * @param variables variables used to resolve {@code name}, or {@code null} to use it as written
   * @param name connection name, which is also the VFS scheme
   * @return {@code name:///}, or {@code null} when the name is empty
   */
  public static String namedConnectionRoot(IVariables variables, String name) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    String resolved = variables == null ? name : variables.resolve(name);
    if (StringUtils.isBlank(resolved)) {
      return null;
    }
    return resolved + ":///";
  }
}
