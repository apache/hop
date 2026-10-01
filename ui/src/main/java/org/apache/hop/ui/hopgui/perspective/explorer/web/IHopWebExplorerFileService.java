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

package org.apache.hop.ui.hopgui.perspective.explorer.web;

import org.apache.hop.core.variables.IVariables;

/**
 * Builds a same-origin URL that the Hop Web explorer browser widget can load so relative CSS,
 * images, and links resolve against the file's directory. Implemented in the RAP module.
 */
public interface IHopWebExplorerFileService {

  /**
   * Context-relative URL for {@code vfsFilename} under the current explorer root, or {@code null}
   * when the file cannot be served (not under the root, unknown extension, no RAP session).
   *
   * @param vfsFilename HopVfs path of the file to open
   * @param variables variables used to resolve the path
   * @return a path starting with {@code /} (never a {@code http://} URL), or {@code null}
   */
  String urlFor(String vfsFilename, IVariables variables);
}
