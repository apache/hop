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

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * A metadata object whose files can be browsed in the VFS File Explorer.
 *
 * <p>The root is {@code name:///}. That is the provider root: HDFS applies its default root and
 * Databricks applies its default base path when the path is {@code /}. Those configured paths are
 * not appended here, or they would be applied twice.
 */
public interface IVfsBrowseLocation {

  /**
   * Root URI to open in the VFS File Explorer, or {@code null} when the connection has no name.
   *
   * @param variables variables used to resolve the connection name
   * @return {@code name:///}, or {@code null}
   */
  default String getBrowseRoot(IVariables variables) {
    String name = this instanceof IHopMetadata metadata ? metadata.getName() : null;
    return VfsBrowseRoots.namedConnectionRoot(variables, name);
  }
}
