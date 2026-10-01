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

package org.apache.hop.projects.resources;

import lombok.Value;

/**
 * Minimum free disk space required for a (possibly variable-containing) local folder path.
 *
 * <p>{@link #minFreeBytes} holds the configured expression in <strong>mebibytes</strong> (as
 * entered in the UI). It may contain Hop variables. Resolve and convert with {@code
 * Const.toLongExpanded(variables.resolve(...))} before comparing to {@link
 * java.io.File#getUsableSpace()}.
 */
@Value
public class DiskSpaceRequirement {
  /** Folder path; may contain Hop variables until resolved. */
  String path;

  /**
   * Minimum free space in MiB as a string expression (literals, expanded numbers, or Hop
   * variables). Not pre-converted to bytes.
   */
  String minFreeBytes;
}
