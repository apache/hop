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

/**
 * File content attribute names published by Hop VFS providers when a listing already has the
 * values. The file explorer reads these and leaves the cell blank when a provider has neither.
 */
public final class VfsFileAttributes {

  /** Owner name or numeric id. Absent when the provider has no owner. */
  public static final String OWNER = "owner";

  /**
   * Permission text already known to the provider, for example {@code rwxr-xr-x} or {@code 644}.
   */
  public static final String PERMISSIONS = "permissions";

  private VfsFileAttributes() {}
}
