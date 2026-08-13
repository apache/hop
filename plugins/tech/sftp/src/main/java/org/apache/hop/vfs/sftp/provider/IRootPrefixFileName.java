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
package org.apache.hop.vfs.sftp.provider;

/**
 * A file name which remembers that its path began with a second slash.
 *
 * <p>POSIX gives a path starting with exactly two slashes a meaning of its own, and z/OS uses that
 * meaning: {@code /A.B.C} is a USS path and {@code //A.B.C} is an MVS data set. Three or more
 * slashes are the same as one, again per POSIX.
 *
 * <p>The extra slash cannot be kept in the path. Commons VFS normalises a path element by element
 * and drops the empty element a {@code //} amounts to, and it does that again on every child
 * listing and every {@link org.apache.commons.vfs2.FileName#getParent()}, because {@code
 * createName(absPath, type)} is handed an already normalised path. Only the root survives being
 * passed along, which is why the Windows parser hoists a UNC share into the root instead of
 * guarding it in the path. This is the same hoist: the path stays an ordinary {@code /A.B.C} and
 * the prefix rides along in the root.
 */
public interface IRootPrefixFileName {

  /**
   * @return {@code "/"} when the path is rooted at a second slash, {@code ""} otherwise
   */
  String getRootPrefix();
}
