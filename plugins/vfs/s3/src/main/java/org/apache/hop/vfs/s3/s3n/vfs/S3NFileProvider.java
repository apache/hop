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

package org.apache.hop.vfs.s3.s3n.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.vfs.s3.s3common.S3CommonFileProvider;

public class S3NFileProvider extends S3CommonFileProvider {

  /** The scheme this provider was designed to support */
  public static final String SCHEME = "s3n";

  /** User Information. */
  public static final String ATTR_USER_INFO = "UI";

  public S3NFileProvider() {
    super();
    setFileNameParser(S3NFileNameParser.getInstance());
  }

  public FileSystem doCreateFileSystem(
      final FileName name, final FileSystemOptions fileSystemOptions) throws FileSystemException {
    return new S3NFileSystem(name, fileSystemOptions);
  }
}
