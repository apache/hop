/*!
 * Hop : The Hop Orchestration Platform
 *
 * Copyright 2019 Hitachi Vantara.  All rights reserved.
 * http://www.project-hop.org
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
 *
 */

package org.apache.hop.vfs.s3.s3a.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.vfs.s3.s3common.S3CommonFileProvider;

public class S3AFileProvider extends S3CommonFileProvider {

  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "s3a";

  /**
   * User Information.
   */
  public static final String ATTR_USER_INFO = "UI";

  public S3AFileProvider() {
    super();
    setFileNameParser( S3AFileNameParser.getInstance() );
  }

  protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    return new S3AFileSystem( name, fileSystemOptions );
  }

}
