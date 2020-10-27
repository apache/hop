/*!
 * Hop : The Hop Orchestration Platform
 *
 * Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
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

package org.apache.hop.vfs.s3.s3n.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.hop.vfs.s3.s3common.S3CommonFileSystem;

public class S3NFileSystem extends S3CommonFileSystem {

  public S3NFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    super( rootName, fileSystemOptions );
  }

  public FileObject createFile( AbstractFileName name ) throws Exception {
    return new S3NFileObject( name, this );
  }

}
