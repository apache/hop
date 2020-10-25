/*!
* Hop : The Hop Orchestration Platform
 *
 * Copyright 2010 - 2020 Hitachi Vantara.  All rights reserved.
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

package org.apache.hop.vfs.s3.s3.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;

/**
 * Custom parser for the s3 URL
 *
 * @author asimoes
 * @since 09-11-2017
 */

public class S3FileNameParser extends AbstractFileNameParser {
  private static final S3FileNameParser INSTANCE = new S3FileNameParser();

  public S3FileNameParser() {
    super();
  }

  public static FileNameParser getInstance() {
    return INSTANCE;
  }

  public FileName parseUri( VfsComponentContext context, FileName base, String uri ) throws FileSystemException {
    StringBuilder name = new StringBuilder();

    String scheme = UriParser.extractScheme( uri, name );
    UriParser.canonicalizePath( name, 0, name.length(), this );

    // Normalize separators in the path
    UriParser.fixSeparators( name );

    // Normalise the path
    FileType fileType = UriParser.normalisePath( name );

    String fullPath = name.toString();
    // Extract bucket name
    final String bucketName = UriParser.extractFirstElement( name );

    return new S3FileName( scheme, bucketName, fullPath, fileType );
  }
}
