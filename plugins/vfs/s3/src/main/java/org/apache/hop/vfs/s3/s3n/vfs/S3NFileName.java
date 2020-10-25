/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.vfs.s3.s3n.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

/**
 * Custom filename that represents an S3 file with the bucket and its relative path
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3NFileName extends AbstractFileName {
  public static final String DELIMITER = "/";

  private String bucketId;
  private String bucketRelativePath;

  public S3NFileName( String scheme, String bucketId, String path, FileType type ) {
    super( scheme, path, type );

    this.bucketId = bucketId;

    if ( path.length() > 1 ) {
      this.bucketRelativePath = path.substring( 1 );
      if ( type.equals( FileType.FOLDER ) ) {
        this.bucketRelativePath += DELIMITER;
      }
    } else {
      this.bucketRelativePath = "";
    }
  }

  @Override public String getURI() {
    final StringBuilder buffer = new StringBuilder();
    appendRootUri( buffer, false );
    buffer.append( getPath() );
    return buffer.toString();
  }

  public String getBucketId() {
    return bucketId;
  }

  public String getBucketRelativePath() {
    return bucketRelativePath;
  }

  public FileName createName( String absPath, FileType type ) {
    return new S3NFileName( getScheme(), bucketId, absPath, type );
  }

  protected void appendRootUri( StringBuilder buffer, boolean addPassword ) {
    buffer.append( getScheme() );
    buffer.append( "://" );
    buffer.append( bucketId );
  }
}
