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
package org.apache.hop.vfs.s3.s3.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

/**
 * Custom filename that represents an S3 file with the bucket and its relative path
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3FileName extends AbstractFileName {
  public static final String DELIMITER = "/";

  private String bucketId;
  private String bucketRelativePath;

  public S3FileName(String scheme, String bucketId, String path, FileType type) {
    super(scheme, path, type);

    this.bucketId = bucketId;

    if (path.length() > 1) {
      this.bucketRelativePath = path.substring(1);
      if (type.equals(FileType.FOLDER)) {
        this.bucketRelativePath += DELIMITER;
      }
    } else {
      this.bucketRelativePath = "";
    }
  }

  @Override
  public String getURI() {
    final StringBuilder buffer = new StringBuilder();
    appendRootUri(buffer, false);
    buffer.append(getPath());
    return buffer.toString();
  }

  public String getBucketId() {
    return bucketId;
  }

  public String getBucketRelativePath() {
    return bucketRelativePath;
  }

  public FileName createName(String absPath, FileType type) {
    return new S3FileName(getScheme(), bucketId, absPath, type);
  }

  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    buffer.append(getScheme());
    // [PDI-18634] Only 1 slash is needed here, because this class is not expecting an authority,
    // instead it is
    // expecting that the connection has already been established to the Amazon AWS S3 file system,
    // the second slash
    // comes from the absolute path of the file stored in the file system.  So the root path with
    // this Uri ends up
    // being: s3:// instead of s3:///.  A file located in a top level bucket would be
    // s3://bucket/example.txt instead of
    // s3:///bucket/example.txt.  In our VFS, this is handled the same, in CLS 3 slashes do not
    // resolve appropriately.
    // For consistency, the code here changes so that we will end up with 2 slashes.
    buffer.append("://");
  }
}
