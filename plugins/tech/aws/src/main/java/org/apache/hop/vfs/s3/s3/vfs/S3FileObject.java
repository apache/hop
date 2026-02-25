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

package org.apache.hop.vfs.s3.s3.vfs;

import java.io.OutputStream;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.hop.vfs.s3.s3common.S3CommonFileObject;
import org.apache.hop.vfs.s3.s3common.S3CommonPipedOutputStream;

public class S3FileObject extends S3CommonFileObject {

  public S3FileObject(final AbstractFileName name, final S3FileSystem fileSystem) {
    super(name, fileSystem);
  }

  @Override
  public void doDelete() throws FileSystemException {
    super.doDelete();
  }

  @Override
  public OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    return new S3CommonPipedOutputStream(
        this.fileSystem, bucketName, key, ((S3FileSystem) this.fileSystem).getPartSize());
  }
}
