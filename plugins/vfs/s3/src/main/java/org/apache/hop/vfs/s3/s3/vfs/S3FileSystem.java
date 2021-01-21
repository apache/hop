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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StorageUnitConverter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.s3.s3common.S3CommonFileSystem;
import org.apache.hop.vfs.s3.s3common.S3HopProperty;

public class S3FileSystem extends S3CommonFileSystem {

  private static final Class<?> PKG = S3FileSystem.class; // For Translator
  
  private static final ILogChannel consoleLog =
      new LogChannel(BaseMessages.getString(PKG, "TITLE.S3File"));

  public StorageUnitConverter storageUnitConverter;
  public S3HopProperty s3HopProperty;

  /**
   * Minimum part size specified in documentation see
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  private static final String MIN_PART_SIZE = "5MB";

  /**
   * Maximum part size specified in documentation see
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  private static final String MAX_PART_SIZE = "5GB";

  public S3FileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions) {
    this(rootName, fileSystemOptions, new StorageUnitConverter(), new S3HopProperty());
  }

  public S3FileSystem(
      final FileName rootName,
      final FileSystemOptions fileSystemOptions,
      final StorageUnitConverter storageUnitConverter,
      final S3HopProperty s3HopProperty) {
    super(rootName, fileSystemOptions);
    this.storageUnitConverter = storageUnitConverter;
    this.s3HopProperty = s3HopProperty;
  }

  public FileObject createFile(AbstractFileName name) throws Exception {
    return new S3FileObject(name, this);
  }

  public int getPartSize() {
    long parsedPartSize = parsePartSize(s3HopProperty.getPartSize());
    return convertToInt(parsedPartSize);
  }

  public long parsePartSize(String partSizeString) {
    long parsePartSize = convertToLong(partSizeString);
    if (parsePartSize < convertToLong(MIN_PART_SIZE)) {
      consoleLog.logBasic(
          BaseMessages.getString(
              PKG, "WARN.S3MultiPart.DefaultPartSize", partSizeString, MIN_PART_SIZE));
      parsePartSize = convertToLong(MIN_PART_SIZE);
    }

    // still allow > 5GB, api might be updated in the future
    if (parsePartSize > convertToLong(MAX_PART_SIZE)) {
      consoleLog.logBasic(
          BaseMessages.getString(
              PKG, "WARN.S3MultiPart.MaximumPartSize", partSizeString, MAX_PART_SIZE));
    }
    return parsePartSize;
  }

  public int convertToInt(long parsedPartSize) {
    return (int) Long.min(Integer.MAX_VALUE, parsedPartSize);
  }

  public long convertToLong(String partSize) {
    return storageUnitConverter.displaySizeToByteCount(partSize);
  }
}
