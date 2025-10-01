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
 *
 */

package org.apache.hop.vfs.minio;

import io.minio.MinioClient;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StorageUnitConverter;
import org.apache.hop.i18n.BaseMessages;

@Getter
@Setter
public class MinioFileSystem extends AbstractFileSystem {
  private static final Class<?> PKG = MinioFileSystem.class;
  private static final ILogChannel consoleLog =
      new LogChannel(BaseMessages.getString(PKG, "TITLE.Minio"));

  private String endPointHostname;
  private int endPointPort;
  private boolean endPointSecure;
  private String accessKey;
  private String secretKey;
  private String region;
  protected long partSize;

  private MinioClient client;

  protected MinioFileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  @Override
  protected void addCapabilities(Collection<Capability> caps) {
    caps.addAll(CAPABILITIES);
  }

  public static final List<Capability> CAPABILITIES =
      Arrays.asList(
          Capability.CREATE,
          Capability.DELETE,
          Capability.RENAME,
          Capability.GET_TYPE,
          Capability.LIST_CHILDREN,
          Capability.READ_CONTENT,
          Capability.URI,
          Capability.WRITE_CONTENT,
          Capability.GET_LAST_MODIFIED,
          Capability.RANDOM_ACCESS_READ);

  public StorageUnitConverter storageUnitConverter;

  /**
   * Minimum part size specified in <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html">documentation</a>.
   */
  private static final String MIN_PART_SIZE = "5MB";

  /**
   * Maximum part size specified in <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html">documentation</a>.
   */
  private static final String MAX_PART_SIZE = "5GB";

  public MinioClient getClient() {
    if (client == null) {
      client =
          MinioClient.builder()
              .credentials(accessKey, secretKey)
              .endpoint(endPointHostname, endPointPort, endPointSecure)
              .region(region)
              .build();
    }
    return client;
  }

  @Override
  public FileObject createFile(AbstractFileName name) throws Exception {
    return new MinioFileObject(name, this);
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

  public long convertToLong(String partSize) {
    return storageUnitConverter.displaySizeToByteCount(partSize);
  }
}
