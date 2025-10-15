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

import io.minio.BucketExistsArgs;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.vfs.minio.util.MinioPipedOutputStream;

public class MinioFileObject extends AbstractFileObject<MinioFileSystem> {
  public static final String PREFIX = "minio:///";
  public static final String DELIMITER = "/";

  protected MinioFileSystem fileSystem;
  protected String bucketName;
  protected String key;
  protected boolean attached = false;
  protected StatObjectResponse statObjectResponse;

  protected MinioPipedOutputStream outputStream;
  protected GetObjectResponse responseInputStream;

  protected MinioFileObject(final AbstractFileName name, final MinioFileSystem fileSystem) {
    super(name, fileSystem);
    this.fileSystem = fileSystem;
    this.bucketName = getMinioBucketName();
    this.key = getBucketRelativePath();
  }

  public String getMinioBucketName() {
    String bucketName = getName().getPath();
    if (bucketName.indexOf(DELIMITER, 1) > 1) {
      // this file is a file, to get the bucket, remove the name from the path
      bucketName = bucketName.substring(0, bucketName.indexOf(DELIMITER, 1));
    } else {
      // this file is a bucket
      bucketName = bucketName.replaceAll(DELIMITER, "");
    }
    if (bucketName.startsWith("/")) {
      bucketName = bucketName.substring(1);
    }
    return bucketName;
  }

  protected boolean isRootBucket() {
    return StringUtils.isEmpty(bucketName);
  }

  @Override
  protected long doGetContentSize() {
    if (statObjectResponse != null) {
      return statObjectResponse.size();
    } else {
      return -1L;
    }
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    LogChannel.GENERAL.logDebug("Accessing content {0}", getQualifiedName());
    close();
    GetObjectArgs args = GetObjectArgs.builder().object(key).bucket(bucketName).build();
    responseInputStream = fileSystem.getClient().getObject(args);
    return responseInputStream;
  }

  @Override
  protected FileType doGetType() throws Exception {
    return getType();
  }

  @Override
  protected String[] doListChildren() throws Exception {
    List<String> childrenList = new ArrayList<>();

    // Do we need to list the buckets as "folders" of the root?
    if (isRootBucket()) {
      List<Bucket> buckets = fileSystem.getClient().listBuckets();
      for (Bucket bucket : buckets) {
        childrenList.add(bucket.name() + DELIMITER);
      }
    } else {
      String path = key;
      if (!path.endsWith("/")) {
        path += DELIMITER;
      }
      // The regular case
      ListObjectsArgs args = ListObjectsArgs.builder().bucket(bucketName).prefix(path).build();
      Iterable<Result<Item>> results = fileSystem.getClient().listObjects(args);
      for (Result<Item> result : results) {
        Item item = result.get();
        if (item != null) {
          String objectName = item.objectName();
          if (item.isDir() && !objectName.endsWith(DELIMITER)) {
            objectName += DELIMITER;
          }

          if (objectName.length() >= key.length()) {
            objectName = objectName.substring(key.length());
            if (!objectName.isEmpty() && !DELIMITER.equals(objectName)) {
              childrenList.add(objectName);
            }
          }
        }
      }
    }
    return childrenList.toArray(new String[0]);
  }

  protected String getBucketRelativePath() {
    if (getName().getPath().indexOf(DELIMITER, 1) >= 0) {
      return getName().getPath().substring(getName().getPath().indexOf(DELIMITER, 1) + 1);
    } else {
      return "";
    }
  }

  @Override
  public void doAttach() throws FileSystemException {
    // This allows us to sprinkle doAttach() where needed without incurring a performance hit.
    //
    if (attached) {
      return;
    }
    attached = true;

    LogChannel.GENERAL.logDebug("Attach called on {0}", getQualifiedName());
    injectType(FileType.IMAGINARY);

    // The root bucket is minio:/// and is a folder
    // If the key is empty and the bucket is known, it's also a folder
    //
    if (StringUtils.isEmpty(key)) {
      // cannot attach to root bucket
      injectType(FileType.FOLDER);
      return;
    }

    try {
      // We'll first try the file scenario:
      //
      StatObjectArgs statArgs = StatObjectArgs.builder().bucket(bucketName).object(key).build();
      statObjectResponse = fileSystem.getClient().statObject(statArgs);

      // In MinIO keys with a trailing slash (delimiter), are considered folders.
      //
      if (key.endsWith(DELIMITER)) {
        injectType(FileType.FOLDER);
      } else {
        injectType(getName().getType());
      }
    } catch (Exception e) {
      // File does not exist.
      // Perhaps it's a folder and we can find it by looking for the key with an extra slash?
      //
      if (key.endsWith(DELIMITER)) {
        statObjectResponse = null;
      } else {
        try {
          StatObjectArgs statArgs =
              StatObjectArgs.builder().bucket(bucketName).object(key + DELIMITER).build();
          statObjectResponse = fileSystem.getClient().statObject(statArgs);
          injectType(FileType.FOLDER);
        } catch (Exception ex) {
          // Still doesn't exist?
          statObjectResponse = null;
        }
      }
    } finally {
      close();
    }
  }

  public void closeMinio() {
    try {
      if (outputStream != null) {
        outputStream.close();
      }
      if (responseInputStream != null) {
        responseInputStream.close();
      }
    } catch (Exception e) {
      // Ignore for now. Just making sure these things are closed properly.
    } finally {
      injectType(FileType.IMAGINARY);
      outputStream = null;
      responseInputStream = null;
      statObjectResponse = null;
      attached = false;
    }
  }

  @Override
  public void doDetach() throws Exception {
    LogChannel.GENERAL.logDebug("detaching {0}", getQualifiedName());
    closeMinio();
  }

  @Override
  protected void doDelete() throws FileSystemException {
    doDelete(this.key, this.bucketName);
  }

  protected void doDelete(String key, String bucketName) throws FileSystemException {
    try {
      MinioClient client = fileSystem.getClient();

      // We can only delete a folder if it's empty.
      // So we delete any objects in the folder first.
      //
      if (getType() == FileType.FOLDER) {
        // list all children inside the folder
        //
        ListObjectsArgs listArgs = ListObjectsArgs.builder().bucket(bucketName).build();
        Iterable<Result<Item>> items = client.listObjects(listArgs);
        Iterator<Result<Item>> iterator = items.iterator();
        List<DeleteObject> deleteObjects = new ArrayList<>();
        while (iterator.hasNext()) {
          Result<Item> result = iterator.next();
          deleteObjects.add(new DeleteObject(result.get().objectName()));
        }

        RemoveObjectsArgs removeArgs =
            RemoveObjectsArgs.builder().bucket(bucketName).objects(deleteObjects).build();
        client.removeObjects(removeArgs);
      }

      RemoveObjectArgs removeObjectArgs =
          RemoveObjectArgs.builder().bucket(bucketName).object(key).build();
      fileSystem.getClient().removeObject(removeObjectArgs);
      doDetach();
    } catch (Exception e) {
      throw new FileSystemException("Error deleting object " + key + " in bucket " + bucketName, e);
    }
  }

  @Override
  public void createFolder() throws FileSystemException {
    MinioClient client = fileSystem.getClient();
    try {
      if (key.isEmpty()) {
        if (StringUtils.isEmpty(bucketName)) {
          // Nothing to do here, move along!
          return;
        }

        // Does the bucket exist?  If not, create it.
        //
        BucketExistsArgs existsArgs = BucketExistsArgs.builder().bucket(bucketName).build();
        boolean exists = client.bucketExists(existsArgs);

        if (!exists) {
          // We want to create a bucket.
          //
          MakeBucketArgs args = MakeBucketArgs.builder().bucket(bucketName).build();
          client.makeBucket(args);
        }
        return;
      }

      // We know it's a folder and MinIO needs to have a / at the end to create and find folders
      //
      if (!key.endsWith(DELIMITER)) {
        key += DELIMITER;
      }
      PutObjectArgs args =
          PutObjectArgs.builder()
              .bucket(bucketName)
              .object(key)
              .contentType("application/x-directory")
              .stream(new ByteArrayInputStream(new byte[] {}), 0, -1)
              .build();
      client.putObject(args);
    } catch (Exception e) {
      throw new FileSystemException("Error creating folder", e);
    } finally {
      closeMinio();
      doAttach();
    }
  }

  @Override
  public void createFile() throws FileSystemException {
    try {
      OutputStream os = doGetOutputStream(false);

      // Open and close it to create an empty file.
      os.close();
      closeMinio();
      // Update the metadata
      doAttach();
    } catch (Exception e) {
      throw new FileSystemException("error creating file", e);
    }
  }

  @Override
  public OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    if (outputStream != null) {
      // createFile() already built the output stream.
      return outputStream;
    }

    outputStream = new MinioPipedOutputStream(fileSystem, bucketName, key);
    return outputStream;
  }

  @Override
  public long doGetLastModifiedTime() {
    if (statObjectResponse == null) {
      return 0;
    }
    return statObjectResponse.lastModified().toInstant().toEpochMilli();
  }

  @Override
  protected void doCreateFolder() throws Exception {
    if (!isRootBucket()) {
      try {
        PutObjectArgs args =
            PutObjectArgs.builder()
                .bucket(bucketName)
                .object(key)
                .contentType("binary/octet-stream")
                .build();
        fileSystem.getClient().putObject(args);
      } catch (Exception e) {
        throw new FileSystemException("vfs.provider.local/create-folder.error", this, e);
      }
    } else {
      throw new FileSystemException("vfs.provider/create-folder-not-supported.error");
    }
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    // Renames are not supported on the S3 protocol.
    // For a file we can copy to a new name and delete the old one.
    // The folders, we throw an error.
    //
    if (getType().equals(FileType.FOLDER)) {
      throw new FileSystemException("vfs.provider/rename-not-supported.error");
    }

    MinioFileObject dest = (MinioFileObject) newFile;
    CopySource source = CopySource.builder().bucket(bucketName).object(key).build();

    if (!exists()) {
      throw new FileSystemException("vfs.provider/rename.error", this, newFile);
    }

    CopyObjectArgs args =
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(dest.key)
            .bucket(dest.bucketName)
            .source(source)
            .build();
    fileSystem.getClient().copyObject(args);

    // Delete self
    delete();

    // Invalidate metadata: the old file no longer exists
    //
    closeMinio();
  }

  protected String getQualifiedName() {
    return getQualifiedName(this);
  }

  protected String getQualifiedName(MinioFileObject fileObject) {
    return fileObject.bucketName + "/" + fileObject.key;
  }
}
