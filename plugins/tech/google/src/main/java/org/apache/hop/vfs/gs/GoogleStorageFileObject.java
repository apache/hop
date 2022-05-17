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

package org.apache.hop.vfs.gs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GoogleStorageFileObject extends AbstractFileObject<GoogleStorageFileSystem> {

  Blob blob = null;
  Bucket bucket = null;

  String bucketName;
  String bucketPath;

  protected GoogleStorageFileObject(AbstractFileName name, GoogleStorageFileSystem fs) {
    super(name, fs);
    bucketName = getAbstractFileSystem().getBucketName(getName()).trim();
    bucketPath = getAbstractFileSystem().getBucketPath(getName()).trim();
  }

  protected GoogleStorageFileObject(
      AbstractFileName name, GoogleStorageFileSystem fs, Bucket bucket, Blob blob) {
    super(name, fs);
    bucketName = getAbstractFileSystem().getBucketName(getName()).trim();
    bucketPath = getAbstractFileSystem().getBucketPath(getName()).trim();
    this.bucket = bucket;
    this.blob = blob;
  }

  private boolean hasBucket() {
    return bucket != null;
  }

  private boolean hasObject() {
    return blob != null;
  }

  @Override
  protected void doAttach() throws Exception {
    Storage storage = getAbstractFileSystem().setupStorage();

    if (bucketName.length() > 0) {
      if (this.bucket == null) {
        this.bucket = storage.get(bucketName);
      }
      if (bucketPath.length() > 0) {
        if (this.blob == null) {
          this.blob = storage.get(bucketName, bucketPath);
          if (this.blob == null) {
            String parent = getParentFolder(bucketPath);
            String child = lastPathElement(stripTrailingSlash(bucketPath));
            Page<Blob> page;
            if (parent.length() > 0) {
              page =
                  storage.list(
                      bucketName,
                      BlobListOption.currentDirectory(),
                      BlobListOption.prefix(parent + "/"));

            } else {
              page = storage.list(bucketName, BlobListOption.currentDirectory());
            }
            for (Blob b : page.iterateAll()) {
              if (lastPathElement(stripTrailingSlash(b.getName())).equals(child)) {
                this.blob = b;
                break;
              }
            }
          }
        }
      }
    }
  }

  @Override
  protected long doGetContentSize() throws Exception {
    return hasObject() ? blob.getSize() : 0L;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    if (!isFile()) {
      throw new FileNotFoundException();
    }
    Storage storage = getAbstractFileSystem().setupStorage();
    return new ReadChannelInputStream(storage.reader(blob.getBlobId()));
  }

  @Override
  protected FileType doGetType() throws Exception {
    if (getName() instanceof GoogleStorageFileName) {
      return getName().getType();
    }
    if (hasBucket()) {
      if (hasObject()) {
        return blob.isDirectory() ? FileType.FOLDER : FileType.FILE;
      }
      if (bucketPath.length() == 0) {
        return FileType.FOLDER;
      }
      return FileType.IMAGINARY;
    }
    return FileType.FOLDER;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    if (!isFolder()) {
      throw new IOException("Object is not a directory");
    }
    Storage storage = getAbstractFileSystem().setupStorage();
    List<String> results = new ArrayList<>();
    if (!hasBucket()) {
      Page<Bucket> page = storage.list();
      for (Bucket b : page.iterateAll()) {
        results.add(b.getName());
      }
    } else {
      Page<Blob> page;
      if (bucketPath.length() > 0) {
        page =
            storage.list(
                bucketName,
                BlobListOption.currentDirectory(),
                BlobListOption.prefix(appendTrailingSlash(bucketPath)));
      } else {
        page = storage.list(bucketName, BlobListOption.currentDirectory());
      }
      for (Blob b : page.iterateAll()) {
        results.add(lastPathElement(stripTrailingSlash(b.getName())));
      }
    }
    return results.toArray(new String[0]);
  }

  @Override
  protected void doCreateFolder() throws Exception {

    Storage storage = getAbstractFileSystem().setupStorage();
    if (!hasBucket()) {
      this.bucket = storage.create(BucketInfo.newBuilder(bucketName).build());
    } else {
      this.blob =
          storage.create(BlobInfo.newBuilder(bucket, appendTrailingSlash(bucketPath)).build());
    }
  }

  @Override
  protected void doDelete() throws Exception {
    if (!hasObject()) {
      throw new IOException("Object is not attached");
    }
    if (!blob.delete()) {
      throw new IOException("Failed to delete object");
    }
  }

  @Override
  protected void doDetach() throws Exception {
    bucket = null;
    blob = null;
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    if (hasObject()) {
      if (blob == null) {
        return 0;
      }
      if (isFolder()) {
        // getting the update time of a folder gives an NPE
        return 0;
      }
      return blob.getUpdateTime();
    }
    if (hasBucket()) {
      if (bucket == null) {
        return 0;
      }
      return bucket.getCreateTime();
    } else {
      return 0L;
    }
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    if (isFolder()) {
      throw new IOException("Object is a directory or bucket");
    }
    if (!hasBucket()) {
      throw new IOException("Object must be placed in bucket");
    }
    if (bucketPath.length() == 0) {
      throw new IOException("Object needs a path within the bucket");
    }
    Storage storage = getAbstractFileSystem().setupStorage();
    if (!hasObject()) {
      this.blob =
          storage.create(BlobInfo.newBuilder(bucket, stripTrailingSlash(bucketPath)).build());
    }
    return new WriteChannelOutputStream(storage.writer(blob));
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    if (!isFolder()) {
      throw new IOException("Object is not a directory");
    }
    Storage storage = getAbstractFileSystem().setupStorage();
    List<FileObject> results = new ArrayList<>();
    if (!hasBucket()) {
      Page<Bucket> page = storage.list();
      for (Bucket b : page.iterateAll()) {
        results.add(
            new GoogleStorageFileObject(
                new GoogleStorageFileName(b.getName(), FileType.FOLDER), getAbstractFileSystem()));
      }
    } else {
      Page<Blob> page;
      if (bucketPath.length() > 0) {
        page =
            storage.list(
                bucketName,
                BlobListOption.currentDirectory(),
                BlobListOption.prefix(appendTrailingSlash(bucketPath)));
      } else {
        page = storage.list(bucketName, BlobListOption.currentDirectory());
      }
      for (Blob b : page.iterateAll()) {
        if (this.blob != null && b.getName().equals(this.blob.getName())) {
          continue;
        }
        results.add(
            new GoogleStorageFileObject(
                new GoogleStorageFileName(
                    getName().getPath() + "/" + lastPathElement(stripTrailingSlash(b.getName())),
                    b.isDirectory() ? FileType.FOLDER : FileType.FILE),
                getAbstractFileSystem(),
                this.bucket != null ? bucket : storage.get(bucketName),
                b));
      }
    }
    return results.toArray(new FileObject[0]);
  }

  String getParentFolder(String name) {
    name = stripTrailingSlash(name);
    int idx = name.lastIndexOf('/');
    if (idx > -1) {
      return name.substring(0, idx);
    }
    return "";
  }

  boolean hasTrailingSlash(String name) {
    return name.endsWith("/");
  }

  String stripTrailingSlash(String name) {
    if (name.endsWith("/")) {
      return name.substring(0, name.length() - 1);
    }
    return name;
  }

  String appendTrailingSlash(String name) {
    if (!name.endsWith("/")) {
      return name + "/";
    }
    return name;
  }

  String lastPathElement(String name) {
    int idx = name.lastIndexOf('/');
    if (idx > -1) {
      return name.substring(idx + 1);
    } else {
      return name;
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GoogleStorageFileObject that = (GoogleStorageFileObject) o;

    return Objects.equals(getName().getPath(), that.getName().getPath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName().getPath());
  }
}
