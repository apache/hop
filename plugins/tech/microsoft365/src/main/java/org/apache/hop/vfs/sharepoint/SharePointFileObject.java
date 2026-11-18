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

package org.apache.hop.vfs.sharepoint;

import com.microsoft.graph.models.DriveItem;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.util.FileObjectUtils;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * A managed path, a site, a document library, or a file or folder of a document library, depending
 * on the depth of its name.
 */
@NullMarked
public class SharePointFileObject extends AbstractFileObject<SharePointFileSystem> {

  /** The id of the site, only for a site. */
  private @Nullable String siteId;

  /** The id of the document library, for a library and everything below. */
  private @Nullable String driveId;

  /** The metadata of the file or folder, only below a document library. */
  private @Nullable DriveItem item;

  protected SharePointFileObject(AbstractFileName name, SharePointFileSystem fileSystem) {
    super(name, fileSystem);
  }

  private SharePointFileName name() {
    return (SharePointFileName) getName();
  }

  @Override
  protected void doAttach() throws Exception {
    // Already filled in by the listing of the parent folder
    if (item != null) {
      return;
    }
    SharePointFileName name = name();
    SharePointFileSystem fileSystem = getAbstractFileSystem();
    int depth = name.getLevel();
    if (depth == SharePointFileName.SITE_DEPTH) {
      siteId = fileSystem.findSiteId(name.getManagedPath(), name.getSiteName());
    } else if (depth >= SharePointFileName.DRIVE_DEPTH) {
      driveId = fileSystem.findDriveId(name);
      if (driveId != null && name.isItem()) {
        item = fileSystem.findItem(driveId, name.getEncodedItemPath());
      }
    }
  }

  @Override
  protected void doDetach() {
    siteId = null;
    driveId = null;
    item = null;
  }

  @Override
  protected FileType doGetType() {
    int depth = name().getLevel();
    if (depth < SharePointFileName.MANAGED_PATH_DEPTH) {
      return FileType.FOLDER;
    }
    if (depth == SharePointFileName.MANAGED_PATH_DEPTH) {
      // The folders of the root are fixed, whether their sites can be listed or not
      return SharePointFileSystem.isManagedPath(name().getManagedPath())
          ? FileType.FOLDER
          : FileType.IMAGINARY;
    }
    if (depth == SharePointFileName.SITE_DEPTH) {
      return siteId == null ? FileType.IMAGINARY : FileType.FOLDER;
    }
    if (depth == SharePointFileName.DRIVE_DEPTH) {
      return driveId == null ? FileType.IMAGINARY : FileType.FOLDER;
    }
    return typeOf(item);
  }

  private static FileType typeOf(@Nullable DriveItem item) {
    if (item == null) {
      return FileType.IMAGINARY;
    }
    // A package, such as a OneNote notebook, is browsed like a folder
    return item.getFolder() != null || item.getPackage() != null ? FileType.FOLDER : FileType.FILE;
  }

  @Override
  protected long doGetContentSize() {
    return item == null || item.getSize() == null ? 0 : item.getSize();
  }

  @Override
  protected long doGetLastModifiedTime() {
    return item == null || item.getLastModifiedDateTime() == null
        ? 0
        : item.getLastModifiedDateTime().toInstant().toEpochMilli();
  }

  @Override
  protected String @Nullable [] doListChildren() throws Exception {
    // A file, or a site, library or folder that does not exist
    if (doGetType() != FileType.FOLDER) {
      return null;
    }
    SharePointFileSystem fileSystem = getAbstractFileSystem();
    int depth = name().getLevel();
    List<String> names = new ArrayList<>();
    if (depth < SharePointFileName.MANAGED_PATH_DEPTH) {
      names.addAll(SharePointFileSystem.MANAGED_PATHS);
    } else if (depth == SharePointFileName.MANAGED_PATH_DEPTH) {
      names.addAll(fileSystem.listSites(name().getManagedPath()).keySet());
    } else if (depth == SharePointFileName.SITE_DEPTH) {
      names.addAll(fileSystem.listDrives(siteId).keySet());
    } else {
      for (DriveItem child : fileSystem.listChildren(driveId, folderItemId())) {
        names.add(child.getName());
      }
    }
    // The names go through the file name parser, which decodes them
    return names.stream().map(UriParser::encode).toArray(String[]::new);
  }

  @Override
  protected FileObject @Nullable [] doListChildrenResolved() throws Exception {
    // The managed paths, sites and libraries are resolved by name, anything that is not a folder is
    // rejected by the listing of the names
    if (name().getLevel() < SharePointFileName.DRIVE_DEPTH || doGetType() != FileType.FOLDER) {
      return null;
    }
    SharePointFileSystem fileSystem = getAbstractFileSystem();
    List<DriveItem> children = fileSystem.listChildren(driveId, folderItemId());
    List<FileObject> files = new ArrayList<>(children.size());
    for (DriveItem child : children) {
      FileObject file =
          fileSystem.resolveFile(
              fileSystem
                  .getFileSystemManager()
                  .resolveName(getName(), UriParser.encode(child.getName()), NameScope.CHILD));
      // Hand over the metadata so the child does not have to fetch it again
      if (FileObjectUtils.getAbstractFileObject(file)
          instanceof SharePointFileObject sharePointFile) {
        sharePointFile.inject(driveId, child);
      }
      files.add(file);
    }
    return files.toArray(new FileObject[0]);
  }

  private void inject(String driveId, DriveItem item) throws FileSystemException {
    this.driveId = driveId;
    this.item = item;
    injectType(typeOf(item));
  }

  /**
   * @return the id of this folder to list its children, a document library is its own root folder
   */
  private String folderItemId() {
    return item == null ? SharePointFileSystem.ROOT_ITEM_ID : item.getId();
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    if (item == null) {
      throw new FileNotFoundException(getName());
    }
    InputStream stream = getAbstractFileSystem().download(driveId, item.getId());
    if (stream == null) {
      throw new FileNotFoundException(getName());
    }
    return stream;
  }

  @Override
  protected OutputStream doGetOutputStream(boolean append) throws Exception {
    if (append) {
      throw new FileSystemException("vfs.provider/write-append-not-supported.error", getName());
    }
    final String targetDriveId = requireDriveId();
    final String encodedPath = name().getEncodedItemPath();

    // Microsoft Graph needs the size before the first byte is sent, so the content is buffered
    final Path buffer = Files.createTempFile("hop-sharepoint-", ".tmp");
    return new FilterOutputStream(Files.newOutputStream(buffer)) {
      private boolean closed;

      @Override
      public void write(byte[] bytes, int offset, int length) throws IOException {
        out.write(bytes, offset, length);
      }

      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        closed = true;
        try {
          super.close();
          driveId = targetDriveId;
          item = getAbstractFileSystem().upload(targetDriveId, encodedPath, buffer);
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new IOException("Failed to upload " + getName(), e);
        } finally {
          Files.deleteIfExists(buffer);
        }
      }
    };
  }

  @Override
  protected void doCreateFolder() throws Exception {
    String targetDriveId = requireDriveId();
    SharePointFileName parent = (SharePointFileName) getName().getParent();
    driveId = targetDriveId;
    item =
        getAbstractFileSystem()
            .createFolder(
                targetDriveId,
                requireItemId(targetDriveId, parent),
                name().getDecodedBaseName(),
                name().getEncodedItemPath());
  }

  @Override
  protected void doDelete() throws Exception {
    String targetDriveId = requireDriveId();
    if (item == null) {
      throw new FileNotFoundException(getName());
    }
    getAbstractFileSystem().delete(targetDriveId, item.getId());
    item = null;
  }

  /** An item can only be moved inside its document library, anything else is a copy and delete. */
  @Override
  public boolean canRenameTo(FileObject newFile) {
    if (!super.canRenameTo(newFile) || !(newFile.getName() instanceof SharePointFileName target)) {
      return false;
    }
    SharePointFileName source = name();
    return source.isItem()
        && target.isItem()
        && source.getManagedPath().equalsIgnoreCase(target.getManagedPath())
        && source.getSiteName().equalsIgnoreCase(target.getSiteName())
        && source.getDriveName().equalsIgnoreCase(target.getDriveName());
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    if (item == null) {
      throw new FileNotFoundException(getName());
    }
    String targetDriveId = requireDriveId();
    SharePointFileName target = (SharePointFileName) newFile.getName();
    getAbstractFileSystem()
        .move(
            targetDriveId,
            item.getId(),
            requireItemId(targetDriveId, (SharePointFileName) target.getParent()),
            target.getDecodedBaseName());
    item = null;
  }

  /**
   * @return the id of the document library of this file or folder
   * @throws FileSystemException for the root, a site or a library, which can only be read, or when
   *     the site or the library does not exist
   */
  private String requireDriveId() throws FileSystemException {
    if (!name().isItem()) {
      throw new FileSystemException(
          "Sites and document libraries are read-only, \"" + getName() + "\" can not be modified");
    }
    String id = driveId != null ? driveId : getAbstractFileSystem().findDriveId(name());
    if (id == null) {
      throw new FileSystemException(
          "The site or document library of \"" + getName() + "\" does not exist");
    }
    return id;
  }

  private String requireItemId(String driveId, SharePointFileName folder)
      throws FileSystemException {
    DriveItem folderItem = getAbstractFileSystem().findItem(driveId, folder.getEncodedItemPath());
    if (folderItem == null) {
      throw new FileNotFoundException(folder);
    }
    return folderItem.getId();
  }
}
