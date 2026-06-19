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
package org.apache.hop.vfs.webdav;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileObject;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystem;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.jackrabbit.webdav.DavConstants;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.client.methods.HttpPropfind;
import org.apache.jackrabbit.webdav.property.DavProperty;
import org.apache.jackrabbit.webdav.property.DavPropertyName;
import org.apache.jackrabbit.webdav.property.DavPropertyNameSet;
import org.apache.jackrabbit.webdav.property.DavPropertySet;
import org.w3c.dom.Node;

/**
 * Wire WebDAV4 file object with a {@code DAV:resourcetype} check that supports extended
 * collections.
 *
 * <p>Stock {@link Webdav4FileObject} casts the {@code resourcetype} property value to a single
 * {@link Node}. CardDAV address books and CalDAV calendars (e.g. Nextcloud {@code
 * /remote.php/dav/addressbooks/…}) report multiple resource types ({@code
 * <collection/><card:addressbook/>}), for which Jackrabbit yields a {@code List<Node>} instead. The
 * stock cast then fails with {@link ClassCastException} and VFS reports "Could not determine the
 * type of file". This subclass inspects all resource type nodes, so such collections behave as
 * regular folders.
 */
final class HopWireWebDavFileObject extends Webdav4FileObject {

  HopWireWebDavFileObject(AbstractFileName name, Webdav4FileSystem fileSystem)
      throws FileSystemException {
    super(name, fileSystem);
  }

  @Override
  protected FileType doGetType() throws Exception {
    try {
      return isDavCollection() ? FileType.FOLDER : FileType.FILE;
    } catch (FileNotFoundException | FileNotFolderException e) {
      return FileType.IMAGINARY;
    }
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    try {
      if (!isDavCollection()) {
        throw new FileNotFolderException(getName());
      }
      DavPropertyNameSet nameSet = new DavPropertyNameSet();
      nameSet.add(DavPropertyName.create(DavConstants.PROPERTY_DISPLAYNAME));
      HttpPropfind request =
          new HttpPropfind(getInternalURI().toString(), nameSet, DavConstants.DEPTH_1);
      try (CloseableHttpResponse res = executePropfind(request)) {
        List<FileObject> children = new ArrayList<>();
        MultiStatusResponse[] responses = request.getResponseBodyAsMultiStatus(res).getResponses();
        for (MultiStatusResponse response : responses) {
          if (isSelf(response.getHref())) {
            continue;
          }
          String resourceName = lastPathSegment(response.getHref());
          if (!resourceName.isEmpty()) {
            children.add(
                getFileSystem()
                    .resolveFile(
                        getFileSystem()
                            .getFileSystemManager()
                            .resolveName(getName(), resourceName, NameScope.CHILD)));
          }
        }
        return children.toArray(new FileObject[0]);
      }
    } catch (FileNotFoundException e) {
      throw new FileNotFolderException(getName(), e);
    } catch (FileNotFolderException e) {
      throw e;
    } catch (DavException | IOException e) {
      throw new FileSystemException(e.getMessage(), e);
    }
  }

  /** PROPFIND Depth:0 for {@code resourcetype}; true when any reported type is a collection. */
  private boolean isDavCollection() throws IOException, DavException {
    DavPropertyNameSet nameSet = new DavPropertyNameSet();
    nameSet.add(DavPropertyName.create(DavConstants.PROPERTY_RESOURCETYPE));
    HttpPropfind request =
        new HttpPropfind(getInternalURI().toString(), nameSet, DavConstants.DEPTH_0);
    try (CloseableHttpResponse res = executePropfind(request)) {
      MultiStatusResponse[] responses = request.getResponseBodyAsMultiStatus(res).getResponses();
      if (responses.length == 0) {
        return false;
      }
      DavPropertySet props = responses[0].getProperties(HttpStatus.SC_OK);
      DavProperty<?> resourceType =
          props.get(DavConstants.PROPERTY_RESOURCETYPE, DavConstants.NAMESPACE);
      return resourceTypeIndicatesCollection(resourceType == null ? null : resourceType.getValue());
    }
  }

  /**
   * Jackrabbit materializes {@code resourcetype} as {@code null} (no content), a single {@link
   * Node}, or a {@code List<Node>} when the collection advertises several types.
   */
  static boolean resourceTypeIndicatesCollection(Object propertyValue) {
    if (propertyValue instanceof Node node) {
      return isCollectionElement(node);
    }
    if (propertyValue instanceof Iterable<?> nodes) {
      for (Object item : nodes) {
        if (item instanceof Node node && isCollectionElement(node)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isCollectionElement(Node node) {
    return DavConstants.XML_COLLECTION.equals(node.getLocalName());
  }

  private CloseableHttpResponse executePropfind(HttpPropfind request)
      throws IOException, DavException {
    request.addHeader("Cache-control", "no-cache");
    request.addHeader("Pragma", "no-cache");
    CloseableHttpResponse res = (CloseableHttpResponse) executeHttpUriRequest(request);
    boolean usable = false;
    try {
      int status = res.getStatusLine().getStatusCode();
      if (status == HttpURLConnection.HTTP_NOT_FOUND || status == HttpURLConnection.HTTP_GONE) {
        EntityUtils.consume(res.getEntity());
        throw new FileNotFoundException(getName());
      }
      request.checkSuccess(res);
      usable = true;
      return res;
    } finally {
      if (!usable) {
        res.close();
      }
    }
  }

  /** Multistatus hrefs may be absolute URLs or absolute paths, with or without trailing slash. */
  private boolean isSelf(String href) {
    return stripTrailingSlash(decodedPath(href))
        .equals(stripTrailingSlash(getInternalURI().getPath()));
  }

  private static String decodedPath(String href) {
    try {
      String path = URI.create(href).getPath();
      return path != null ? path : href;
    } catch (IllegalArgumentException e) {
      return href;
    }
  }

  private static String stripTrailingSlash(String path) {
    String p = path;
    while (p.length() > 1 && p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  private static String lastPathSegment(String href) {
    String path = href;
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    int i = path.lastIndexOf('/');
    return i >= 0 ? path.substring(i + 1) : path;
  }
}
