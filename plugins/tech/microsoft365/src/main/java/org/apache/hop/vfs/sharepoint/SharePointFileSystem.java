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

import com.microsoft.graph.core.models.UploadResult;
import com.microsoft.graph.core.tasks.LargeFileUploadTask;
import com.microsoft.graph.drives.item.items.item.DriveItemItemRequestBuilder;
import com.microsoft.graph.drives.item.items.item.children.ChildrenRequestBuilder;
import com.microsoft.graph.drives.item.items.item.createuploadsession.CreateUploadSessionPostRequestBody;
import com.microsoft.graph.models.BaseCollectionPaginationCountResponse;
import com.microsoft.graph.models.DriveCollectionResponse;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.DriveItemCollectionResponse;
import com.microsoft.graph.models.DriveItemUploadableProperties;
import com.microsoft.graph.models.Folder;
import com.microsoft.graph.models.ItemReference;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.SiteCollectionResponse;
import com.microsoft.graph.models.UploadSession;
import com.microsoft.graph.models.odataerrors.ODataError;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.graph.sites.item.drives.DrivesRequestBuilder;
import com.microsoft.kiota.ApiException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.core.logging.LogChannel;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * One named Microsoft 365 connection. The sites are the folders of the root, the document libraries
 * (drives) of a site are the folders of that site.
 */
@NullMarked
public class SharePointFileSystem extends AbstractFileSystem {

  public static final List<Capability> CAPABILITIES =
      List.of(
          Capability.CREATE,
          Capability.DELETE,
          Capability.RENAME,
          Capability.GET_TYPE,
          Capability.GET_LAST_MODIFIED,
          Capability.LIST_CHILDREN,
          Capability.READ_CONTENT,
          Capability.URI,
          Capability.WRITE_CONTENT);

  /** Above this size the content is sent in slices through an upload session. */
  static final long SIMPLE_UPLOAD_LIMIT = 4L * 1024 * 1024;

  /** The id that addresses the root folder of a document library, and the root site of a tenant. */
  static final String ROOT_ITEM_ID = "root";

  /** A site created by a Microsoft Teams team may live under /teams. */
  private static final List<String> MANAGED_PATHS = List.of("sites", "teams");

  /** The only properties of a file or folder that are used, the others are not requested. */
  private static final String[] ITEM_PROPERTIES = {
    "id", "name", "size", "lastModifiedDateTime", "file", "folder", "package"
  };

  private static final String CONFLICT_BEHAVIOR = "@microsoft.graph.conflictBehavior";
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_CONFLICT = 409;

  private final GraphServiceClient client;

  /** Site name to site id, whatever the case of the name, for the sites found so far. */
  private volatile Map<String, String> siteIds = newNameMap();

  /** The SharePoint host of the tenant, null until it is needed. */
  private volatile @Nullable String hostname;

  /** True once the sites were listed, or Microsoft Graph refused to list them. */
  private volatile boolean sitesListed;

  /** Site id to (drive name to drive id, whatever the case of the name). */
  private final Map<String, Map<String, String>> driveIds = new ConcurrentHashMap<>();

  public SharePointFileSystem(
      FileName rootName,
      GraphServiceClient client,
      @Nullable String hostname,
      @Nullable FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
    this.client = client;
    this.hostname = hostname == null || hostname.isBlank() ? null : hostname.trim();
  }

  @Override
  protected void addCapabilities(Collection<Capability> capabilities) {
    capabilities.addAll(CAPABILITIES);
  }

  @Override
  protected FileObject createFile(AbstractFileName name) {
    return new SharePointFileObject(name, this);
  }

  /**
   * Lists the sites of the tenant and refreshes the cache.
   *
   * @return the site ids by site name
   */
  Map<String, String> listSites() throws FileSystemException {
    Map<String, String> sites = newNameMap();
    try {
      addSites(sites, client.sites().get());
    } catch (ApiException e) {
      throw new FileSystemException(
          "Microsoft Graph refused to list the SharePoint sites: " + describe(e), e);
    }
    if (sites.isEmpty()) {
      // Depending on the tenant the plain listing is empty and only the search returns the sites
      try {
        String url = client.getRequestAdapter().getBaseUrl() + "/sites?search=*";
        addSites(sites, client.sites().withUrl(url).get());
      } catch (ApiException e) {
        LogChannel.GENERAL.logBasic(
            "Microsoft Graph refused to search the SharePoint sites: " + describe(e));
      }
    }
    if (sites.isEmpty()) {
      LogChannel.GENERAL.logBasic(
          "Microsoft Graph returned no SharePoint site. Listing the sites needs the application"
              + " permission Sites.Read.All with admin consent. With Sites.Selected the sites"
              + " can not be listed, use the full path of a site the application was granted.");
    }
    siteIds = sites;
    sitesListed = true;
    driveIds.clear();
    return sites;
  }

  /**
   * @return the HTTP status, the error code and the message of Microsoft Graph
   */
  static String describe(ApiException e) {
    StringBuilder text = new StringBuilder("HTTP ").append(e.getResponseStatusCode());
    if (e instanceof ODataError error && error.getError() != null) {
      text.append(' ').append(error.getError().getCode());
      text.append(": ").append(error.getError().getMessage());
    } else if (e.getMessage() != null) {
      text.append(": ").append(e.getMessage());
    }
    return text.toString();
  }

  private void addSites(Map<String, String> sites, @Nullable SiteCollectionResponse firstPage) {
    forEachPaged(
        firstPage,
        SiteCollectionResponse::getValue,
        nextLink -> client.sites().withUrl(nextLink).get(),
        site -> addSite(sites, site));
  }

  /** Two sites can have the same name, only the first one is kept. */
  private static void addSite(Map<String, String> sites, Site site) {
    String name = site.getName() != null ? site.getName() : site.getDisplayName();
    if (name == null || site.getId() == null) {
      return;
    }
    String namesake = sites.putIfAbsent(name, site.getId());
    if (namesake != null && !namesake.equals(site.getId())) {
      LogChannel.GENERAL.logBasic(
          "The SharePoint site "
              + site.getWebUrl()
              + " is ignored: another site is already named '"
              + name
              + "'");
    }
  }

  /**
   * Reads a site by its address, {@code /sites/name} of the SharePoint host of the tenant. This
   * also works when the application can not list the sites (Sites.Selected).
   *
   * @return the id of the site or null if it does not exist or can not be read
   */
  private @Nullable String findSiteIdByPath(String siteName) {
    String host;
    try {
      host = hostname();
    } catch (ApiException e) {
      LogChannel.GENERAL.logBasic(
          "The SharePoint host of the tenant can not be read, set it on the connection: "
              + describe(e));
      return null;
    }
    if (host == null) {
      return null;
    }
    String encodedSiteName = SharePointFileName.encodeSegment(siteName);
    for (String managedPath : MANAGED_PATHS) {
      // The address of the site relative to the host, such as contoso.sharepoint.com:/sites/Finance
      String address = host + ":/" + managedPath + "/";
      String url = client.getRequestAdapter().getBaseUrl() + "/sites/" + address + encodedSiteName;
      try {
        Site site = client.sites().bySiteId(ROOT_ITEM_ID).withUrl(url).get();
        if (site != null && site.getId() != null) {
          return site.getId();
        }
      } catch (ApiException e) {
        String message =
            "The SharePoint site "
                + host
                + "/"
                + managedPath
                + "/"
                + siteName
                + " can not be read: "
                + describe(e);
        // The site is expected to be missing under one of the managed paths
        if (e.getResponseStatusCode() == HTTP_NOT_FOUND) {
          LogChannel.GENERAL.logDetailed(message);
        } else {
          LogChannel.GENERAL.logBasic(message);
        }
      }
    }
    return null;
  }

  /**
   * Lists the sites, an application that is only granted some sites (Sites.Selected) can not list
   * them and reaches its sites by their address.
   */
  private Map<String, String> listSitesIfAllowed() {
    try {
      return listSites();
    } catch (FileSystemException e) {
      LogChannel.GENERAL.logDetailed(e.getMessage());
      sitesListed = true;
      return siteIds;
    }
  }

  /**
   * @return the SharePoint host of the tenant, such as contoso.sharepoint.com, or null if the root
   *     site can not be read
   */
  private @Nullable String hostname() {
    if (hostname == null) {
      Site root = client.sites().bySiteId(ROOT_ITEM_ID).get();
      if (root != null && root.getSiteCollection() != null) {
        hostname = root.getSiteCollection().getHostname();
      }
      if (hostname == null && root != null && root.getWebUrl() != null) {
        hostname = URI.create(root.getWebUrl()).getHost();
      }
    }
    return hostname;
  }

  /**
   * @return the id of the site or null if there is no site with this name
   */
  @Nullable String findSiteId(String siteName) throws FileSystemException {
    String siteId = siteIds.get(siteName);
    if (siteId != null) {
      return siteId;
    }
    // Reading the site by its address takes one or two requests, whatever the number of sites
    siteId = findSiteIdByPath(siteName);
    if (siteId != null) {
      siteIds.put(siteName, siteId);
      return siteId;
    }
    // A site that has no such address may still be listed, the sites are only listed once for this
    if (!sitesListed) {
      siteId = listSitesIfAllowed().get(siteName);
    }
    return siteId;
  }

  /**
   * Lists the document libraries of a site and refreshes the cache.
   *
   * @return the drive ids by drive name
   */
  Map<String, String> listDrives(String siteId) {
    Map<String, String> drives = newNameMap();
    DrivesRequestBuilder request = client.sites().bySiteId(siteId).drives();
    forEachPaged(
        request.get(),
        DriveCollectionResponse::getValue,
        nextLink -> request.withUrl(nextLink).get(),
        drive -> {
          if (drive.getName() != null && drive.getId() != null) {
            drives.putIfAbsent(drive.getName(), drive.getId());
          }
        });
    driveIds.put(siteId, drives);
    return drives;
  }

  /**
   * @return the id of the document library or null if the site or the library does not exist
   */
  @Nullable String findDriveId(SharePointFileName name) throws FileSystemException {
    String siteId = findSiteId(name.getSiteName());
    if (siteId == null) {
      return null;
    }
    Map<String, String> drives = driveIds.get(siteId);
    boolean cached = drives != null;
    if (!cached) {
      drives = listDrives(siteId);
    }
    String driveId = drives.get(name.getDriveName());
    // The library may have been created since the libraries were listed
    if (driveId == null && cached) {
      driveId = listDrives(siteId).get(name.getDriveName());
    }
    return driveId;
  }

  /**
   * Reads the metadata of an item addressed by its path in the document library.
   *
   * @param encodedPath the URL encoded path, empty for the root folder of the library
   * @return the item or null if it does not exist
   */
  @Nullable DriveItem findItem(String driveId, String encodedPath) {
    try {
      return item(driveId, ROOT_ITEM_ID).withUrl(itemUrl(driveId, encodedPath)).get();
    } catch (ApiException e) {
      if (e.getResponseStatusCode() == HTTP_NOT_FOUND) {
        return null;
      }
      throw e;
    }
  }

  List<DriveItem> listChildren(String driveId, String itemId) {
    List<DriveItem> children = new ArrayList<>();
    ChildrenRequestBuilder request = item(driveId, itemId).children();
    forEachPaged(
        // A folder can hold thousands of items, the unused properties are left out of the answer
        request.get(configuration -> configuration.queryParameters.select = ITEM_PROPERTIES),
        DriveItemCollectionResponse::getValue,
        nextLink -> request.withUrl(nextLink).get(),
        children::add);
    return children;
  }

  /**
   * Hands every element of a Microsoft Graph collection to a consumer, following the links to the
   * next pages.
   *
   * @param firstPage the answer to the request of the collection
   * @param values reads the elements of a page
   * @param nextPage requests a page with the link given by the previous one
   */
  private static <P extends BaseCollectionPaginationCountResponse, T> void forEachPaged(
      @Nullable P firstPage,
      Function<P, @Nullable List<T>> values,
      Function<String, @Nullable P> nextPage,
      Consumer<T> consumer) {
    P page = firstPage;
    while (page != null) {
      List<T> pageValues = values.apply(page);
      if (pageValues != null) {
        pageValues.forEach(consumer);
      }
      String nextLink = page.getOdataNextLink();
      page = nextLink == null ? null : nextPage.apply(nextLink);
    }
  }

  /** A map keyed by the name of a site or of a document library, whatever the case of the name. */
  private static Map<String, String> newNameMap() {
    return Collections.synchronizedMap(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
  }

  @Nullable InputStream download(String driveId, String itemId) {
    return item(driveId, itemId).content().get();
  }

  /**
   * Creates or replaces a file with the content of a local file.
   *
   * @param encodedPath the URL encoded path of the file in the document library
   * @return the uploaded item
   */
  @Nullable DriveItem upload(String driveId, String encodedPath, Path content) throws Exception {
    long size = Files.size(content);
    String url = itemUrl(driveId, encodedPath);
    if (size <= SIMPLE_UPLOAD_LIMIT) {
      // An in-memory stream has a known length and can be sent again when the request is retried
      InputStream stream = new ByteArrayInputStream(Files.readAllBytes(content));
      return item(driveId, ROOT_ITEM_ID).content().withUrl(url + ":/content").put(stream);
    }
    try (InputStream stream = Files.newInputStream(content)) {
      DriveItemUploadableProperties properties = new DriveItemUploadableProperties();
      properties.getAdditionalData().put(CONFLICT_BEHAVIOR, "replace");
      CreateUploadSessionPostRequestBody body = new CreateUploadSessionPostRequestBody();
      body.setItem(properties);
      UploadSession session =
          item(driveId, ROOT_ITEM_ID)
              .createUploadSession()
              .withUrl(url + ":/createUploadSession")
              .post(body);

      LargeFileUploadTask<DriveItem> task =
          new LargeFileUploadTask<>(
              client.getRequestAdapter(),
              session,
              stream,
              size,
              DriveItem::createFromDiscriminatorValue);
      UploadResult<DriveItem> result = task.upload();
      if (!result.isUploadSuccessful()) {
        throw new IOException("The upload session did not complete");
      }
      // The session does not always return the item
      return result.itemResponse != null ? result.itemResponse : findItem(driveId, encodedPath);
    }
  }

  /**
   * Creates a folder, a folder created by someone else in the meantime is not an error.
   *
   * @param encodedPath the URL encoded path of the folder in the document library
   * @return the created or existing folder
   */
  DriveItem createFolder(
      String driveId, String parentItemId, String folderName, String encodedPath) {
    DriveItem folder = new DriveItem();
    folder.setName(folderName);
    folder.setFolder(new Folder());
    folder.getAdditionalData().put(CONFLICT_BEHAVIOR, "fail");
    try {
      return item(driveId, parentItemId).children().post(folder);
    } catch (ApiException e) {
      DriveItem existing =
          e.getResponseStatusCode() == HTTP_CONFLICT ? findItem(driveId, encodedPath) : null;
      if (existing == null || existing.getFolder() == null) {
        throw e;
      }
      return existing;
    }
  }

  void delete(String driveId, String itemId) {
    item(driveId, itemId).delete();
  }

  /** Renames an item and moves it to another folder of the same document library. */
  DriveItem move(String driveId, String itemId, String newParentItemId, String newName) {
    ItemReference parent = new ItemReference();
    parent.setId(newParentItemId);
    DriveItem update = new DriveItem();
    update.setName(newName);
    update.setParentReference(parent);
    return item(driveId, itemId).patch(update);
  }

  private DriveItemItemRequestBuilder item(String driveId, String itemId) {
    return client.drives().byDriveId(driveId).items().byDriveItemId(itemId);
  }

  private String itemUrl(String driveId, String encodedPath) {
    String url = client.getRequestAdapter().getBaseUrl() + "/drives/" + driveId + "/root";
    return encodedPath.isEmpty() ? url : url + ":/" + encodedPath;
  }
}
