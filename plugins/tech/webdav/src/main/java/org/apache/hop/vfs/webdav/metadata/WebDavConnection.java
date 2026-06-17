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
package org.apache.hop.vfs.webdav.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * Named WebDAV connection. Use URLs {@code <connection-name>:///path/under/root} where {@code
 * rootUrl} is a full Commons VFS WebDAV root (for example {@code
 * webdav4://localhost/remote.php/dav/files/admin/}). Credentials are stored here and applied via
 * VFS options, not embedded in the URL.
 */
@Getter
@Setter
@HopMetadata(
    key = "WebDavConnectionDefinition",
    name = "i18n::WebDavConnection.Name",
    description = "i18n::WebDavConnection.Description",
    image = "ui/images/authentication.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/webdav-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_WEBDAV_CONNECTION)
public class WebDavConnection extends HopMetadataBase implements Serializable, IHopMetadata {

  @HopMetadataProperty private String description;

  /**
   * Full WebDAV root URL including scheme {@code webdav4://} or {@code webdav4s://}, host, optional
   * port, and path ending at the user's DAV files root (typically with a trailing slash).
   */
  @HopMetadataProperty private String rootUrl;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private boolean followRedirects;

  @HopMetadataProperty private boolean preemptiveAuth;

  public WebDavConnection() {
    followRedirects = true;
    preemptiveAuth = true;
  }
}
