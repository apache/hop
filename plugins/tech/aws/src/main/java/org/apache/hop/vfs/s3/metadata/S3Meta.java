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
package org.apache.hop.vfs.s3.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "S3ConnectionDefinition",
    name = "i18n::S3Meta.name",
    description = "i18n::S3Meta.description",
    image = "s3.svg",
    documentationUrl = "/metadata-types/s3-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_S3_CONNECTION)
public class S3Meta extends HopMetadataBase implements Serializable, IHopMetadata {

  @HopMetadataProperty private String description;

  @HopMetadataProperty(password = true)
  private String accessKey;

  @HopMetadataProperty(password = true)
  private String secretKey;

  @HopMetadataProperty(password = true)
  private String sessionToken;

  @HopMetadataProperty private String region;

  @HopMetadataProperty private String endpoint;

  @HopMetadataProperty private boolean pathStyleAccess;

  @HopMetadataProperty private String credentialsFile;

  @HopMetadataProperty private String profileName;

  @HopMetadataProperty private String partSize;

  /** Cache TTL in seconds for list-result caching (avoids redundant headObject calls). */
  @HopMetadataProperty private String cacheTtlSeconds;

  /** Authentication type: {@link S3AuthType#name()} (Default, ACCESS_KEYS, CREDENTIALS_FILE). */
  @HopMetadataProperty private String authenticationType;

  public S3Meta() {
    partSize = "5MB";
    cacheTtlSeconds = "5";
    authenticationType = S3AuthType.DEFAULT.name();
  }
}
