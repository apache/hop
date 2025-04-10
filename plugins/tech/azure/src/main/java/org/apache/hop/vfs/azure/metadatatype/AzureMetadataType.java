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
package org.apache.hop.vfs.azure.metadatatype;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "AzureConnectionDefinition",
    name = "i18n::AzureMetadataType.Name",
    description = "i18n::AzureMetadataType.Description",
    image = "ui/images/authentication.svg",
    documentationUrl = "/metadata-types/azure-authentication.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_AZURE_CONNECTION)
@Getter
@Setter
public class AzureMetadataType extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final Class<?> PKG = AzureMetadataType.class;
  @HopMetadataProperty private String description;
  @HopMetadataProperty private String storageAccountName;

  @HopMetadataProperty(password = true)
  private String storageAccountKey;

  @HopMetadataProperty private String storageAccountEndpoint;

  public AzureMetadataType() {
    // Do nothing
  }
}
