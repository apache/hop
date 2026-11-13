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

package org.apache.hop.datacatalog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** A data catalog Hop reads dataset metadata from, such as DataHub. Experimental. */
@HopMetadata(
    key = "data-catalog",
    name = "i18n::DataCatalog.name",
    description = "i18n::DataCatalog.description",
    image = "ui/images/catalog.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/data-catalog.html",
    hopMetadataPropertyType = HopMetadataPropertyType.DATA_CATALOG,
    supportsGlobalReplace = true)
@Getter
@Setter
public class DataCatalog extends HopMetadataBase implements IHopMetadata, Cloneable {

  /** The parent id of the widgets of a catalog type in the editor. */
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DataCatalog-PluginSpecific-Options";

  @HopMetadataProperty private String description;

  /** The catalog type and its configuration. */
  @HopMetadataProperty private IDataCatalog catalog;

  public DataCatalog() {}

  public DataCatalog(String name, String description, IDataCatalog catalog) {
    super(name);
    this.description = description;
    this.catalog = catalog;
  }

  public DataCatalog(DataCatalog other) {
    this(other.name, other.description, other.catalog == null ? null : other.catalog.clone());
  }

  @Override
  public DataCatalog clone() {
    return new DataCatalog(this);
  }

  /**
   * An initialized copy of the catalog, ready to answer questions. Close it when done.
   *
   * @throws HopException when no catalog type is set or it can't be initialized
   */
  public IDataCatalog connect(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    if (catalog == null) {
      throw new HopException("Data catalog '" + name + "' has no catalog type");
    }
    IDataCatalog copy = catalog.clone();
    try {
      copy.initialize(variables, metadataProvider);
    } catch (HopException | RuntimeException e) {
      copy.close();
      throw e;
    }
    return copy;
  }
}
