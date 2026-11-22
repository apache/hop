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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Reads the fields a data catalog has for a dataset. */
public final class CatalogSchemas {
  private static final Class<?> PKG = CatalogSchemas.class;

  private CatalogSchemas() {}

  /**
   * The fields a data catalog has for a dataset.
   *
   * @throws HopException when there is no such catalog or dataset, or the catalog doesn't answer
   */
  public static List<CatalogField> read(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String catalogName,
      CatalogDataset dataset)
      throws HopException {
    String resolvedName = variables.resolve(catalogName);
    if (StringUtils.isBlank(resolvedName)) {
      throw new HopException(BaseMessages.getString(PKG, "CatalogSchemas.Error.NoCatalog"));
    }
    DataCatalog dataCatalog = metadataProvider.getSerializer(DataCatalog.class).load(resolvedName);
    if (dataCatalog == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "CatalogSchemas.Error.MissingCatalog", resolvedName));
    }
    DatasetRef ref = dataset.toRef(variables, metadataProvider);
    try (IDataCatalog catalog = dataCatalog.connect(variables, metadataProvider)) {
      ISchemaCapability schemaCapability =
          catalog
              .capability(ISchemaCapability.class)
              .orElseThrow(
                  () ->
                      new HopException(
                          BaseMessages.getString(
                              PKG, "CatalogSchemas.Error.NoSchemaSupport", resolvedName)));
      CatalogEntityId id = known(catalog.resolve(ref), resolvedName, ref);
      List<CatalogField> fields = known(schemaCapability.schema(id), resolvedName, ref);
      if (fields == null || fields.isEmpty()) {
        // Nothing to fill a field table with: say so rather than change nothing
        throw new HopException(
            BaseMessages.getString(
                PKG, "CatalogSchemas.Error.NotFound", resolvedName, ref.describe()));
      }
      return fields;
    }
  }

  private static <T> T known(CatalogAnswer<T> answer, String catalogName, DatasetRef ref)
      throws HopException {
    return switch (answer) {
      case CatalogAnswer.Known<T> known -> known.value();
      case CatalogAnswer.NotFound<T> notFound ->
          throw new HopException(
              BaseMessages.getString(
                  PKG, "CatalogSchemas.Error.NotFound", catalogName, ref.describe()));
      case CatalogAnswer.Unknown<T> unknown ->
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "CatalogSchemas.Error.Unknown",
                  catalogName,
                  ref.describe(),
                  unknown.reason().name(),
                  StringUtils.defaultString(unknown.detail())));
    };
  }
}
