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
package org.apache.hop.naming.gui;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.imp.HopImportBase;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeSelector;
import org.apache.hop.naming.metadata.NamingSchemeType;

/**
 * When a Kettle import is about to normalize relational connection names, apply the unique
 * hop-metadata (else general) naming scheme from the target metadata provider, or an explicitly
 * selected scheme name.
 */
@ExtensionPoint(
    id = "NamingSchemeHopImportRewriteMetadata",
    description = "Apply a naming scheme to imported relational connection names",
    extensionPointId = "HopImportRewriteMetadata")
public class NamingSchemeImportExtension implements IExtensionPoint<HopImportBase> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, HopImportBase hopImport)
      throws HopException {
    if (hopImport == null || !hopImport.isApplyNamingSchemes()) {
      return;
    }
    IHopMetadataProvider provider = hopImport.getMetadataProvider();
    if (provider == null) {
      return;
    }
    List<NamingScheme> schemes;
    try {
      schemes = provider.getSerializer(NamingScheme.class).loadAll();
    } catch (Exception e) {
      if (log != null) {
        log.logError("Unable to load naming schemes from the import target metadata", e);
      }
      return;
    }
    NamingScheme scheme =
        NamingSchemeSelector.resolve(
            schemes, NamingSchemeType.HOP_METADATA.getCode(), hopImport.getNamingSchemeName());
    if (scheme == null && StringUtils.isEmpty(hopImport.getNamingSchemeName())) {
      scheme = NamingSchemeSelector.resolve(schemes, NamingSchemeType.GENERAL.getCode(), null);
    }
    if (scheme == null) {
      if (log != null && StringUtils.isNotEmpty(hopImport.getNamingSchemeName())) {
        log.logError(
            "Naming scheme '"
                + hopImport.getNamingSchemeName()
                + "' was not found in the target metadata");
      }
      return;
    }
    NamingScheme chosen = scheme;
    hopImport.setAppliedNamingSchemeName(chosen.getName());
    hopImport.setConnectionNameMapper(
        name -> NamingEngine.apply(chosen, name, NamingSchemeType.HOP_METADATA.getCode()));
    if (log != null) {
      log.logBasic(
          "Applying naming scheme '"
              + chosen.getName()
              + "' to imported relational connection names");
    }
  }
}
