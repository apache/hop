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

package org.apache.hop.config;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.util.SharedMetadata;
import picocli.CommandLine;

/** Sets the shared metadata folder and imports a metadata export into it. */
@ConfigPlugin(id = "shared-metadata", description = "Shared metadata for the Hop configuration")
public class SharedMetadataConfigPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"--shared-metadata-folder"},
      description =
          "Folder for metadata shared by the whole Hop configuration. Stored in hop-config.json."
              + " An empty value clears it. Example: ${HOP_CONFIG_FOLDER}/metadata")
  private String sharedMetadataFolder;

  @CommandLine.Option(
      names = {"--shared-metadata-import-json"},
      description =
          "Import a metadata export by passing the JSON document itself. This is the same document"
              + " hop-conf --export-metadata writes. No file is read. An object of the same name"
              + " is replaced. Other objects in the folder are left in place. When no shared"
              + " folder is configured yet, ${HOP_CONFIG_FOLDER}/metadata is used and saved.")
  private String sharedMetadataImportJson;

  @CommandLine.Option(
      names = {"--shared-metadata-import-file"},
      description =
          "Import a metadata export JSON file into the shared metadata folder. Same document and"
              + " same result as --shared-metadata-import-json. Use this when the export is"
              + " already a file.")
  private String sharedMetadataImportFile;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    ILogChannel channel = log == null ? LogChannel.GENERAL : log;
    boolean changed = false;
    if (sharedMetadataFolder != null) {
      HopConfig.getInstance().saveOption(SharedMetadata.CONFIG_KEY, sharedMetadataFolder);
      channel.logBasic(
          StringUtils.isEmpty(sharedMetadataFolder)
              ? "Cleared the shared metadata folder"
              : "Shared metadata folder set to '" + sharedMetadataFolder + "'");
      changed = true;
    }
    if (StringUtils.isNotEmpty(sharedMetadataImportJson)
        && StringUtils.isNotEmpty(sharedMetadataImportFile)) {
      throw new HopException(
          "Set only one of --shared-metadata-import-json and --shared-metadata-import-file");
    }
    if (StringUtils.isNotEmpty(sharedMetadataImportJson)) {
      importJson(channel, variables, sharedMetadataImportJson);
      changed = true;
    }
    if (StringUtils.isNotEmpty(sharedMetadataImportFile)) {
      importJson(channel, variables, readExport(variables.resolve(sharedMetadataImportFile)));
      changed = true;
    }
    return changed;
  }

  private void importJson(ILogChannel log, IVariables variables, String json) throws HopException {
    String configured = HopConfig.readOptionString(SharedMetadata.CONFIG_KEY, "");
    if (StringUtils.isEmpty(StringUtils.trim(configured))) {
      configured = SharedMetadata.DEFAULT_FOLDER;
      HopConfig.getInstance().saveOption(SharedMetadata.CONFIG_KEY, configured);
      log.logBasic("Shared metadata folder set to '" + configured + "'");
    }
    String resolved = SharedMetadata.resolveFolder(variables, configured);
    SharedMetadata.checkLocalFolder(resolved);

    SerializableMetadataProvider imported = new SerializableMetadataProvider(json);
    JsonMetadataProvider store = new JsonMetadataProvider(Encr.getEncoder(), resolved, variables);
    int saved = 0;
    for (Class<IHopMetadata> managedClass : imported.getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> source = imported.getSerializer(managedClass);
      IHopMetadataSerializer<IHopMetadata> target = store.getSerializer(managedClass);
      for (IHopMetadata object : source.loadAll()) {
        if (object != null && StringUtils.isNotEmpty(object.getName())) {
          target.save(object);
          saved++;
        }
      }
    }
    log.logBasic("Imported " + saved + " shared metadata object(s) into '" + resolved + "'");
  }

  private static String readExport(String filename) throws HopException {
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new HopException("Unable to read metadata export '" + filename + "'", e);
    }
  }
}
