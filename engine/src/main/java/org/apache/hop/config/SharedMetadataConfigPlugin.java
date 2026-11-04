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
import org.apache.commons.vfs2.FileObject;
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
      names = {"--shared-metadata-import"},
      description =
          "Import a metadata export JSON file into the shared metadata folder. An object of the"
              + " same name is replaced. Other objects in the folder are left in place. When no"
              + " folder is configured yet, ${HOP_CONFIG_FOLDER}/metadata is used and saved.")
  private String sharedMetadataImport;

  @CommandLine.Option(
      names = {"--shared-metadata-import-if-empty"},
      description =
          "Import a metadata export JSON file into the shared metadata folder only when that"
              + " folder does not already contain metadata. A later start of a container leaves an"
              + " existing folder unchanged.")
  private String sharedMetadataImportIfEmpty;

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
    if (StringUtils.isNotEmpty(sharedMetadataImport)) {
      importExport(channel, variables, sharedMetadataImport, false);
      changed = true;
    }
    if (StringUtils.isNotEmpty(sharedMetadataImportIfEmpty)) {
      importExport(channel, variables, sharedMetadataImportIfEmpty, true);
      changed = true;
    }
    return changed;
  }

  private void importExport(
      ILogChannel log, IVariables variables, String filename, boolean onlyIfEmpty)
      throws HopException {
    String configured = HopConfig.readOptionString(SharedMetadata.CONFIG_KEY, "");
    if (StringUtils.isEmpty(StringUtils.trim(configured))) {
      configured = SharedMetadata.DEFAULT_FOLDER;
      HopConfig.getInstance().saveOption(SharedMetadata.CONFIG_KEY, configured);
      log.logBasic("Shared metadata folder set to '" + configured + "'");
    }
    String resolved = SharedMetadata.resolveFolder(variables, configured);
    SharedMetadata.checkLocalFolder(resolved);
    if (onlyIfEmpty && folderHasMetadata(resolved)) {
      log.logBasic(
          "Shared metadata folder '"
              + resolved
              + "' already contains metadata. Leaving it unchanged.");
      return;
    }

    String json = readExport(variables.resolve(filename));
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

  static boolean folderHasMetadata(String resolved) throws HopException {
    try (FileObject folder = HopVfs.getFileObject(resolved)) {
      if (!folder.exists() || !folder.isFolder()) {
        return false;
      }
      FileObject[] children = folder.getChildren();
      if (children == null) {
        return false;
      }
      for (FileObject child : children) {
        if (child.isFile() && child.getName().getBaseName().endsWith(".json")) {
          return true;
        }
        if (!child.isFolder()) {
          continue;
        }
        FileObject[] files = child.getChildren();
        if (files == null) {
          continue;
        }
        for (FileObject file : files) {
          if (file.isFile() && file.getName().getBaseName().endsWith(".json")) {
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      throw new HopException("Unable to read shared metadata folder '" + resolved + "'", e);
    }
  }
}
