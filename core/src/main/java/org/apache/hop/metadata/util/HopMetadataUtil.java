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

package org.apache.hop.metadata.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

public class HopMetadataUtil {

  public static final MultiMetadataProvider getStandardHopMetadataProvider(IVariables variables) {

    String[] folders;
    String foldersValue = variables.getVariable(Const.HOP_METADATA_FOLDER);
    if (StringUtils.isEmpty(foldersValue)) {
      // The folder is the "metadata" folder in the configuration folder...
      //
      String configDirectory = Const.HOP_CONFIG_FOLDER;
      if (!configDirectory.endsWith(Const.FILE_SEPARATOR)) {
        configDirectory += Const.FILE_SEPARATOR;
      }
      folders = new String[] {configDirectory + "metadata"};
    } else {
      folders = foldersValue.split(",");
      for (int i = 0; i < folders.length; i++) {
        folders[i] = Const.trim(folders[i]);
      }
    }

    // Create a multi to wrap the various folders
    //
    List<IHopMetadataProvider> providers = new ArrayList<>();
    for (String folder : folders) {
      IHopMetadataProvider provider =
          new JsonMetadataProvider(Encr.getEncoder(), folder, variables);
      providers.add(provider);
    }
    return new MultiMetadataProvider(Encr.getEncoder(), providers, variables);
  }

  /**
   * The file holding a metadata object, found the way {@link
   * org.apache.hop.metadata.serializer.multi.MultiMetadataSerializer#load(String)} finds the object
   * itself: the last provider which has it wins, which is the child project when a project inherits
   * from another. Within a file based provider the folder of the current key comes before the
   * folders of the {@link HopMetadata#legacyKeys()}.
   *
   * @param provider the provider, can be a multi-provider
   * @param managedClass the metadata type
   * @param name the name of the metadata object
   * @return the file, or null when no provider has the object or the one which has it is not file
   *     based
   * @throws HopException in case of a file system error
   */
  public static <T extends IHopMetadata> String findFilename(
      IHopMetadataProvider provider, Class<T> managedClass, String name) throws HopException {
    List<IHopMetadataProvider> providers = getProviders(provider);
    for (int i = providers.size() - 1; i >= 0; i--) {
      IHopMetadataSerializer<T> serializer = providers.get(i).getSerializer(managedClass);
      if (serializer instanceof JsonMetadataSerializer jsonSerializer) {
        String filename = jsonSerializer.findFilename(name);
        if (filename != null) {
          return filename;
        }
      } else if (serializer.exists(name)) {
        return null;
      }
    }
    return null;
  }

  /**
   * The providers behind a provider, a multi-provider unwrapped, in the order it lists them: the
   * parent project first, the child project last.
   */
  public static List<IHopMetadataProvider> getProviders(IHopMetadataProvider provider) {
    List<IHopMetadataProvider> providers = new ArrayList<>();
    if (provider instanceof MultiMetadataProvider multiProvider) {
      for (IHopMetadataProvider childProvider : multiProvider.getProviders()) {
        providers.addAll(getProviders(childProvider));
      }
    } else if (provider != null) {
      providers.add(provider);
    }
    return providers;
  }

  public static <T extends IHopMetadata> HopMetadata getHopMetadataAnnotation(
      Class<T> managedClass) {
    return managedClass.getAnnotation(HopMetadata.class);
  }

  /**
   * The current key of a metadata type followed by its {@link HopMetadata#legacyKeys()}. This is
   * the order in which to look for stored objects: the current key always wins.
   *
   * @param annotation the metadata type annotation
   * @return the current key and the legacy keys, without empty or duplicate values
   */
  public static List<String> getAllKeys(HopMetadata annotation) {
    List<String> keys = new ArrayList<>();
    keys.add(Const.NVL(annotation.key(), annotation.name()));
    for (String legacyKey : annotation.legacyKeys()) {
      if (StringUtils.isNotEmpty(legacyKey) && !keys.contains(legacyKey)) {
        keys.add(legacyKey);
      }
    }
    return keys;
  }

  /**
   * @param annotation the metadata type annotation
   * @param key a metadata type key, for example from a filter or a serialized metadata export
   * @return true if the key is the current key or one of the legacy keys of the metadata type
   */
  public static boolean matchesKey(HopMetadata annotation, String key) {
    return annotation != null && key != null && getAllKeys(annotation).contains(key);
  }

  public static String[] getHopMetadataKeys(IHopMetadataProvider provider) {
    List<String> keys = new ArrayList<>();
    for (Class<IHopMetadata> metadataClass : provider.getMetadataClasses()) {
      HopMetadata hopMetadata = getHopMetadataAnnotation(metadataClass);
      keys.add(hopMetadata.key());
    }
    Collections.sort(keys);
    return keys.toArray(new String[0]);
  }
}
