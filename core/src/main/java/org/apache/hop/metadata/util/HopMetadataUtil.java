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

package org.apache.hop.metadata.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

import java.util.ArrayList;
import java.util.List;

public class HopMetadataUtil {

  public static final IHopMetadataProvider getStandardHopMetadataProvider(IVariables variables) {

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

    // One folder: simple JSON provider...
    //
    if (folders.length == 1) {
      return new JsonMetadataProvider(Encr.getEncoder(), folders[0], variables);
    } else {
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
  }

  public static <T extends IHopMetadata> HopMetadata getHopMetadataAnnotation(
      Class<T> managedClass) {
    HopMetadata hopMetadata = managedClass.getAnnotation(HopMetadata.class);
    return hopMetadata;
  }
}
