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

package org.apache.hop.projects.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.memory.SharedMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.metadata.util.MetadataPlace;
import org.apache.hop.metadata.util.MetadataPlaces;

/**
 * Names the providers of an enabled project so the metadata editor can say where an object lives.
 */
public final class ProjectMetadataPlaces {

  private ProjectMetadataPlaces() {}

  /**
   * A project in the inheritance chain.
   *
   * @param name the project name
   * @param metadataBaseFolder the unresolved metadata folder, for example {@code
   *     ${PROJECT_HOME}/metadata}
   */
  public record NamedProject(String name, String metadataBaseFolder) {}

  /**
   * @param parents parent projects, root first, in the same order as their metadata providers
   */
  public static void apply(
      MultiMetadataProvider multi, NamedProject project, List<NamedProject> parents) {
    if (multi == null) {
      return;
    }
    List<IHopMetadataProvider> providers = HopMetadataUtil.getProviders(multi);
    List<NamedProject> parentProjects = parents == null ? List.of() : parents;
    List<MetadataPlace> places = new ArrayList<>();
    int parentIndex = 0;
    for (int i = 0; i < providers.size(); i++) {
      IHopMetadataProvider provider = providers.get(i);
      boolean last = i == providers.size() - 1;
      if (provider instanceof SharedMetadataProvider shared) {
        places.add(MetadataPlaces.sharedPlace(shared));
      } else if (last && project != null) {
        places.add(projectPlace(provider, project));
      } else if (parentIndex < parentProjects.size()) {
        places.add(parentPlace(provider, parentProjects.get(parentIndex++)));
      } else {
        places.add(MetadataPlaces.generic(provider, last));
      }
    }
    multi.setPlaces(places);
  }

  private static MetadataPlace projectPlace(IHopMetadataProvider provider, NamedProject project) {
    String folder =
        Const.NVL(
            project.metadataBaseFolder(), "${" + ProjectsUtil.VARIABLE_PROJECT_HOME + "}/metadata");
    return new MetadataPlace(
        provider.getDescription(),
        "Project metadata : " + folder,
        "Stored in this project. New metadata is saved here unless another location is selected."
            + " Path: "
            + resolvedFolder(provider),
        true);
  }

  private static MetadataPlace parentPlace(IHopMetadataProvider provider, NamedProject parent) {
    String folder = Const.NVL(parent.metadataBaseFolder(), "");
    String name = Const.NVL(parent.name(), "parent");
    return new MetadataPlace(
        provider.getDescription(),
        "Parent project metadata (" + name + ") : " + folder,
        "Inherited from parent project '"
            + name
            + "'. Saving here changes that parent for every project that inherits it. Path: "
            + resolvedFolder(provider),
        false);
  }

  private static String resolvedFolder(IHopMetadataProvider provider) {
    if (provider instanceof JsonMetadataProvider json) {
      return json.getBaseFolder();
    }
    return provider.getDescription();
  }
}
