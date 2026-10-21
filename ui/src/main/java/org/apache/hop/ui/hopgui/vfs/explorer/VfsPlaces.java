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

package org.apache.hop.ui.hopgui.vfs.explorer;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.IVfsBrowseLocation;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * The places listed next to the folder tree: the project, the named VFS connections, the local
 * roots and the bookmarks. No SWT, so the lists can be tested headless.
 */
public final class VfsPlaces {

  public enum Kind {
    PROJECT,
    CONNECTION,
    LOCAL,
    BOOKMARK
  }

  /** One entry in the places list. */
  @Getter
  public static final class Place {
    private final Kind kind;
    private final String name;
    private final String uri;

    /** Image of the metadata type for a connection, otherwise null. */
    private final String image;

    /** Loads {@link #image}: connection types live in plugins. */
    private final ClassLoader classLoader;

    public Place(Kind kind, String name, String uri, String image, ClassLoader classLoader) {
      this.kind = kind;
      this.name = name == null ? "" : name;
      this.uri = uri == null ? "" : uri;
      this.image = image;
      this.classLoader = classLoader;
    }
  }

  private static final Comparator<Place> BY_NAME =
      Comparator.comparing(Place::getName, String.CASE_INSENSITIVE_ORDER);

  private VfsPlaces() {}

  /** The project home, when a project is enabled. */
  public static List<Place> project(IVariables variables, String label) {
    if (variables == null) {
      return List.of();
    }
    String home = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isBlank(home)) {
      return List.of();
    }
    return List.of(new Place(Kind.PROJECT, label, variables.resolve(home), null, null));
  }

  /**
   * Every saved connection whose type can be browsed, sorted by name. A type that fails to load is
   * logged and left out.
   */
  public static List<Place> connections(IHopMetadataProvider provider, IVariables variables) {
    List<Place> places = new ArrayList<>();
    if (provider == null) {
      return places;
    }
    List<Class<IHopMetadata>> classes;
    try {
      classes = provider.getMetadataClasses();
    } catch (Exception e) {
      LogChannel.UI.logError("Error listing metadata types for the VFS File Explorer", e);
      return places;
    }
    for (Class<IHopMetadata> metadataClass : classes) {
      if (!IVfsBrowseLocation.class.isAssignableFrom(metadataClass)) {
        continue;
      }
      HopMetadata annotation = metadataClass.getAnnotation(HopMetadata.class);
      String image = annotation == null ? null : annotation.image();
      try {
        IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
        for (IHopMetadata metadata : serializer.loadAll()) {
          String root = ((IVfsBrowseLocation) metadata).getBrowseRoot(variables);
          if (StringUtils.isNotBlank(root)) {
            places.add(
                new Place(
                    Kind.CONNECTION,
                    metadata.getName(),
                    root,
                    image,
                    metadataClass.getClassLoader()));
          }
        }
      } catch (Exception e) {
        LogChannel.UI.logError(
            "Error loading " + metadataClass.getSimpleName() + " connections for the explorer", e);
      }
    }
    places.sort(BY_NAME);
    return places;
  }

  /** The user home and the file system roots: drive letters on Windows, {@code /} elsewhere. */
  public static List<Place> local(String homeLabel) {
    List<Place> places = new ArrayList<>();
    String home = System.getProperty("user.home");
    if (StringUtils.isNotBlank(home)) {
      places.add(new Place(Kind.LOCAL, homeLabel, home, null, null));
    }
    for (Path root : FileSystems.getDefault().getRootDirectories()) {
      String path = root.toString();
      places.add(new Place(Kind.LOCAL, path, path, null, null));
    }
    return places;
  }

  public static List<Place> bookmarks(Map<String, String> bookmarks) {
    List<Place> places = new ArrayList<>();
    if (bookmarks != null) {
      bookmarks.forEach((name, uri) -> places.add(new Place(Kind.BOOKMARK, name, uri, null, null)));
    }
    places.sort(BY_NAME);
    return places;
  }
}
