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

package org.apache.hop.core.fatjar;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import lombok.Getter;
import org.apache.hop.core.plugins.manifest.PluginManifest;
import org.apache.hop.core.plugins.manifest.PluginManifestScanner;

/**
 * The files of a Hop installation that only some plugins install, according to the plugin manifests
 * ({@link PluginManifest}): the files to leave out of a fat jar that should not contain those
 * plugins.
 *
 * <p>A fat jar for one engine leaves out the other engine this way. The Beam engine puts about 230
 * jars of its own in {@code lib/core}, next to jars it shares with other plugins, and only the
 * manifests tell the two apart. A file that any other installed plugin or engine module also claims
 * stays in.
 *
 * <p>This only leaves things out: everything else that is installed goes in, because which
 * transforms, actions and metadata types a project needs is not known yet. A fat jar for one
 * project would use the same ownership the other way round: the files of the plugins the project
 * needs, and nothing else.
 */
public final class PluginOwnedFiles {

  /** The folder of the Beam engine plugin, relative to the installation. */
  public static final String BEAM_ENGINE_FOLDER = "plugins/engines/beam";

  /** The folder of the native Spark engine plugin, relative to the installation. */
  public static final String SPARK_ENGINE_FOLDER = "plugins/engines/spark";

  private final File installation;
  private final Set<String> folders;

  /** The files only these plugins install, relative to the installation, with forward slashes. */
  @Getter private final Set<String> paths;

  /**
   * Plugins that are installed but have no manifest: nothing tells which of the jars in {@code
   * lib/core} are theirs, so only their own folder is left out.
   */
  @Getter private final List<String> foldersWithoutManifest;

  private PluginOwnedFiles(
      File installation,
      Set<String> folders,
      Set<String> paths,
      List<String> foldersWithoutManifest) {
    this.installation = installation;
    this.folders = folders;
    this.paths = Set.copyOf(paths);
    this.foldersWithoutManifest = List.copyOf(foldersWithoutManifest);
  }

  /**
   * @param installation the Hop installation folder
   * @param pluginFolders plugin folders relative to the installation, such as {@value
   *     #BEAM_ENGINE_FOLDER}
   * @return the files that only these plugins install. A plugin that is installed without a
   *     manifest, from a build before manifests existed, only has its folder left out: see {@code
   *     getFoldersWithoutManifest()}.
   */
  public static PluginOwnedFiles onlyInstalledBy(File installation, String... pluginFolders) {
    Set<String> folders = new TreeSet<>(List.of(pluginFolders));
    List<String> withoutManifest = new ArrayList<>();
    List<PluginManifest> manifests;
    try {
      manifests = PluginManifestScanner.scan(installation.toPath());
    } catch (IOException e) {
      // An unreadable manifest: leave out the folders only, like an install without manifests
      manifests = List.of();
    }

    Set<String> withManifest = new HashSet<>();
    for (PluginManifest manifest : manifests) {
      if (manifest.getFolder() != null && folders.contains(manifest.getFolder())) {
        withManifest.add(manifest.getFolder());
      }
    }
    for (String folder : folders) {
      if (new File(installation, folder).isDirectory() && !withManifest.contains(folder)) {
        withoutManifest.add(folder);
      }
    }

    Set<String> paths = new HashSet<>();
    if (!withManifest.isEmpty()) {
      for (Map.Entry<String, List<PluginManifest>> entry :
          PluginManifestScanner.owners(manifests).entrySet()) {
        if (entry.getValue().stream().allMatch(owner -> isOneOf(owner, withManifest))) {
          paths.add(entry.getKey());
        }
      }
    }
    return new PluginOwnedFiles(installation, folders, paths, withoutManifest);
  }

  private static boolean isOneOf(PluginManifest manifest, Set<String> folders) {
    return manifest.getFolder() != null && folders.contains(manifest.getFolder());
  }

  /**
   * Leaves out the plugin folders themselves, and every file only these plugins install.
   *
   * @return a filter for {@link InstalledJars#find(File, Predicate)} on the same installation
   */
  public Predicate<File> asExclusion() {
    Path root = installation.toPath().toAbsolutePath().normalize();
    return file -> {
      String relative = relative(root, file);
      return file.isDirectory() ? folders.contains(relative) : paths.contains(relative);
    };
  }

  private static String relative(Path root, File file) {
    return root.relativize(file.toPath().toAbsolutePath().normalize())
        .toString()
        .replace('\\', '/');
  }

  /** The number of jar files only these plugins install. */
  public long getJarCount() {
    return paths.stream().filter(path -> path.endsWith(".jar")).count();
  }
}
