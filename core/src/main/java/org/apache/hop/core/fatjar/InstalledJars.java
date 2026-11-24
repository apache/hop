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

package org.apache.hop.core.fatjar;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The jar files of a Hop installation that go into a fat jar: everything under {@code lib/} and
 * {@code plugins/}, plus the web application libraries of a Hop Web container.
 *
 * <p>Folders whose name starts with a dot are skipped: the marketplace keeps staged downloads and
 * backed-up jars there ({@code plugins/.staging}, {@code plugins/.marketplace}), and those must not
 * end up in a fat jar next to the live ones.
 *
 * <p>The installation is the working directory, which is where the Hop scripts start the JVM.
 * Reading the installation itself is one of the few places where {@link File} is the right tool:
 * these are local jar files, never VFS locations.
 */
public final class InstalledJars {

  /** Folders, relative to the installation, that hold jar files. */
  static final String[] JAR_FOLDERS = {
    "lib", "lib/swt/linux/x86_64", "plugins", "webapps/ROOT/WEB-INF/lib"
  };

  private InstalledJars() {}

  /**
   * @param exclude folders and jar files to leave out; a folder that matches is not visited
   * @return the jar filenames of the installation in the working directory, sorted so that the
   *     "first one wins" collision handling of {@link FatJarBuilder} is stable
   */
  public static List<String> find(Predicate<File> exclude) {
    return find(new File("."), exclude);
  }

  /**
   * @param installation the Hop installation folder
   * @param exclude folders and jar files to leave out; a folder that matches is not visited
   * @return the jar filenames, sorted
   */
  public static List<String> find(File installation, Predicate<File> exclude) {
    Set<File> jars = new HashSet<>();
    for (String folder : JAR_FOLDERS) {
      File dir =
          ".".equals(installation.getPath()) ? new File(folder) : new File(installation, folder);
      collect(dir, exclude, jars);
    }
    List<String> filenames = new ArrayList<>();
    jars.forEach(file -> filenames.add(file.toString()));
    filenames.sort(String::compareTo);
    return filenames;
  }

  private static void collect(File dir, Predicate<File> exclude, Set<File> jars) {
    if (!dir.isDirectory() || exclude.test(dir)) {
      return;
    }
    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        if (!child.getName().startsWith(".")) {
          collect(child, exclude, jars);
        }
      } else if (child.isFile() && child.getName().endsWith(".jar") && !exclude.test(child)) {
        jars.add(child);
      }
    }
  }
}
