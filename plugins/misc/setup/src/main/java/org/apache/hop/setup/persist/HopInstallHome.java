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

package org.apache.hop.setup.persist;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.setup.HopSetupException;

/** Resolves the Hop client install directory (folder containing {@code plugins/}). */
public final class HopInstallHome {

  private HopInstallHome() {}

  public static Path resolve() throws HopSetupException {
    List<String> tried = new ArrayList<>();
    for (Path candidate : candidates()) {
      Path abs = candidate.toAbsolutePath().normalize();
      tried.add(abs.toString());
      if (isHopHome(abs)) {
        return abs;
      }
    }
    throw new HopSetupException(
        "Cannot determine Hop installation directory (need a folder containing plugins/)."
            + " Tried: "
            + String.join(", ", tried));
  }

  public static Path resolveOrNull() {
    try {
      return resolve();
    } catch (HopSetupException e) {
      return null;
    }
  }

  private static Set<Path> candidates() {
    Set<Path> paths = new LinkedHashSet<>();
    Path cwd = Paths.get(System.getProperty("user.dir", "."));
    paths.add(cwd);
    Path parent = cwd.toAbsolutePath().normalize().getParent();
    if (parent != null) {
      paths.add(parent);
    }
    return paths;
  }

  static boolean isHopHome(Path path) {
    return path != null && Files.isDirectory(path) && Files.isDirectory(path.resolve("plugins"));
  }
}
