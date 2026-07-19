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

package org.apache.hop.marketplace.install;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/**
 * Resolves the Hop installation directory (the folder that contains {@code plugins/}).
 *
 * <p>Order: {@code HOP_HOME} / {@code hop.home} if they point at a valid install, then {@code
 * user.dir}, then parent of {@code user.dir}. A relative {@code HOP_HOME} that is resolved against
 * a cwd already inside the install (e.g. {@code assemblies/client/target/hop} while sitting in that
 * directory) is ignored so we do not double the path.
 */
public final class HopHome {

  public static final String ENV_HOP_HOME = "HOP_HOME";
  public static final String PROP_HOP_HOME = "hop.home";

  private HopHome() {}

  public static Path resolve() throws HopException {
    List<String> tried = new ArrayList<>();
    for (Path candidate : candidates()) {
      Path abs = candidate.toAbsolutePath().normalize();
      tried.add(abs.toString());
      if (isHopHome(abs)) {
        return abs;
      }
    }
    throw new HopException(
        "Cannot determine Hop installation directory (need a folder containing plugins/)."
            + " Tried: "
            + String.join(", ", tried)
            + ". Unset a relative HOP_HOME if you are already in the hop install, or set HOP_HOME"
            + " to an absolute path.");
  }

  private static Set<Path> candidates() {
    // LinkedHashSet preserves order and drops duplicates after normalize
    Set<Path> paths = new LinkedHashSet<>();
    String fromEnv = System.getenv(ENV_HOP_HOME);
    if (StringUtils.isNotBlank(fromEnv)) {
      paths.add(Paths.get(fromEnv.trim()));
    }
    String fromProp = System.getProperty(PROP_HOP_HOME);
    if (StringUtils.isNotBlank(fromProp)) {
      paths.add(Paths.get(fromProp.trim()));
    }
    Path cwd = Paths.get(System.getProperty("user.dir", "."));
    paths.add(cwd);
    Path parent = cwd.toAbsolutePath().normalize().getParent();
    if (parent != null) {
      paths.add(parent);
    }
    return paths;
  }

  /** True if path is a directory that looks like a Hop install (has {@code plugins/}). */
  static boolean isHopHome(Path path) {
    return path != null && Files.isDirectory(path) && Files.isDirectory(path.resolve("plugins"));
  }
}
