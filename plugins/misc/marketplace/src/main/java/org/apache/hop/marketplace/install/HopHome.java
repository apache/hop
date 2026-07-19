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
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/** Resolves the Hop installation directory (the folder that contains {@code plugins/}). */
public final class HopHome {

  public static final String ENV_HOP_HOME = "HOP_HOME";
  public static final String PROP_HOP_HOME = "hop.home";

  private HopHome() {}

  public static Path resolve() throws HopException {
    String fromEnv = System.getenv(ENV_HOP_HOME);
    if (StringUtils.isNotBlank(fromEnv)) {
      return validate(Paths.get(fromEnv));
    }
    String fromProp = System.getProperty(PROP_HOP_HOME);
    if (StringUtils.isNotBlank(fromProp)) {
      return validate(Paths.get(fromProp));
    }
    Path cwd = Paths.get(System.getProperty("user.dir"));
    if (Files.isDirectory(cwd.resolve("plugins"))) {
      return cwd.toAbsolutePath().normalize();
    }
    // hop-gui often runs with cwd = hop home already; otherwise walk up one level
    Path parent = cwd.getParent();
    if (parent != null && Files.isDirectory(parent.resolve("plugins"))) {
      return parent.toAbsolutePath().normalize();
    }
    throw new HopException(
        "Cannot determine Hop installation directory. Set HOP_HOME or run from the Hop install root"
            + " (the directory that contains plugins/).");
  }

  private static Path validate(Path path) throws HopException {
    Path abs = path.toAbsolutePath().normalize();
    if (!Files.isDirectory(abs)) {
      throw new HopException("Hop home is not a directory: " + abs);
    }
    return abs;
  }
}
