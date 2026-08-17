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

package org.apache.hop.setup;

import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/** User-level directories used to compute recommended Hop environment paths. */
@Getter
public class UserPaths {

  private final Path home;
  private final Path xdgData;
  private final Path xdgState;
  private final Path xdgConfig;
  private final String shell;

  public UserPaths(Path home, Path xdgData, Path xdgState, Path xdgConfig, String shell) {
    this.home = home;
    this.xdgData = xdgData;
    this.xdgState = xdgState;
    this.xdgConfig = xdgConfig;
    this.shell = shell;
  }

  public static UserPaths system() {
    Path home = Paths.get(System.getProperty("user.home", "."));
    return new UserPaths(
        home,
        envPath("XDG_DATA_HOME", home.resolve(".local").resolve("share")),
        envPath("XDG_STATE_HOME", home.resolve(".local").resolve("state")),
        envPath("XDG_CONFIG_HOME", home.resolve(".config")),
        System.getenv("SHELL"));
  }

  private static Path envPath(String name, Path fallback) {
    String value = System.getenv(name);
    if (StringUtils.isNotBlank(value)) {
      return Paths.get(value);
    }
    return fallback;
  }
}
