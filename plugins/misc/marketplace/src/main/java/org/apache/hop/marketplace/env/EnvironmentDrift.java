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

package org.apache.hop.marketplace.env;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/** Result of comparing a hop-env file against the local install. */
@Getter
public class EnvironmentDrift {
  private final List<String> missingPlugins = new ArrayList<>();
  private final List<String> versionMismatches = new ArrayList<>();
  private final List<String> extraMarketplacePlugins = new ArrayList<>();
  private final List<String> missingDependencies = new ArrayList<>();

  public boolean hasDrift() {
    return !missingPlugins.isEmpty()
        || !versionMismatches.isEmpty()
        || !extraMarketplacePlugins.isEmpty()
        || !missingDependencies.isEmpty();
  }

  public String formatReport() {
    StringBuilder sb = new StringBuilder();
    for (String s : missingPlugins) {
      sb.append("  MISSING plugin: ").append(s).append('\n');
    }
    for (String s : versionMismatches) {
      sb.append("  VERSION mismatch: ").append(s).append('\n');
    }
    for (String s : extraMarketplacePlugins) {
      sb.append("  EXTRA marketplace plugin (not in env file): ").append(s).append('\n');
    }
    for (String s : missingDependencies) {
      sb.append("  MISSING dependency jar: ").append(s).append('\n');
    }
    return sb.toString();
  }
}
