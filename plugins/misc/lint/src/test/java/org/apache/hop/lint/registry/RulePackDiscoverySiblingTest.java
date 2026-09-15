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
package org.apache.hop.lint.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

public class RulePackDiscoverySiblingTest {

  @Test
  public void discoversVendorYamlFromSiblingMiscPluginFolder() throws Exception {
    Path root = Files.createTempDirectory("hop-lint-discovery");
    try {
      Path plugins = root.resolve("plugins/misc");
      Path engineDir = plugins.resolve("hop-lint");
      Path vendorDir = plugins.resolve("vendor-hop-lint-rules");
      Files.createDirectories(engineDir);
      Files.createDirectories(vendorDir);

      Files.writeString(
          engineDir.resolve("hop-lint-core.yml"),
          """
                pack:
                  id: hop-core
                  owner: APACHE
                rules:
                  DOC-001:
                    type: custom
                    enabled: true
                    severity: WARNING
                    target: PIPELINE
                    targetField: description
                    condition: NOT_EMPTY
                    name: Pipeline Description Required
                """);

      Files.writeString(
          vendorDir.resolve("hop-lint-pack.yml"),
          """
                pack:
                  id: vendor-pack
                  owner: VENDOR
                  priority: 200
                rules:
                  TRANS-001:
                    type: custom
                    enabled: true
                    severity: WARNING
                    target: PIPELINE
                    targetField: transformCount
                    condition: MAX_VALUE
                    conditionValue: "20"
                    name: Max Transforms In Pipeline
                """);

      RulePackDiscovery discovery = new RulePackDiscovery();
      java.util.Map<String, IHopLintRulePack> packsById = new java.util.LinkedHashMap<>();
      discovery.registerInstalledPluginYamlPacksFromEngineDir(engineDir.toFile(), packsById);

      assertNotNull(packsById.get(RulePackIds.HOP_CORE));
      assertNotNull(packsById.get("vendor-pack"));
      assertEquals(1, packsById.get(RulePackIds.HOP_CORE).loadRules().size());
      assertEquals(RulePackOwner.VENDOR, packsById.get("vendor-pack").getOwner());
      assertEquals(1, packsById.get("vendor-pack").loadRules().size());
      assertTrue(
          packsById.get("vendor-pack").loadRules().stream()
              .anyMatch(rule -> "TRANS-001".equals(rule.generateRuleId())));
    } finally {
      deleteRecursively(root.toFile());
    }
  }

  private static void deleteRecursively(File file) {
    if (file == null || !file.exists()) {
      return;
    }
    File[] children = file.listFiles();
    if (children != null) {
      for (File child : children) {
        deleteRecursively(child);
      }
    }
    file.delete();
  }
}
