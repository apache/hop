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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopHomeTest {

  @TempDir Path tempDir;

  @Test
  void isHopHomeRequiresPluginsDir() throws Exception {
    Path hop = tempDir.resolve("hop");
    Files.createDirectories(hop);
    assertFalse(HopHome.isHopHome(hop));
    Files.createDirectories(hop.resolve("plugins"));
    assertTrue(HopHome.isHopHome(hop));
  }

  @Test
  void resolveUsesUserDirWhenItIsHopHome() throws Exception {
    Path hop = tempDir.resolve("hop");
    Files.createDirectories(hop.resolve("plugins"));
    String previous = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", hop.toString());
      assertEquals(hop.toAbsolutePath().normalize(), HopHome.resolve());
    } finally {
      System.setProperty("user.dir", previous);
    }
  }

  @Test
  void resolveFallsBackToParentOfUserDir() throws Exception {
    Path hop = tempDir.resolve("hop");
    Files.createDirectories(hop.resolve("plugins"));
    Path nested = hop.resolve("subdir");
    Files.createDirectories(nested);
    String previous = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", nested.toString());
      assertEquals(hop.toAbsolutePath().normalize(), HopHome.resolve());
    } finally {
      System.setProperty("user.dir", previous);
    }
  }
}
