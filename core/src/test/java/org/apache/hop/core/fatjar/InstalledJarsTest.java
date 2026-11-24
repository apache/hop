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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InstalledJarsTest {

  @TempDir File installation;

  @Test
  void pluginJarsSkipMarketplaceStagingAndBackups() throws Exception {
    File live = touch("plugins/tech/test/lib/x.jar");
    File pluginJar = touch("plugins/tech/test/hop-test.jar");
    touch("plugins/.marketplace/backup/hop-test/1.0.0/20261121T100000Z/lib/core/x.jar");
    touch("plugins/.staging/hop-test/plugins/tech/test/hop-test.jar");
    touch("plugins/tech/test/readme.txt");

    assertEquals(
        List.of(pluginJar.toString(), live.toString()),
        InstalledJars.find(installation, file -> false));
  }

  @Test
  void aMissingInstallationHasNoJars() {
    assertTrue(InstalledJars.find(new File(installation, "missing"), file -> false).isEmpty());
  }

  private File touch(String path) throws Exception {
    File file = new File(installation, path);
    file.getParentFile().mkdirs();
    assertTrue(file.createNewFile());
    return file;
  }
}
