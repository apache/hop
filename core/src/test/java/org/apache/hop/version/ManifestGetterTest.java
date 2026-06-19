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

package org.apache.hop.version;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URL;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;

/**
 * {@link ManifestGetter} only supports {@code META-INF/MANIFEST.MF} resolved from a {@code jar:}
 * URL. When the first manifest on the classpath comes from a directory ({@code file:}), the call
 * fails with {@link ClassCastException}; this test is skipped in that case.
 */
class ManifestGetterTest {

  @Test
  void getManifest_readsFromJarUrlWhenApplicable() throws Exception {
    ManifestGetter getter = new ManifestGetter();
    URL url = getter.getClass().getResource("/META-INF/MANIFEST.MF");
    assumeTrue(url != null, "META-INF/MANIFEST.MF not found on classpath");
    assumeTrue(
        "jar".equalsIgnoreCase(url.getProtocol()),
        "ManifestGetter requires a JAR manifest URL, got: " + url);

    Manifest manifest = getter.getManifest();
    assertNotNull(manifest);
    assertNotNull(manifest.getMainAttributes());
  }
}
