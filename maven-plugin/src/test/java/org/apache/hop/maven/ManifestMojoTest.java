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
package org.apache.hop.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.hop.core.plugins.manifest.DependencyList;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.junit.jupiter.api.Test;

class ManifestMojoTest {

  @Test
  void includeScopeFollowsTheDependencyPlugin() {
    for (String scope : new String[] {"compile", "provided", "runtime", "system", "test"}) {
      assertTrue(ManifestMojo.inScope(null, scope), "no includeScope: everything");
      assertTrue(ManifestMojo.inScope("test", scope), "test: everything");
    }
    assertTrue(ManifestMojo.inScope("runtime", "compile"));
    assertTrue(ManifestMojo.inScope("runtime", "runtime"));
    assertFalse(ManifestMojo.inScope("runtime", "provided"));
    assertFalse(ManifestMojo.inScope("runtime", "test"));
    assertTrue(ManifestMojo.inScope("compile", "provided"));
    assertTrue(ManifestMojo.inScope("compile", "system"));
    assertFalse(ManifestMojo.inScope("compile", "runtime"));
    assertTrue(ManifestMojo.inScope("provided", "provided"));
    assertFalse(ManifestMojo.inScope("provided", "compile"));
  }

  @Test
  void artifactsKeepTheirCoordinatesAndTheBaseVersion() {
    DefaultArtifact snapshot =
        new DefaultArtifact(
            "org.acme",
            "lib",
            "1.0-20261121.101010-3",
            Artifact.SCOPE_PROVIDED,
            "test-jar",
            "tests",
            new DefaultArtifactHandler("test-jar"));
    snapshot.setBaseVersion("1.0-SNAPSHOT");
    snapshot.setFile(new File("/m2/org/acme/lib/1.0-SNAPSHOT/lib-1.0-SNAPSHOT-tests.jar"));

    DependencyList.Artifact listed = ManifestMojo.toListArtifact(snapshot);

    assertEquals("org.acme:lib", listed.ga());
    assertEquals("test-jar", listed.type());
    assertEquals("tests", listed.classifier());
    assertEquals("1.0-SNAPSHOT", listed.version(), "dependency:list prints the base version");
    assertEquals("provided", listed.scope());
    assertEquals("lib-1.0-SNAPSHOT-tests.jar", listed.fileName());
    assertEquals("lib-1.0-SNAPSHOT-tests.jar", listed.defaultFileName());
  }

  @Test
  void artifactsWithoutAFileGetTheAssemblyFileName() {
    DefaultArtifact release =
        new DefaultArtifact(
            "org.acme",
            "lib",
            "2.0",
            Artifact.SCOPE_COMPILE,
            "jar",
            null,
            new DefaultArtifactHandler("jar"));

    DependencyList.Artifact listed = ManifestMojo.toListArtifact(release);

    assertNull(listed.file());
    assertNull(listed.classifier());
    assertEquals("lib-2.0.jar", listed.fileName());
  }
}
