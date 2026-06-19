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

package org.apache.hop.core.plugins;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.annotations.TransformEngineSupport;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Unit tests for {@link TransformEngineSupportPluginType} — the fragment plugin type that lets
 * third-party jars attach engine-compatibility metadata to transforms they do not own.
 *
 * <p>The tests do not load real {@code @Transform} classes (avoiding the find-annotated-class-files
 * scan); instead they exercise the merge path directly via a real {@link Plugin} host and a
 * synthetic fragment {@link Plugin}. {@link BaseFragmentType}'s listener wiring is verified by the
 * existing {@code PluginRegistryUnitTest#testMergingPluginFragment}.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class TransformEngineSupportPluginTypeTest {

  @Test
  void singletonReturnsSameInstance() {
    assertSame(
        TransformEngineSupportPluginType.getInstance(),
        TransformEngineSupportPluginType.getInstance());
  }

  @Test
  void pluginTypeIsFragment() {
    assertTrue(TransformEngineSupportPluginType.getInstance().isFragment());
  }

  @Test
  void extractIdReadsAnnotationId() {
    TransformEngineSupportPluginType type = TransformEngineSupportPluginType.getInstance();
    TransformEngineSupport ann = HostHasLocalOnly.class.getAnnotation(TransformEngineSupport.class);
    assertNotNull(ann);
    // The fragment carries id="HostId" — see HostHasLocalOnly annotation below.
    assertEquals("HostId", type.extractID(ann));
  }

  @Test
  void extractEngineArraysComeFromAnnotation() {
    TransformEngineSupportPluginType type = TransformEngineSupportPluginType.getInstance();
    TransformEngineSupport supported =
        FragmentAllowsBeam.class.getAnnotation(TransformEngineSupport.class);
    assertArrayEquals(new String[] {"Beam*"}, type.extractSupportedEngines(supported));
    assertArrayEquals(new String[0], type.extractExcludedEngines(supported));

    TransformEngineSupport excluded =
        FragmentExcludesRemote.class.getAnnotation(TransformEngineSupport.class);
    assertArrayEquals(new String[0], type.extractSupportedEngines(excluded));
    assertArrayEquals(new String[] {"Remote*"}, type.extractExcludedEngines(excluded));
  }

  /**
   * Verify the real {@link Plugin}.{@code merge} path: a host carrying {@code
   * supportedEngines={"Local"}} merged with a fragment carrying {@code supportedEngines={"Beam*"}}
   * yields the unioned list (order preserved, deduplicated).
   */
  @Test
  void mergeUnionsSupportedEngines() {
    IPlugin host = newHostPlugin("HostId", new String[] {"Local"}, new String[0]);
    IPlugin fragment = newFragmentPlugin("HostId", new String[] {"Beam*"}, new String[0]);

    host.merge(fragment);

    assertArrayEquals(new String[] {"Local", "Beam*"}, host.getSupportedEngines());
    assertArrayEquals(new String[0], host.getExcludedEngines());
  }

  /** The reverse order is irrelevant for unit-merge — the resolver dedups order-preserving. */
  @Test
  void mergeDeduplicatesIdenticalEntries() {
    IPlugin host = newHostPlugin("HostId", new String[] {"Local", "Beam*"}, new String[0]);
    IPlugin fragment = newFragmentPlugin("HostId", new String[] {"Beam*"}, new String[0]);

    host.merge(fragment);

    assertArrayEquals(new String[] {"Local", "Beam*"}, host.getSupportedEngines());
  }

  @Test
  void mergeUnionsExcludedEngines() {
    IPlugin host = newHostPlugin("HostId", new String[0], new String[] {"Beam*"});
    IPlugin fragment = newFragmentPlugin("HostId", new String[0], new String[] {"Remote*"});

    host.merge(fragment);

    assertArrayEquals(new String[] {"Beam*", "Remote*"}, host.getExcludedEngines());
  }

  /**
   * The mutual-exclusion check on {@code @Transform} / fragment annotations is enforced inside
   * {@link BasePluginType#handlePluginAnnotation}. Driving a fragment that declares <em>both</em>
   * non-empty arrays must throw at registration time.
   */
  @Test
  void mutualExclusionThrowsAtRegistration() {
    TransformEngineSupportPluginType type = TransformEngineSupportPluginType.getInstance();
    HopPluginException ex =
        assertThrows(
            HopPluginException.class,
            () -> type.registerClassPathPlugin(FragmentBothAllowAndDeny.class));
    assertTrue(
        ex.getMessage().contains("supportedEngines") && ex.getMessage().contains("excludedEngines"),
        "Expected mutual-exclusion message, got: " + ex.getMessage());
  }

  // ---- helpers --------------------------------------------------------------------------------

  private static IPlugin newHostPlugin(String id, String[] supported, String[] excluded) {
    Map<Class<?>, String> classMap = new HashMap<>();
    Plugin p =
        new Plugin(
            new String[] {id},
            TransformPluginType.class,
            String.class, // mainType — unused by these assertions
            "cat",
            id,
            "desc",
            "img",
            false,
            true,
            classMap,
            new java.util.ArrayList<>(),
            null,
            new String[0],
            null,
            false);
    p.setSupportedEngines(supported);
    p.setExcludedEngines(excluded);
    return p;
  }

  private static IPlugin newFragmentPlugin(String id, String[] supported, String[] excluded) {
    Map<Class<?>, String> classMap = new HashMap<>();
    Plugin p =
        new Plugin(
            new String[] {id},
            TransformEngineSupportPluginType.class,
            String.class,
            null,
            id,
            null,
            null,
            false,
            true,
            classMap,
            new java.util.ArrayList<>(),
            null,
            new String[0],
            null,
            false);
    p.setSupportedEngines(supported);
    p.setExcludedEngines(excluded);
    return p;
  }

  // ---- fixture annotations --------------------------------------------------------------------

  @TransformEngineSupport(
      id = "HostId",
      supportedEngines = {"Local"})
  private static final class HostHasLocalOnly {}

  @TransformEngineSupport(
      id = "HostId",
      supportedEngines = {"Beam*"})
  private static final class FragmentAllowsBeam {}

  @TransformEngineSupport(
      id = "HostId",
      excludedEngines = {"Remote*"})
  private static final class FragmentExcludesRemote {}

  @TransformEngineSupport(
      id = "HostId",
      supportedEngines = {"Local"},
      excludedEngines = {"Beam*"})
  private static final class FragmentBothAllowAndDeny {}
}
