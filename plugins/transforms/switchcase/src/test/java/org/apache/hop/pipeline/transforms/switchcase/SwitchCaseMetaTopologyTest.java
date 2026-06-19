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

package org.apache.hop.pipeline.transforms.switchcase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for topology-management methods in {@link SwitchCaseMeta}:
 *
 * <ul>
 *   <li>{@code cleanAfterHopFromRemove} – clears stale target references when a hop is removed
 *   <li>{@code searchInfoAndTargetTransforms} – resolves stored names → TransformMeta objects
 *   <li>{@code convertIOMetaToTransformNames} – flushes resolved stream names back to stored fields
 *   <li>{@code handleStreamSelection} – keeps stored names in sync when a stream is repointed
 * </ul>
 */
class SwitchCaseMetaTopologyTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private static SwitchCaseMeta metaWithTargets(
      String case1Name, String case2Name, String defaultName) {
    SwitchCaseMeta meta = new SwitchCaseMeta();
    meta.setFieldName("status");
    addCaseTarget(meta, "A", case1Name);
    addCaseTarget(meta, "B", case2Name);
    meta.setDefaultTargetTransformName(defaultName);
    return meta;
  }

  private static void addCaseTarget(SwitchCaseMeta meta, String caseValue, String targetName) {
    SwitchCaseTarget t = new SwitchCaseTarget();
    t.setCaseValue(caseValue);
    t.setCaseTargetTransformName(targetName);
    meta.getCaseTargets().add(t);
  }

  /** Minimal TransformMeta – only the name matters for topology look-ups. */
  private static TransformMeta tm(String name) {
    return new TransformMeta(name, new SwitchCaseMeta());
  }

  // ---------------------------------------------------------------------------
  // cleanAfterHopFromRemove
  // ---------------------------------------------------------------------------

  @Test
  void cleanAfterHopFromRemove_returnsFalseForNull() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    assertFalse(meta.cleanAfterHopFromRemove(null));
    assertEquals("Out1", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Default", meta.getDefaultTargetTransformName());
  }

  @Test
  void cleanAfterHopFromRemove_returnsFalseWhenNotReferenced() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    assertFalse(meta.cleanAfterHopFromRemove(tm("NotReferenced")));
    assertEquals("Out1", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
    assertEquals("Default", meta.getDefaultTargetTransformName());
  }

  @Test
  void cleanAfterHopFromRemove_returnsFalseWithEmptyCaseTargets() {
    SwitchCaseMeta meta = new SwitchCaseMeta();
    assertFalse(meta.cleanAfterHopFromRemove(tm("Anything")));
  }

  @Test
  void cleanAfterHopFromRemove_clearsCaseTargetName() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    assertTrue(meta.cleanAfterHopFromRemove(tm("Out1")));
    assertNull(meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
    assertEquals("Default", meta.getDefaultTargetTransformName());
  }

  @Test
  void cleanAfterHopFromRemove_clearsDefaultTargetName() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    assertTrue(meta.cleanAfterHopFromRemove(tm("Default")));
    assertNull(meta.getDefaultTargetTransformName());
    assertEquals("Out1", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
  }

  @Test
  void cleanAfterHopFromRemove_clearsAllCaseTargetsWithSameName() {
    // Two case targets pointing at the same transform — both should be cleared
    SwitchCaseMeta meta = metaWithTargets("Shared", "Shared", "Default");
    assertTrue(meta.cleanAfterHopFromRemove(tm("Shared")));
    assertNull(meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertNull(meta.getCaseTargets().get(1).getCaseTargetTransformName());
    assertEquals("Default", meta.getDefaultTargetTransformName());
  }

  @Test
  void cleanAfterHopFromRemove_clearsCaseAndDefaultWhenSameName() {
    SwitchCaseMeta meta = metaWithTargets("Both", "Out2", "Both");
    assertTrue(meta.cleanAfterHopFromRemove(tm("Both")));
    assertNull(meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertNull(meta.getDefaultTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
  }

  @Test
  void cleanAfterHopFromRemove_resetsIoMetaCacheOnChange() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    ITransformIOMeta before = meta.getTransformIOMeta();
    meta.cleanAfterHopFromRemove(tm("Out1"));
    // The cached IO meta object must be replaced (resetTransformIoMeta was called)
    assertNotSame(before, meta.getTransformIOMeta());
  }

  @Test
  void cleanAfterHopFromRemove_doesNotResetIoMetaWhenNoChange() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    ITransformIOMeta before = meta.getTransformIOMeta();
    meta.cleanAfterHopFromRemove(tm("NotReferenced"));
    // No change → same cached object
    assertNotSame(null, before); // guard
    // The identity of the IO meta should be unchanged
    assertEquals(before, meta.getTransformIOMeta());
  }

  @Test
  void cleanAfterHopFromRemove_rebuiltIoMetaReflectsClearedName() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    assertEquals(3, meta.getTransformIOMeta().getTargetStreams().size());

    meta.cleanAfterHopFromRemove(tm("Out1"));

    // The case target entry survives (the SwitchCaseTarget itself is kept),
    // so the rebuilt IO meta still has 3 streams; first stream now has a null subject.
    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(3, streams.size());
    assertNull(streams.get(0).getSubject());
    assertEquals("Out2", streams.get(1).getSubject());
    assertEquals("Default", streams.get(2).getSubject());
  }

  // ---------------------------------------------------------------------------
  // searchInfoAndTargetTransforms
  // ---------------------------------------------------------------------------

  @Test
  void searchInfoAndTargetTransforms_resolvesCaseTargets() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", null);
    meta.setDefaultTargetTransformName(null);
    TransformMeta out1 = tm("Out1");
    TransformMeta out2 = tm("Out2");

    meta.searchInfoAndTargetTransforms(List.of(out1, out2));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(out1, streams.get(0).getTransformMeta());
    assertEquals(out2, streams.get(1).getTransformMeta());
  }

  @Test
  void searchInfoAndTargetTransforms_resolvesDefaultTarget() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Def");
    TransformMeta def = tm("Def");

    meta.searchInfoAndTargetTransforms(List.of(tm("Out1"), tm("Out2"), def));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(def, streams.get(2).getTransformMeta());
  }

  @Test
  void searchInfoAndTargetTransforms_yieldsNullStreamForUnknownName() {
    SwitchCaseMeta meta = metaWithTargets("Known", "Unknown", null);
    meta.setDefaultTargetTransformName(null);

    meta.searchInfoAndTargetTransforms(List.of(tm("Known")));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertNotNull(streams.get(0).getTransformMeta());
    assertNull(streams.get(1).getTransformMeta());
  }

  @Test
  void searchInfoAndTargetTransforms_handlesEmptyCaseTargets() {
    SwitchCaseMeta meta = new SwitchCaseMeta();
    meta.setDefaultTargetTransformName("Def");
    TransformMeta def = tm("Def");

    meta.searchInfoAndTargetTransforms(List.of(def));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(1, streams.size());
    assertEquals(def, streams.get(0).getTransformMeta());
  }

  @Test
  void searchInfoAndTargetTransforms_afterClean_remainingTargetsStillResolve() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Default");
    meta.cleanAfterHopFromRemove(tm("Out1"));

    // "Out1" was cleared; Out2 and Default are still valid
    meta.searchInfoAndTargetTransforms(List.of(tm("Out2"), tm("Default")));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertNull(streams.get(0).getTransformMeta(), "Cleared target should resolve to null");
    assertNotNull(streams.get(1).getTransformMeta());
    assertEquals("Out2", streams.get(1).getTransformMeta().getName());
    assertNotNull(streams.get(2).getTransformMeta());
    assertEquals("Default", streams.get(2).getTransformMeta().getName());
  }

  // ---------------------------------------------------------------------------
  // convertIOMetaToTransformNames
  // ---------------------------------------------------------------------------

  @Test
  void convertIOMetaToTransformNames_syncsCaseTargetNamesFromStreams() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", null);
    meta.setDefaultTargetTransformName(null);
    TransformMeta out1 = tm("Out1");
    TransformMeta out2 = tm("Out2");
    meta.searchInfoAndTargetTransforms(List.of(out1, out2));

    // Repoint stream 0 to a new transform (as handleStreamSelection would do during detach)
    TransformMeta newOut = tm("NewOut1");
    meta.getTransformIOMeta().getTargetStreams().get(0).setTransformMeta(newOut);

    meta.convertIOMetaToTransformNames();

    assertEquals("NewOut1", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
  }

  @Test
  void convertIOMetaToTransformNames_syncsDefaultTargetNameFromStream() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "Def");
    meta.searchInfoAndTargetTransforms(List.of(tm("Out1"), tm("Out2"), tm("Def")));

    TransformMeta newDef = tm("NewDef");
    meta.getTransformIOMeta().getTargetStreams().get(2).setTransformMeta(newDef);

    meta.convertIOMetaToTransformNames();

    assertEquals("NewDef", meta.getDefaultTargetTransformName());
  }

  @Test
  void convertIOMetaToTransformNames_doesNotUpdateAbsentDefault() {
    // When defaultTargetTransformName is empty the guard in convertIOMetaToTransformNames
    // must not overwrite it even if there happened to be a stream at that index.
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", null);
    meta.setDefaultTargetTransformName(null);
    meta.searchInfoAndTargetTransforms(List.of(tm("Out1"), tm("Out2")));

    meta.convertIOMetaToTransformNames();

    assertNull(meta.getDefaultTargetTransformName());
  }

  // ---------------------------------------------------------------------------
  // handleStreamSelection — repointing existing target streams
  // ---------------------------------------------------------------------------

  @Test
  void handleStreamSelection_updatesCaseTargetName() {
    SwitchCaseMeta meta = metaWithTargets("Old", "Out2", null);
    meta.setDefaultTargetTransformName(null);
    meta.searchInfoAndTargetTransforms(List.of(tm("Old"), tm("Out2")));

    TransformMeta newTarget = tm("New");
    IStream stream = meta.getTransformIOMeta().getTargetStreams().get(0);
    stream.setTransformMeta(newTarget);
    stream.setSubject(newTarget.getName());
    meta.handleStreamSelection(stream);

    assertEquals("New", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
  }

  @Test
  void handleStreamSelection_updatesDefaultTargetName() {
    SwitchCaseMeta meta = metaWithTargets("Out1", "Out2", "OldDefault");
    meta.searchInfoAndTargetTransforms(List.of(tm("Out1"), tm("Out2"), tm("OldDefault")));

    TransformMeta newDef = tm("NewDefault");
    IStream defStream = meta.getTransformIOMeta().getTargetStreams().get(2);
    defStream.setTransformMeta(newDef);
    defStream.setSubject(newDef.getName());
    meta.handleStreamSelection(defStream);

    assertEquals("NewDefault", meta.getDefaultTargetTransformName());
    assertEquals("Out1", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
  }

  // ---------------------------------------------------------------------------
  // handleStreamSelection — adding new streams via getOptionalStreams()
  // ---------------------------------------------------------------------------

  @Test
  void handleStreamSelection_newCaseTargetStreamAddsEntry() {
    SwitchCaseMeta meta = new SwitchCaseMeta(); // no targets yet

    // Prime the IO meta cache first — the canvas always renders existing streams before the user
    // can interact with the "add" placeholder, so the cache is already warm when
    // handleStreamSelection is called in practice.
    assertEquals(0, meta.getTransformIOMeta().getTargetStreams().size());

    // newCaseTargetStream is always the last element returned by getOptionalStreams()
    List<IStream> optional = meta.getOptionalStreams();
    IStream newCaseTargetStream = optional.get(optional.size() - 1);

    TransformMeta added = tm("CaseX");
    newCaseTargetStream.setTransformMeta(added);
    meta.handleStreamSelection(newCaseTargetStream);

    assertEquals(1, meta.getCaseTargets().size());
    assertEquals("CaseX", meta.getCaseTargets().get(0).getCaseTargetTransformName());
    // The IO meta should carry exactly the one new stream
    assertEquals(1, meta.getTransformIOMeta().getTargetStreams().size());
  }

  @Test
  void handleStreamSelection_newDefaultStreamSetsDefaultName() {
    SwitchCaseMeta meta = new SwitchCaseMeta(); // no default yet

    // Prime the IO meta cache (see note in newCaseTargetStream test above)
    meta.getTransformIOMeta();

    // newDefaultStream is only offered when defaultTargetTransformName is empty
    List<IStream> optional = meta.getOptionalStreams();
    IStream newDefaultStream = optional.get(0); // first when no default exists

    TransformMeta defTarget = tm("Default");
    newDefaultStream.setTransformMeta(defTarget);
    meta.handleStreamSelection(newDefaultStream);

    assertEquals("Default", meta.getDefaultTargetTransformName());
  }

  @Test
  void handleStreamSelection_newDefaultStreamIsNoLongerOfferedAfterSet() {
    SwitchCaseMeta meta = new SwitchCaseMeta();
    meta.getTransformIOMeta(); // prime cache

    List<IStream> optional = meta.getOptionalStreams();
    IStream newDefaultStream = optional.get(0);
    newDefaultStream.setTransformMeta(tm("Default"));
    meta.handleStreamSelection(newDefaultStream);

    // Once default is set, getOptionalStreams should only return newCaseTargetStream
    List<IStream> afterSet = meta.getOptionalStreams();
    assertEquals(1, afterSet.size());
  }

  // ---------------------------------------------------------------------------
  // End-to-end topology simulations
  // ---------------------------------------------------------------------------

  /**
   * Simulates inserting a new transform C between Switch Case and B:
   *
   * <pre>SwitchCase → B  becomes  SwitchCase → C → B</pre>
   *
   * The UI delegate calls handleStreamSelection on the upstream (Switch Case) with its target
   * stream repointed to C. The stored case-target name must be updated to "C".
   */
  @Test
  void simulation_insertTransformBetweenSwitchCaseAndTarget() {
    SwitchCaseMeta meta = metaWithTargets("B", "Out2", "Default");
    meta.searchInfoAndTargetTransforms(List.of(tm("B"), tm("Out2"), tm("Default")));

    // Insert C: detach target stream from B, attach to C
    TransformMeta c = tm("C");
    IStream streamToB = meta.getTransformIOMeta().getTargetStreams().get(0);
    streamToB.setTransformMeta(c);
    streamToB.setSubject(c.getName());
    meta.handleStreamSelection(streamToB);

    assertEquals(
        "C",
        meta.getCaseTargets().get(0).getCaseTargetTransformName(),
        "After insert, Switch Case case target must point to the inserted transform C");
    assertEquals("Out2", meta.getCaseTargets().get(1).getCaseTargetTransformName());
    assertEquals("Default", meta.getDefaultTargetTransformName());

    // Re-resolve with the new topology (C replaced B in this slot)
    meta.searchInfoAndTargetTransforms(List.of(c, tm("Out2"), tm("Default")));
    assertEquals(c, meta.getTransformIOMeta().getTargetStreams().get(0).getTransformMeta());
  }

  /**
   * Simulates detaching B from between Switch Case and C:
   *
   * <pre>SwitchCase → B → C  becomes  SwitchCase → C</pre>
   *
   * The UI delegate calls handleStreamSelection on Switch Case with the target stream repointed to
   * C. The stored case-target name must be updated from "B" to "C".
   */
  @Test
  void simulation_detachTransformBetweenSwitchCaseAndDownstream() {
    SwitchCaseMeta meta = metaWithTargets("B", "Out2", "Default");
    meta.searchInfoAndTargetTransforms(List.of(tm("B"), tm("Out2"), tm("Default")));

    // Detach B, reconnect Switch Case → C
    TransformMeta c = tm("C");
    IStream streamToB = meta.getTransformIOMeta().getTargetStreams().get(0);
    streamToB.setTransformMeta(c);
    streamToB.setSubject(c.getName());
    meta.handleStreamSelection(streamToB);

    assertEquals(
        "C",
        meta.getCaseTargets().get(0).getCaseTargetTransformName(),
        "After detach, Switch Case case target must be updated to the new downstream C");

    // searchInfoAndTargetTransforms with the post-detach topology must resolve correctly
    meta.searchInfoAndTargetTransforms(List.of(tm("B"), c, tm("Out2"), tm("Default")));
    assertEquals(
        c,
        meta.getTransformIOMeta().getTargetStreams().get(0).getTransformMeta(),
        "Re-resolve must find C, not the detached B");
  }

  /**
   * Full round-trip: build → resolve → repoint → re-resolve. Verifies that stored names and
   * resolved stream objects stay consistent across multiple cycles.
   */
  @Test
  void roundTrip_buildResolveRepointResolve() {
    SwitchCaseMeta meta = metaWithTargets("A", "B", "Default");
    TransformMeta tmA = tm("A");
    TransformMeta tmB = tm("B");
    TransformMeta tmDefault = tm("Default");
    meta.searchInfoAndTargetTransforms(List.of(tmA, tmB, tmDefault));

    // Verify initial resolution
    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(tmA, streams.get(0).getTransformMeta());
    assertEquals(tmB, streams.get(1).getTransformMeta());
    assertEquals(tmDefault, streams.get(2).getTransformMeta());

    // Repoint "A" → "C"
    TransformMeta tmC = tm("C");
    streams.get(0).setTransformMeta(tmC);
    streams.get(0).setSubject(tmC.getName());
    meta.handleStreamSelection(streams.get(0));
    assertEquals("C", meta.getCaseTargets().get(0).getCaseTargetTransformName());

    // Re-resolve — "C" now in the pipeline, "A" dropped
    meta.searchInfoAndTargetTransforms(List.of(tmC, tmB, tmDefault));
    List<IStream> resolved = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(tmC, resolved.get(0).getTransformMeta());
    assertEquals(tmB, resolved.get(1).getTransformMeta());
    assertEquals(tmDefault, resolved.get(2).getTransformMeta());
  }
}
