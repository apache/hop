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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.pipeline.transform.transforms.FakeMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Copies are refused when a transform is the named target of a previous transform. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class PipelineMetaMultipleCopiesTest {

  private PipelineMeta pipelineMeta;
  private IVariables variables;

  @BeforeEach
  void setUp() throws Exception {
    pipelineMeta = new PipelineMeta();
    variables = new Variables();
    PluginRegistry.getInstance()
        .registerPluginClass(FakeMeta.class.getName(), TransformPluginType.class, Transform.class);
  }

  @Test
  void multipleCopiesAreDisallowedWhenAPreviousTransformTargetsIt() {
    TransformMeta filter = transform("Filter");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    nameTargets(filter, selectValues);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(filter, selectValues));

    assertFalse(pipelineMeta.allowsMultipleCopies(selectValues));
    assertTrue(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, selectValues), variables));
  }

  @Test
  void multipleCopiesStayAllowedWhenThePreviousTransformDoesNotTargetIt() {
    TransformMeta filter = transform("Filter");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(filter, selectValues));

    assertTrue(pipelineMeta.allowsMultipleCopies(selectValues));
    assertFalse(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, selectValues), variables));
  }

  @Test
  void multipleCopiesStayAllowedWithoutAHopEvenIfTheTargetNameIsSet() {
    TransformMeta filter = transform("Filter");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    nameTargets(filter, selectValues);

    assertTrue(pipelineMeta.allowsMultipleCopies(selectValues));
    assertTrue(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, selectValues), variables));
  }

  @Test
  void secondTargetStreamAlsoDisallowsMultipleCopies() {
    TransformMeta filter = transform("Filter");
    TransformMeta trueTarget = transform("True path");
    TransformMeta falseTarget = transform("False path");
    falseTarget.setCopies(3);
    nameTargets(filter, trueTarget, falseTarget);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(filter, falseTarget));

    assertTrue(pipelineMeta.allowsMultipleCopies(trueTarget));
    assertFalse(pipelineMeta.allowsMultipleCopies(falseTarget));
    assertFalse(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, trueTarget), variables));
    assertTrue(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, falseTarget), variables));
  }

  @Test
  void targetNameMatchIsCaseInsensitive() {
    TransformMeta filter = transform("Filter");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    nameTargets(filter, selectValues);

    TransformMeta sameName = transform("SELECT VALUES");
    sameName.setCopies(2);

    assertTrue(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, sameName), variables));
  }

  @Test
  void singleCopyTargetHopIsAllowed() {
    TransformMeta filter = transform("Filter");
    TransformMeta selectValues = transform("Select values");
    nameTargets(filter, selectValues);

    assertFalse(pipelineMeta.hasMultipleCopies(selectValues, variables));
    assertFalse(pipelineMeta.isMultipleCopiesTargetHop(hop(filter, selectValues), variables));
  }

  @Test
  void disabledTargetHopIsNotRefused() {
    TransformMeta filter = transform("Filter");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    nameTargets(filter, selectValues);
    PipelineHopMeta hop = hop(filter, selectValues);
    hop.setEnabled(false);
    pipelineMeta.addPipelineHop(hop);

    assertFalse(pipelineMeta.isMultipleCopiesTargetHop(hop, variables));
    assertTrue(pipelineMeta.allowsMultipleCopies(selectValues));
  }

  @Test
  void splittingATargetHopOntoMultipleCopiesIsDisallowed() {
    TransformMeta filter = transform("Filter");
    TransformMeta dummy = transform("Dummy");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    nameTargets(filter, dummy);
    PipelineHopMeta hop = hop(filter, dummy);

    assertTrue(pipelineMeta.isMultipleCopiesTargetSplit(hop, selectValues, variables));
    selectValues.setCopies(1);
    assertFalse(pipelineMeta.isMultipleCopiesTargetSplit(hop, selectValues, variables));
  }

  @Test
  void splittingAPlainHopOntoMultipleCopiesIsAllowed() {
    TransformMeta upstream = transform("Data grid");
    TransformMeta downstream = transform("Dummy");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);

    assertFalse(
        pipelineMeta.isMultipleCopiesTargetSplit(
            hop(upstream, downstream), selectValues, variables));
  }

  @Test
  void splittingStillRefusesWhenTheHopIsDisabled() {
    TransformMeta filter = transform("Filter");
    TransformMeta dummy = transform("Dummy");
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopies(2);
    nameTargets(filter, dummy);
    PipelineHopMeta hop = hop(filter, dummy);
    hop.setEnabled(false);

    assertTrue(pipelineMeta.isMultipleCopiesTargetSplit(hop, selectValues, variables));
  }

  @Test
  void copiesStringResolvesVariablesAndIgnoresUnresolvedOnes() {
    TransformMeta selectValues = transform("Select values");
    selectValues.setCopiesString("${COPIES}");

    assertFalse(pipelineMeta.hasMultipleCopies(selectValues, variables));

    variables.setVariable("COPIES", "4");
    assertTrue(pipelineMeta.hasMultipleCopies(selectValues, variables));

    variables.setVariable("COPIES", "1");
    assertFalse(pipelineMeta.hasMultipleCopies(selectValues, variables));
  }

  @Test
  void nullArgumentsAreSafe() {
    assertTrue(pipelineMeta.allowsMultipleCopies(null));
    assertFalse(pipelineMeta.hasMultipleCopies(null, null));
    assertFalse(pipelineMeta.isMultipleCopiesTargetHop(null, variables));
    assertFalse(pipelineMeta.isMultipleCopiesTargetSplit(null, null, null));
    PipelineHopMeta emptyHop = new PipelineHopMeta((TransformMeta) null, (TransformMeta) null);
    emptyHop.setEnabled(true);
    assertFalse(pipelineMeta.isMultipleCopiesTargetHop(emptyHop, variables));
    assertFalse(pipelineMeta.isMultipleCopiesTargetSplit(emptyHop, transform("Copies"), null));
  }

  private TransformMeta transform(String name) {
    return new TransformMeta(name, new FakeMeta());
  }

  private static PipelineHopMeta hop(TransformMeta from, TransformMeta to) {
    return new PipelineHopMeta(from, to);
  }

  private static void nameTargets(TransformMeta source, TransformMeta... targets) {
    TransformIOMeta ioMeta = new TransformIOMeta(true, true, false, false, false, false);
    for (TransformMeta target : targets) {
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET, target, "Result is true", StreamIcon.TRUE, target.getName()));
    }
    ((FakeMeta) source.getTransform()).setTransformIOMeta(ioMeta);
  }
}
