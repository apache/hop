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

package org.apache.hop.testing.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.testing.PipelineUnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UnitTestAutoOpeningTest {

  @Mock IHopMetadataProvider metadataProvider;
  @Mock IHopMetadataSerializer<PipelineUnitTest> serializer;
  @Mock ILogChannel log;

  @Test
  void doesNothingWhenAutoOpeningIsFalse() throws Exception {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.setName("test A");
    unitTest.setAutoOpening(false);
    unitTest.setPipelineFilename("./pipeline.hpl");

    UnitTestAutoOpening.enforceExclusiveAutoOpening(
        log, new Variables(), metadataProvider, unitTest);

    verify(metadataProvider, never()).getSerializer(any());
  }

  @Test
  void disablesAutoOpeningOnOtherTestsForSamePipeline() throws Exception {
    Variables variables = new Variables();

    PipelineUnitTest current = new PipelineUnitTest();
    current.setName("test A");
    current.setAutoOpening(true);
    current.setPipelineFilename("./pipeline.hpl");
    current.setBasePath("/project");

    PipelineUnitTest otherSame = new PipelineUnitTest();
    otherSame.setName("test B");
    otherSame.setAutoOpening(true);
    otherSame.setPipelineFilename("./pipeline.hpl");
    otherSame.setBasePath("/project");

    PipelineUnitTest otherDifferent = new PipelineUnitTest();
    otherDifferent.setName("test C");
    otherDifferent.setAutoOpening(true);
    otherDifferent.setPipelineFilename("./other.hpl");
    otherDifferent.setBasePath("/project");

    when(metadataProvider.getSerializer(PipelineUnitTest.class)).thenReturn(serializer);
    when(serializer.loadAll()).thenReturn(List.of(current, otherSame, otherDifferent));

    UnitTestAutoOpening.enforceExclusiveAutoOpening(log, variables, metadataProvider, current);

    assertFalse(otherSame.isAutoOpening());
    assertTrue(otherDifferent.isAutoOpening());
    assertTrue(current.isAutoOpening());
    verify(serializer).save(otherSame);
    verify(serializer, never()).save(eq(otherDifferent));
    verify(serializer, never()).save(eq(current));
  }
}
