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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.testing.PipelineUnitTest;

/**
 * Ensures at most one unit test per pipeline has the auto-open flag enabled. When a unit test is
 * saved with auto-open on, other tests that reference the same pipeline have the flag cleared.
 */
public final class UnitTestAutoOpening {

  private UnitTestAutoOpening() {
    // utility
  }

  /**
   * If {@code unitTest} has auto-open enabled, disable auto-open on every other unit test that
   * targets the same pipeline and persist those changes.
   *
   * @param log optional log channel
   * @param variables variables for path resolution
   * @param metadataProvider metadata provider (may be null)
   * @param unitTest the unit test that was just created or saved
   */
  public static void enforceExclusiveAutoOpening(
      ILogChannel log,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      PipelineUnitTest unitTest)
      throws HopException {

    if (unitTest == null || !unitTest.isAutoOpening() || Utils.isEmpty(unitTest.getName())) {
      return;
    }
    if (metadataProvider == null || variables == null) {
      return;
    }

    String pipelineFilename = unitTest.calculateCompletePipelineFilename(variables);
    if (StringUtils.isEmpty(pipelineFilename)) {
      return;
    }

    IHopMetadataSerializer<PipelineUnitTest> serializer =
        metadataProvider.getSerializer(PipelineUnitTest.class);
    List<PipelineUnitTest> allTests = serializer.loadAll();
    for (PipelineUnitTest other : allTests) {
      if (other == null || Utils.isEmpty(other.getName())) {
        continue;
      }
      if (unitTest.getName().equals(other.getName())) {
        continue;
      }
      if (!other.isAutoOpening()) {
        continue;
      }
      if (!other.matchesPipelineFilename(variables, pipelineFilename)) {
        continue;
      }

      other.setAutoOpening(false);
      serializer.save(other);
      if (log != null && log.isDetailed()) {
        log.logDetailed(
            "Disabled auto-open on unit test '"
                + other.getName()
                + "' because '"
                + unitTest.getName()
                + "' is now the auto-open test for the same pipeline");
      }
    }
  }
}
