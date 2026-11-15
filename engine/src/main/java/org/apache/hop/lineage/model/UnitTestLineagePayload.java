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

package org.apache.hop.lineage.model;

import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * The result of one golden data set of a Hop unit test: whether the rows that reached a transform
 * were the expected ones. Carried on a {@link LineageEventKind#UNIT_TEST} event of that transform.
 */
@Getter
public final class UnitTestLineagePayload implements LineagePayload {

  /** The name of the pipeline unit test. */
  private final String unitTestName;

  /** The golden data set the rows were compared with. */
  private final String dataSetName;

  /** Whether the rows were the expected ones. */
  private final boolean passed;

  /** What the comparison found, or null. */
  private final String comment;

  /**
   * The tables and files the tested transform declares it writes or reads, as {@link
   * RelationalIoLineagePayload} and {@link FileIoLineagePayload}: what the test vouches for. A test
   * often keeps the transform from writing, so these are what it would write, not what it did.
   */
  private final List<LineagePayload> testedDatasets;

  public UnitTestLineagePayload(
      String unitTestName,
      String dataSetName,
      boolean passed,
      String comment,
      List<LineagePayload> testedDatasets) {
    this.unitTestName = Objects.requireNonNull(unitTestName, "unitTestName");
    this.dataSetName = dataSetName;
    this.passed = passed;
    this.comment = comment;
    this.testedDatasets = testedDatasets == null ? List.of() : List.copyOf(testedDatasets);
  }
}
