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
package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.metadata.validation.ReferencedDatabaseConnectionChecker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Which remarks are worth interrupting a save for. See issue #8295: a connection the checker could
 * not look up is reported, but it describes unreachable metadata rather than a problem with the
 * file being saved, so it must not put a dialog in front of every save.
 */
class ReferencedConnectionSaveValidatorTest {

  private static ICheckResult remark(int type, String code) {
    return new CheckResult(type, code, code, null);
  }

  @Test
  @DisplayName("A connection that does not exist still stops the save")
  void missingConnectionBlocks() {
    List<ICheckResult> blocking =
        ReferencedConnectionSaveValidator.blockingRemarks(
            List.of(
                remark(
                    ICheckResult.TYPE_RESULT_WARNING,
                    ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST)));

    assertEquals(1, blocking.size());
  }

  @Test
  @DisplayName("A connection that could not be checked does not stop the save")
  void unverifiedConnectionDoesNotBlock() {
    List<ICheckResult> blocking =
        ReferencedConnectionSaveValidator.blockingRemarks(
            List.of(
                remark(
                    ICheckResult.TYPE_RESULT_COMMENT,
                    ReferencedDatabaseConnectionChecker.INFO_NOT_VERIFIED)));

    assertTrue(
        blocking.isEmpty(),
        "Unreadable metadata would otherwise put this dialog in front of every save");
  }

  @Test
  @DisplayName("A real problem still gets through when mixed with ones that could not be checked")
  void mixedRemarksKeepTheRealProblem() {
    List<ICheckResult> blocking =
        ReferencedConnectionSaveValidator.blockingRemarks(
            List.of(
                remark(
                    ICheckResult.TYPE_RESULT_COMMENT,
                    ReferencedDatabaseConnectionChecker.INFO_NOT_VERIFIED),
                remark(
                    ICheckResult.TYPE_RESULT_WARNING,
                    ReferencedDatabaseConnectionChecker.ERROR_NOT_ASSIGNED),
                remark(
                    ICheckResult.TYPE_RESULT_COMMENT,
                    ReferencedDatabaseConnectionChecker.INFO_NOT_VERIFIED)));

    assertEquals(1, blocking.size());
    assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_NOT_ASSIGNED, blocking.get(0).getErrorCode());
  }

  @Test
  @DisplayName("Nothing to report is not a reason to prompt")
  void emptyAndNullAreNotBlocking() {
    assertTrue(ReferencedConnectionSaveValidator.blockingRemarks(null).isEmpty());
    assertTrue(ReferencedConnectionSaveValidator.blockingRemarks(List.of()).isEmpty());
  }
}
