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

package org.apache.hop.ui.hopgui.file.delegates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.markdown.NoteLinkHit;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopGuiNoteLinkSupportTest {

  @TempDir Path tempDir;

  @Test
  void isUrlTargetRecognizesHttpSchemes() {
    assertTrue(HopGuiNoteLinkSupport.isUrlTarget("https://hop.apache.org"));
    assertTrue(HopGuiNoteLinkSupport.isUrlTarget("HTTP://example.com"));
    assertFalse(HopGuiNoteLinkSupport.isUrlTarget("other.hpl"));
    assertFalse(HopGuiNoteLinkSupport.isUrlTarget(null));
    assertFalse(HopGuiNoteLinkSupport.isUrlTarget(""));
  }

  @Test
  void resolveTargetSubstitutesVariables() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("HOST", "hop.apache.org");
    String resolved = HopGuiNoteLinkSupport.resolveTarget(variables, null, "https://${HOST}/docs");
    assertEquals("https://hop.apache.org/docs", resolved);
  }

  @Test
  void resolveTargetRelativeToBaseFile(@TempDir Path dir) throws Exception {
    Path base = dir.resolve("main.hpl");
    Files.writeString(base, "<pipeline/>");
    Path sibling = dir.resolve("other.hpl");
    Files.writeString(sibling, "<pipeline/>");

    String resolved =
        HopGuiNoteLinkSupport.resolveTarget(
            new Variables(), base.toAbsolutePath().toString(), "other.hpl");
    assertNotNull(resolved);
    assertTrue(
        resolved.contains("other.hpl") || resolved.endsWith("other.hpl"),
        "resolved path should point at sibling: " + resolved);
  }

  @Test
  void linkHitFromAreaOwner() {
    NotePadMeta note = new NotePadMeta();
    NoteLinkHit hit = new NoteLinkHit(note, "docs", "https://hop.apache.org");
    AreaOwner area = new AreaOwner(AreaType.NOTE_LINK, 0, 0, 10, 10, new DPoint(0, 0), note, hit);
    assertEquals(hit, HopGuiNoteLinkSupport.linkHitFrom(area));
    assertNull(
        HopGuiNoteLinkSupport.linkHitFrom(
            new AreaOwner(AreaType.NOTE, 0, 0, 10, 10, new DPoint(0, 0), null, note)));
  }

  @Test
  void noteLinksEqualComparesIdentityAndTarget() {
    NotePadMeta note = new NotePadMeta();
    NoteLinkHit a = new NoteLinkHit(note, "a", "https://a");
    NoteLinkHit b = new NoteLinkHit(note, "a", "https://a");
    NoteLinkHit c = new NoteLinkHit(note, "a", "https://b");
    assertTrue(HopGuiNoteLinkSupport.noteLinksEqual(a, b));
    assertFalse(HopGuiNoteLinkSupport.noteLinksEqual(a, c));
    assertTrue(HopGuiNoteLinkSupport.noteLinksEqual(null, null));
    assertFalse(HopGuiNoteLinkSupport.noteLinksEqual(a, null));
  }

  @Test
  void tooltipForReturnsTarget() {
    NoteLinkHit hit = new NoteLinkHit(new NotePadMeta(), "label", " other.hpl ");
    assertEquals("other.hpl", HopGuiNoteLinkSupport.tooltipFor(hit));
    assertNull(HopGuiNoteLinkSupport.tooltipFor(null));
  }
}
