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

package org.apache.hop.git.pipeline.transforms.gitinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class GitInputMetaTest {

  private GitInputMeta meta;

  @BeforeAll
  static void initEnv() throws Exception {
    if (!HopClientEnvironment.isInitialized()) {
      HopClientEnvironment.init();
    }
  }

  @BeforeEach
  void setUp() {
    meta = new GitInputMeta();
  }

  @Test
  void defaultsMatchTheDocumentedBehaviour() {
    assertEquals(GitInputSource.REMOTE.name(), meta.getSource());
    assertEquals("COMMITS", meta.getResourceType());
    assertEquals("all", meta.getState());
    assertEquals("50", meta.getPageSize());
    assertEquals("20", meta.getMaxPages());
    assertTrue(meta.isIncludeRawJson());
  }

  @Test
  void everyPropertySurvivesAnXmlRoundTrip() throws Exception {
    meta.setConnectionName("github");
    meta.setSource(GitInputSource.LOCAL.name());
    meta.setLocalRepositoryPath("/tmp/clone");
    meta.setResourceType("COMMIT_FILES");
    meta.setOwner("apache");
    meta.setRepository("hop");
    meta.setState("closed");
    meta.setSince("2026-01-01T00:00:00Z");
    meta.setBranch("main");
    meta.setPageSize("100");
    meta.setMaxPages("5");
    meta.setIncludeRawJson(false);

    String xml = XmlMetadataUtil.serializeObjectToXml(meta);
    assertNotNull(xml);
    Node node = XmlHandler.wrapLoadXmlString(xml);
    GitInputMeta loaded = XmlMetadataUtil.deSerializeFromXml(node, GitInputMeta.class, null);

    assertEquals("github", loaded.getConnectionName());
    assertEquals(GitInputSource.LOCAL.name(), loaded.getSource());
    assertEquals("/tmp/clone", loaded.getLocalRepositoryPath());
    assertEquals("COMMIT_FILES", loaded.getResourceType());
    assertEquals("apache", loaded.getOwner());
    assertEquals("hop", loaded.getRepository());
    assertEquals("closed", loaded.getState());
    assertEquals("2026-01-01T00:00:00Z", loaded.getSince());
    assertEquals("main", loaded.getBranch());
    assertEquals("100", loaded.getPageSize());
    assertEquals("5", loaded.getMaxPages());
    assertFalse(loaded.isIncludeRawJson());
  }

  @Test
  void timestampFieldsAreDatesNotStrings() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "git", null, null, new Variables(), null);

    assertEquals(19, rowMeta.size());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.searchValueMeta("created_at").getType());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.searchValueMeta("updated_at").getType());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.searchValueMeta("closed_at").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.searchValueMeta("number").getType());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.searchValueMeta("title").getType());
    assertEquals("git", rowMeta.searchValueMeta("title").getOrigin());
  }

  @Test
  void rawJsonFieldIsDroppedWhenNotRequested() throws Exception {
    meta.setIncludeRawJson(false);
    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "git", null, null, new Variables(), null);

    assertEquals(18, rowMeta.size());
    assertEquals(null, rowMeta.searchValueMeta("raw_json"));
  }

  @Test
  void checkReportsMissingRemoteSettings() {
    meta.setSource(GitInputSource.REMOTE.name());

    List<ICheckResult> remarks = check();

    assertTrue(hasError(remarks, "No Git connection is selected"));
    assertTrue(hasError(remarks, "repository owner and name are required"));
  }

  @Test
  void checkRejectsCommitFilesOnARemoteSource() {
    meta.setSource(GitInputSource.REMOTE.name());
    meta.setConnectionName("github");
    meta.setOwner("apache");
    meta.setRepository("hop");
    meta.setResourceType("COMMIT_FILES");

    assertTrue(hasError(check(), "COMMIT_FILES is only available for a local repository"));
  }

  @Test
  void checkRejectsIssuesOnALocalSource() {
    meta.setSource(GitInputSource.LOCAL.name());
    meta.setLocalRepositoryPath("/tmp/clone");
    meta.setResourceType("ISSUES");

    assertTrue(hasError(check(), "supports only COMMITS and COMMIT_FILES"));
  }

  @Test
  void checkRejectsANonNumericPageSize() {
    meta.setSource(GitInputSource.LOCAL.name());
    meta.setLocalRepositoryPath("/tmp/clone");
    meta.setPageSize("not-a-number");

    assertTrue(hasError(check(), "Page size"));
  }

  @Test
  void checkLeavesUnresolvedVariablesAlone() {
    meta.setSource(GitInputSource.LOCAL.name());
    meta.setLocalRepositoryPath("/tmp/clone");
    meta.setPageSize("${PAGE_SIZE}");
    meta.setMaxPages("${MAX_PAGES}");

    // A variable set only at run time must not be reported as an invalid number at design time.
    assertFalse(hasError(check(), "Page size"));
    assertFalse(hasError(check(), "Max pages"));
  }

  @Test
  void checkAcceptsZeroMaxPagesAndSaysThereIsNoRowCap() {
    meta.setSource(GitInputSource.REMOTE.name());
    meta.setConnectionName("github");
    meta.setOwner("apache");
    meta.setRepository("hop");
    meta.setMaxPages("0");

    List<ICheckResult> remarks = check();

    assertFalse(hasError(remarks, "Max pages"));
    assertTrue(
        remarks.stream()
            .anyMatch(r -> r.getText().contains("reads every page the provider will serve")),
        "expected the unlimited row-cap remark, got: " + remarks);
  }

  @Test
  void checkRejectsANegativeMaxPages() {
    meta.setSource(GitInputSource.REMOTE.name());
    meta.setConnectionName("github");
    meta.setOwner("apache");
    meta.setRepository("hop");
    meta.setMaxPages("-1");

    assertTrue(hasError(check(), "Max pages"));
  }

  /**
   * GuiCompositeWidgets reports one button press twice - once directly and once through asyncExec -
   * but invokes the annotated method once. The dialog tells the real press from the duplicate by
   * consuming this marker, which is what stops Browse opening its dialog twice.
   */
  @Test
  void aBrowseButtonRecordsWhichButtonWasPressedExactlyOnce() {
    assertNull(meta.getPendingBrowse());

    meta.browseRepository(meta);
    assertEquals(GitInputMeta.WIDGET_BROWSE_REPOSITORY, meta.getPendingBrowse());

    // The dialog consumes it; the duplicate notification then finds nothing to act on.
    meta.setPendingBrowse(null);
    assertNull(meta.getPendingBrowse());

    meta.browseBranch(meta);
    assertEquals(GitInputMeta.WIDGET_BROWSE_BRANCH, meta.getPendingBrowse());
  }

  /** The marker is transient UI state, not a setting: it must never reach the serialized form. */
  @Test
  void thePendingBrowseMarkerIsNotSerialized() throws Exception {
    meta.setSource(GitInputSource.REMOTE.name());
    meta.setPendingBrowse(GitInputMeta.WIDGET_BROWSE_REPOSITORY);

    String xml = meta.getXml();

    assertFalse(xml.contains("pendingBrowse"), xml);
    assertFalse(xml.contains(GitInputMeta.WIDGET_BROWSE_REPOSITORY), xml);
  }

  @Test
  void checkWarnsWhenTheTransformIsGivenIncomingRows() {
    meta.setSource(GitInputSource.LOCAL.name());
    meta.setLocalRepositoryPath("/tmp/clone");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        null,
        new TransformMeta("git", meta),
        null,
        new String[] {"upstream"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("ignores incoming rows")));
  }

  private List<ICheckResult> check() {
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        null,
        new TransformMeta("git", meta),
        null,
        new String[0],
        new String[0],
        null,
        new Variables(),
        null);
    return remarks;
  }

  private static boolean hasError(List<ICheckResult> remarks, String fragment) {
    return remarks.stream()
        .anyMatch(
            r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR && r.getText().contains(fragment));
  }
}
