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
 *
 */

package org.apache.hop.workflow.actions.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ActionFtpTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionFtp action =
        ActionSerializationTestUtil.testSerialization("/action-ftp-get.xml", ActionFtp.class);

    assertEquals("prod-ftp", action.getConnection());
    assertTrue(action.isUsingConnection());
    assertEquals("21", action.getServerPort());
    assertEquals("server", action.getServerName());
    assertEquals("username", action.getUserName());
    assertEquals("password", action.getPassword());
    assertEquals("remote-dir", action.getRemoteDirectory());
    assertEquals("target-folder", action.getTargetDirectory());
    assertEquals(".*", action.getWildcard());
    assertTrue(action.isBinaryMode());
    assertEquals(999, action.getTimeout());
    assertTrue(action.isRemove());
    assertTrue(action.isOnlyGettingNewFiles());
    assertTrue(action.isActiveConnection());
    assertEquals("ISO-8859-1", action.getControlEncoding());
    assertTrue(action.isMoveFiles());
    assertEquals("move-to-folder", action.getMoveToDirectory());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddTime());
    assertTrue(action.isSpecifyFormat());
    assertEquals("dt-format", action.getDateTimeFormat());
    assertTrue(action.isAddDateBeforeExtension());
    assertTrue(action.isAddResult());
    assertTrue(action.isCreateMoveFolder());

    assertEquals("proxy-host", action.getProxyHost());
    assertEquals("proxy-port", action.getProxyPort());
    assertEquals("proxy-username", action.getProxyUsername());
    assertEquals("proxy-password", action.getProxyPassword());

    assertEquals("socks-host", action.getSocksProxyHost());
    assertEquals("1080", action.getSocksProxyPort());
    assertEquals("socks-user", action.getSocksProxyUsername());
    assertEquals("socks-pass", action.getSocksProxyPassword());
    assertEquals(ActionFtp.IfFileExistsOperation.CREATE_UNIQUE, action.getIfFileExistsOperation());
    assertEquals("10", action.getNrLimit());
    assertEquals("success_when_at_least", action.getSuccessCondition());
  }

  @Test
  @DisplayName("A fresh action has no connection, so it uses its own settings")
  void aFreshActionHasNoConnection() {
    assertFalse(new ActionFtp().isUsingConnection());
    assertTrue(new ActionFtp().isEvaluation());
    assertFalse(new ActionFtp().isUnconditional());
  }

  @Test
  @DisplayName("The target file name is the file in the target directory")
  void targetFilenameIsTheFileInTheTargetDirectory() {
    ActionFtp action = new ActionFtp("get");
    action.setTargetDirectory("/tmp/landing");

    assertEquals(
        "/tmp/landing" + Const.FILE_SEPARATOR + "report.csv",
        action.returnTargetFilename("report.csv"));
    assertNull(action.returnTargetFilename(null));
  }

  @Test
  @DisplayName("A date, a time or both can be stamped onto the target file name")
  void targetFilenameCarriesTheDateAndTime() {
    ActionFtp action = new ActionFtp("get");
    action.setTargetDirectory("/tmp");

    action.setAddDate(true);
    String withDate = base(action.returnTargetFilename("report.csv"));
    assertTrue(
        withDate.matches("report\\.csv_\\d{8}"),
        "expected report.csv_yyyyMMdd but got " + withDate);

    action.setAddTime(true);
    String withBoth = base(action.returnTargetFilename("report.csv"));
    assertTrue(
        withBoth.matches("report\\.csv_\\d{8}_\\d{9}"),
        "expected report.csv_yyyyMMdd_HHmmssSSS but got " + withBoth);
  }

  @Test
  @DisplayName("The stamp can go before the extension, so the file keeps its type")
  void theStampCanGoBeforeTheExtension() {
    ActionFtp action = new ActionFtp("get");
    action.setTargetDirectory("/tmp");
    action.setAddDate(true);
    action.setAddDateBeforeExtension(true);

    String name = base(action.returnTargetFilename("report.csv"));

    assertTrue(name.matches("report_\\d{8}\\.csv"), "expected report_yyyyMMdd.csv but got " + name);
  }

  @Test
  @DisplayName("A date format of your own replaces the built-in one")
  void aFormatOfYourOwnIsUsed() {
    ActionFtp action = new ActionFtp("get");
    action.setTargetDirectory("/tmp");
    action.setSpecifyFormat(true);
    action.setDateTimeFormat("'-fixed'");

    assertEquals("report.csv-fixed", base(action.returnTargetFilename("report.csv")));
  }

  @Test
  @DisplayName("A file without an extension is stamped just the same")
  void aFileWithoutAnExtension() {
    ActionFtp action = new ActionFtp("get");
    action.setTargetDirectory("/tmp");
    action.setAddDate(true);
    action.setAddDateBeforeExtension(true);

    assertTrue(base(action.returnTargetFilename("README")).matches("README_\\d{8}"));
  }

  @Test
  @DisplayName("Paths are normalised to forward slashes without a trailing one")
  void normalizePath() {
    ActionFtp action = new ActionFtp("get");

    assertEquals("/a/b/c", action.normalizePath("\\a\\b\\c"));
    assertEquals("/a/b/c", action.normalizePath("/a/b/c/"));
    assertEquals("/a/b/c", action.normalizePath("/a/b/c\\"));
    assertEquals("", action.normalizePath("/"));
  }

  @Test
  @DisplayName("An action without a server, user or target directory is reported by check()")
  void checkReportsTheMissingSettings() {
    List<ICheckResult> remarks = new ArrayList<>();

    new ActionFtp("get")
        .check(remarks, mock(WorkflowMeta.class), new Variables(), new MemoryMetadataProvider());

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "an empty action should be reported as wrong");
  }

  @Test
  @DisplayName("check() leaves the server settings alone when a named connection brings them")
  void checkSkipsTheServerSettingsOfAConnection() {
    ActionFtp action = new ActionFtp("get");
    action.setConnection("prod");
    action.setTargetDirectory(System.getProperty("java.io.tmpdir"));
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, mock(WorkflowMeta.class), new Variables(), new MemoryMetadataProvider());

    assertTrue(
        remarks.stream().noneMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "the connection carries the server, so nothing should be reported: " + remarks);
  }

  @Test
  @DisplayName("The server of the action shows up as a resource it depends on")
  void theServerIsAResourceDependency() {
    ActionFtp action = new ActionFtp("get");
    action.setServerName("ftp.example.com");

    List<ResourceReference> references =
        action.getResourceDependencies(new Variables(), mock(WorkflowMeta.class));

    assertEquals(1, references.size());
    assertEquals("ftp.example.com", references.get(0).getEntries().get(0).getResource());
    assertTrue(
        new ActionFtp("get")
            .getResourceDependencies(new Variables(), mock(WorkflowMeta.class))
            .isEmpty());
  }

  @Test
  @DisplayName("The if-file-exists choices round-trip through their descriptions")
  void ifFileExistsOperationRoundTrips() {
    for (ActionFtp.IfFileExistsOperation operation : ActionFtp.IfFileExistsOperation.values()) {
      assertEquals(
          operation, ActionFtp.IfFileExistsOperation.lookupDescription(operation.getDescription()));
    }
    assertEquals(
        ActionFtp.IfFileExistsOperation.values().length,
        ActionFtp.IfFileExistsOperation.getDescriptions().length);
    assertEquals(
        ActionFtp.IfFileExistsOperation.SKIP,
        ActionFtp.IfFileExistsOperation.lookupDescription("something else"));
  }

  /** The file name without the directory in front of it. */
  private static String base(String path) {
    int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
    return slash < 0 ? path : path.substring(slash + 1);
  }
}
