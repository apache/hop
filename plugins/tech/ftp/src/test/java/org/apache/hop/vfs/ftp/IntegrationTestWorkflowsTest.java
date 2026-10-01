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

package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.ftp.ActionFtp;
import org.apache.hop.workflow.actions.ftpdelete.ActionFtpDelete;
import org.apache.hop.workflow.actions.ftpput.ActionFtpPut;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The integration-test workflows are hand written XML, and a mistake in one only shows up as a
 * failing docker run. Hop itself reads them here, so a workflow which it cannot load - or which
 * loses an action or a hop on the way in - fails in seconds instead.
 *
 * <p>Only the FTP actions are on the class path of this module, so the other action types load as
 * "missing". That is fine: what has to hold is that the FTP actions Hop reads back are the ones
 * that were written, and that every hop still connects two actions which are really there.
 */
class IntegrationTestWorkflowsTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  @DisplayName("Hop can read every integration-test workflow")
  void everyWorkflowLoads() throws Exception {
    List<Path> workflows = workflowFiles();
    assertFalse(workflows.isEmpty(), "no integration-test workflows found");

    for (Path file : workflows) {
      WorkflowMeta workflowMeta = load(file);
      assertNotNull(workflowMeta.getName(), file + " has no name");
      assertTrue(workflowMeta.nrActions() > 0, file + " has no actions");
    }
  }

  @Test
  @DisplayName("Every FTP action in them is read back as the action it says it is")
  void everyFtpActionLoads() throws Exception {
    int ftpActions = 0;
    for (Path file : workflowFiles()) {
      WorkflowMeta workflowMeta = load(file);
      for (int i = 0; i < workflowMeta.nrActions(); i++) {
        ActionMeta actionMeta = workflowMeta.getAction(i);
        IAction action = actionMeta.getAction();
        String where = file.getFileName() + " / " + actionMeta.getName();

        switch (pluginIdOf(actionMeta)) {
          case "FTP" -> {
            assertTrue(action instanceof ActionFtp, where + " did not load as an FTP get action");
            ftpActions++;
          }
          case "FTP_PUT" -> {
            assertTrue(
                action instanceof ActionFtpPut, where + " did not load as an FTP put action");
            ftpActions++;
          }
          case "FTP_DELETE" -> {
            assertTrue(
                action instanceof ActionFtpDelete, where + " did not load as an FTP delete action");
            ftpActions++;
          }
          default -> {
            // Another plugin, not on the class path of this module.
          }
        }
      }
    }
    assertTrue(
        ftpActions >= 4, "expected the suite to exercise the FTP actions, found " + ftpActions);
  }

  @Test
  @DisplayName("Every hop still connects two actions which are really in the workflow")
  void everyHopConnectsRealActions() throws Exception {
    for (Path file : workflowFiles()) {
      WorkflowMeta workflowMeta = load(file);
      assertTrue(
          workflowMeta.nrWorkflowHops() > 0,
          file.getFileName() + " has no hops between its actions");

      for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
        WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
        String where = file.getFileName() + " hop " + i;

        assertNotNull(hop.getFromAction(), where + " starts nowhere");
        assertNotNull(hop.getToAction(), where + " goes nowhere");
        assertNotNull(
            workflowMeta.findAction(hop.getFromAction().getName()),
            where + " starts at an action which is not in the workflow");
        assertNotNull(
            workflowMeta.findAction(hop.getToAction().getName()),
            where + " ends at an action which is not in the workflow");
      }
    }
  }

  /**
   * Every action has to be reachable from the start, or it silently never runs - which reads as a
   * passing test.
   */
  @Test
  @DisplayName("No action is left unreachable by a lost hop")
  void everyActionIsReachable() throws Exception {
    for (Path file : workflowFiles()) {
      WorkflowMeta workflowMeta = load(file);

      for (int i = 0; i < workflowMeta.nrActions(); i++) {
        ActionMeta actionMeta = workflowMeta.getAction(i);
        if (actionMeta.isStart()) {
          continue;
        }
        assertTrue(
            workflowMeta.findNrPrevActions(actionMeta) > 0,
            file.getFileName()
                + ": nothing leads to \""
                + actionMeta.getName()
                + "\", so it never runs");
      }
    }
  }

  @Test
  @DisplayName("A remote directory of an action is written the way the VFS scheme reads it")
  void remoteDirectoriesAreHomeRelative() throws Exception {
    for (Path file : workflowFiles()) {
      WorkflowMeta workflowMeta = load(file);
      for (int i = 0; i < workflowMeta.nrActions(); i++) {
        IAction action = workflowMeta.getAction(i).getAction();
        String remoteDirectory = null;
        if (action instanceof ActionFtp ftp) {
          remoteDirectory = ftp.getRemoteDirectory();
        } else if (action instanceof ActionFtpPut put) {
          remoteDirectory = put.getRemoteDirectory();
        } else if (action instanceof ActionFtpDelete delete) {
          remoteDirectory = delete.getRemoteDirectory();
        }
        // The connections of the suite have "paths are relative to the home directory", which the
        // VFS scheme honours and the server does not: a leading slash means the server root there
        // and the login directory here, and the two only coincide on a server which chroots.
        assertFalse(
            remoteDirectory != null && remoteDirectory.startsWith("/"),
            file.getFileName()
                + " / "
                + workflowMeta.getAction(i).getName()
                + ": the remote directory \""
                + remoteDirectory
                + "\" starts with a slash, so it means the root of the server rather than the"
                + " directory the VFS scheme points at");
      }
    }
  }

  // --- helpers ------------------------------------------------------------------------------

  private static String pluginIdOf(ActionMeta actionMeta) {
    return String.valueOf(actionMeta.getAction().getPluginId());
  }

  private static WorkflowMeta load(Path file) throws Exception {
    return new WorkflowMeta(new Variables(), file.toString(), new MemoryMetadataProvider());
  }

  private static List<Path> workflowFiles() throws Exception {
    Path project = integrationTestProject();
    try (Stream<Path> files = Files.list(project)) {
      List<Path> workflows = new ArrayList<>();
      files
          .filter(f -> f.getFileName().toString().matches("main-.*\\.hwf"))
          .sorted()
          .forEach(workflows::add);
      return workflows;
    }
  }

  /** The integration-test project of this plugin, found by walking up to the repository root. */
  private static Path integrationTestProject() {
    Path directory = Path.of("").toAbsolutePath();
    while (directory != null) {
      Path candidate = directory.resolve("integration-tests/ftp");
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
      directory = directory.getParent();
    }
    throw new IllegalStateException(
        "integration-tests/ftp not found above " + Path.of("").toAbsolutePath());
  }
}
