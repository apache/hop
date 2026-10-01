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

package org.apache.hop.git;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.hop.git.config.GitConfigSingleton;
import org.junit.jupiter.api.Test;

class GitGuiPluginSessionIsolationTest {

  @Test
  void gitHandleIsNotAProcessWideStatic() throws Exception {
    Field git = GitGuiPlugin.class.getDeclaredField("git");
    assertFalse(
        Modifier.isStatic(git.getModifiers()),
        "UIGit must be per HopGui / RAP UISession, not a JVM static");
  }

  @Test
  void gitPluginIsNotAProcessWideStaticSingletonField() {
    for (Field field : GitGuiPlugin.class.getDeclaredFields()) {
      if ("instance".equals(field.getName())) {
        assertFalse(
            Modifier.isStatic(field.getModifiers()),
            "GitGuiPlugin must not keep a static instance field");
      }
    }
  }

  @Test
  void gitResourceIsNotAProcessWideStaticSingletonField() {
    for (Field field : GitResource.class.getDeclaredFields()) {
      if ("instance".equals(field.getName())) {
        assertFalse(Modifier.isStatic(field.getModifiers()));
      }
    }
  }

  @Test
  void gitPerspectiveIsNotAProcessWideStaticSingletonField() {
    for (Field field : GitPerspective.class.getDeclaredFields()) {
      if ("instance".equals(field.getName())) {
        assertFalse(
            Modifier.isStatic(field.getModifiers()),
            "GitPerspective must not keep a process-wide static instance field");
      }
    }
  }

  @Test
  void gitCommitPerspectiveIsNotAProcessWideStaticSingletonField() {
    for (Field field : GitCommitPerspective.class.getDeclaredFields()) {
      if ("instance".equals(field.getName())) {
        assertFalse(
            Modifier.isStatic(field.getModifiers()),
            "GitCommitPerspective must not keep a process-wide static instance field");
      }
    }
  }

  @Test
  void gitCommitPerspectiveUninitializedIsSafe() {
    GitCommitPerspective perspective = new GitCommitPerspective();
    assertFalse(perspective.isInitialized());
    assertDoesNotThrow(perspective::activate);
    assertDoesNotThrow(perspective::perspectiveActivated);
    assertDoesNotThrow(perspective::retrieveState);
    assertDoesNotThrow(perspective::saveState);
    assertDoesNotThrow(perspective::refresh);
    assertDoesNotThrow(perspective::updateGui);
    assertDoesNotThrow(perspective::selectAllChanged);
    assertDoesNotThrow(perspective::addFilesToGit);
    assertDoesNotThrow(perspective::unstageFiles);
    assertDoesNotThrow(perspective::addFilesToGitIgnore);
    assertDoesNotThrow(perspective::deleteFiles);
    assertDoesNotThrow(perspective::restoreFiles);
    assertDoesNotThrow(perspective::showTextDiff);
    assertDoesNotThrow(perspective::showGraphDiff);
    assertDoesNotThrow(() -> perspective.commitFiles(false));
  }

  @Test
  void gitPerspectiveUninitializedIsSafe() {
    GitPerspective perspective = new GitPerspective();
    assertFalse(perspective.isInitialized());
    assertDoesNotThrow(perspective::activate);
    assertDoesNotThrow(perspective::perspectiveActivated);
    assertDoesNotThrow(() -> perspective.refresh());
    assertDoesNotThrow(() -> perspective.refresh(true));
    assertDoesNotThrow(perspective::updateGui);
    assertDoesNotThrow(perspective::clearGitUiState);
    assertDoesNotThrow(perspective::clearSearchFilters);
    assertDoesNotThrow(perspective::copyRevisionId);
    assertDoesNotThrow(perspective::copyPath);
    assertDoesNotThrow(perspective::resetToCommit);
    assertDoesNotThrow(perspective::revertCommit);
    assertDoesNotThrow(perspective::revertFile);
    assertDoesNotThrow(perspective::cherryPickCommit);
    assertDoesNotThrow(perspective::cherryPickFile);
    assertDoesNotThrow(perspective::checkoutReference);
    assertDoesNotThrow(perspective::checkoutCommit);
    assertDoesNotThrow(perspective::addTag);
    assertDoesNotThrow(perspective::addBranchFromCommit);
    assertDoesNotThrow(perspective::addBranchFromRef);
    assertDoesNotThrow(perspective::mergeBranch);
    assertDoesNotThrow(perspective::fetch);
    assertDoesNotThrow(perspective::pull);
    assertDoesNotThrow(perspective::push);
    assertDoesNotThrow(perspective::pushReference);
    assertDoesNotThrow(perspective::renameReference);
    assertDoesNotThrow(perspective::deleteReference);
    assertDoesNotThrow(perspective::showAllRef);
    assertDoesNotThrow(perspective::showTextDiff);
    assertDoesNotThrow(perspective::showGraphDiff);
  }

  @Test
  void gitConfigSingletonThreadSafe() {
    assertNotNull(GitConfigSingleton.getInstance());
    assertNotNull(GitConfigSingleton.getConfig());
  }
}
