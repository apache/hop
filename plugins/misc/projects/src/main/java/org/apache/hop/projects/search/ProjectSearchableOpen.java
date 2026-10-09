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

package org.apache.hop.projects.search;

import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/**
 * Decides whether a search hit may be opened in the project that is active now. A hit from another
 * project is never opened there: the caller switches first, or does not open it.
 */
final class ProjectSearchableOpen {

  private ProjectSearchableOpen() {}

  /**
   * UI actions the open path needs. Tests supply a fake; the GUI supplies the dialog and switch.
   */
  interface Actions {
    String activeProjectName();

    boolean confirmSwitch(String projectName);

    /**
     * @return true only when the named project is now the enabled project
     */
    boolean switchToProject(String projectName);
  }

  static boolean isOtherProject(String hitProject, String activeProject) {
    if (StringUtils.isEmpty(hitProject)) {
      return false;
    }
    return !hitProject.equalsIgnoreCase(StringUtils.defaultString(activeProject));
  }

  /**
   * Opens a hit in its own project.
   *
   * @return true when the hit was opened
   */
  static boolean prepare(
      String hitProject,
      String activeProject,
      BooleanSupplier confirm,
      Predicate<String> switchProject,
      OpenAction open)
      throws HopException {
    if (!isOtherProject(hitProject, activeProject)) {
      open.run();
      return true;
    }
    if (!confirm.getAsBoolean()) {
      return false;
    }
    // Switch before open. A failed switch must not open the hit in the active project.
    if (!switchProject.test(hitProject)) {
      return false;
    }
    open.run();
    return true;
  }

  @FunctionalInterface
  interface OpenAction {
    void run() throws HopException;
  }
}
