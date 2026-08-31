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

package org.apache.hop.git.util;

import java.util.List;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileBeforeCommitExtension;

/**
 * Asks anything listening on {@link HopExtensionPoint#HopGuiFileBeforeCommit} whether a commit may
 * go ahead.
 *
 * <p>With nothing listening — the usual case, since only optional plugins such as the linter do —
 * the call is a no-op and the commit proceeds, so git behaves exactly as it does without them.
 */
public class PreCommitCheck {

  private PreCommitCheck() {
    // Utility class
  }

  /**
   * Run the pre-commit listeners over the files about to be committed.
   *
   * <p>A listener that throws is logged and ignored: a broken listener should cost a warning in the
   * log, not the ability to commit. Only an explicit refusal stops the commit.
   *
   * @param log the log channel to report listener failures on
   * @param variables the variables to pass to the listeners
   * @param gitDirectory the git working tree the commit is made in
   * @param filenames the files in the commit, relative to gitDirectory
   * @return the extension carrying the verdict; check {@code isCancelled()}
   */
  public static HopGuiFileBeforeCommitExtension check(
      ILogChannel log, IVariables variables, String gitDirectory, List<String> filenames) {
    HopGuiFileBeforeCommitExtension extension =
        new HopGuiFileBeforeCommitExtension(gitDirectory, filenames);
    try {
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopGuiFileBeforeCommit.id, extension);
    } catch (Exception e) {
      log.logError("Error running the pre-commit checks, continuing with the commit", e);
    }
    return extension;
  }
}
