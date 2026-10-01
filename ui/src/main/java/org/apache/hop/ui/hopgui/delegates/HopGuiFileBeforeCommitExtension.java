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

package org.apache.hop.ui.hopgui.delegates;

import java.io.File;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.extension.HopExtensionPoint;

/**
 * Payload for {@link HopExtensionPoint#HopGuiFileBeforeCommit}: the files Hop GUI is about to
 * commit to git, and a way for a listener to refuse the commit.
 *
 * <p>A listener that wants to stop the commit calls {@link #cancel(String)} rather than throwing.
 * The reason it gives is shown to the user by the git plugin. A listener which throws instead is
 * logged and ignored, so that a failing listener costs a warning rather than the ability to commit.
 */
@Getter
public class HopGuiFileBeforeCommitExtension {

  /** Absolute path of the git working tree the commit is made in. */
  private final String gitDirectory;

  /**
   * The files in the commit, as paths relative to {@link #gitDirectory}.
   *
   * <p>A commit can delete a file, so not every path here exists on disk. Listeners which read the
   * files have to allow for that.
   */
  private final List<String> filenames;

  /** Set when a listener refused the commit. */
  private boolean cancelled;

  /** Why the commit was refused, shown to the user. Null while {@link #cancelled} is false. */
  private String cancelReason;

  public HopGuiFileBeforeCommitExtension(String gitDirectory, List<String> filenames) {
    this.gitDirectory = gitDirectory;
    this.filenames = filenames == null ? List.of() : Collections.unmodifiableList(filenames);
  }

  /**
   * Refuse the commit.
   *
   * <p>The first listener to refuse decides: a later one cannot overturn it, so the reason the user
   * sees is the first reason given.
   *
   * @param reason why the commit is refused, shown to the user
   */
  public void cancel(String reason) {
    if (!cancelled) {
      cancelled = true;
      cancelReason = reason;
    }
  }

  /** The files in the commit as absolute paths, for listeners that read them from disk. */
  public List<String> getAbsoluteFilenames() {
    return filenames.stream().map(name -> new File(gitDirectory, name).getPath()).toList();
  }
}
