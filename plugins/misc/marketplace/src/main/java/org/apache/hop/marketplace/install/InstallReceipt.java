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

package org.apache.hop.marketplace.install;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InstallReceipt {
  private String groupId;
  private String artifactId;
  private String version;
  private String installedAt;

  /** Repository id that supplied the zip (if known). */
  private String repositoryId;

  /** Repository base URL used for the download. */
  private String repositoryUrl;

  private List<String> paths = new ArrayList<>();
  private boolean pendingActivation;

  /**
   * Files a previous version installed that this version does not have, and that are still on disk
   * because they have not been removed yet (pending activation, or the file was in use). Omitted
   * from the receipt when empty.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<String> obsoletePaths = new ArrayList<>();

  /**
   * SHA-256 (lower-case hex) of every file in the installed zip, keyed by the path in {@link
   * #paths}. For a {@code lib/core} jar this is the packaged content, which is not what is on disk
   * when {@link SharedFile#getAction()} is {@link SharedFileAction#KEPT}. Empty in receipts written
   * before checksums were recorded.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, String> checksums = new LinkedHashMap<>();

  /**
   * What activation did to each shared {@code lib/core} file, keyed by its {@code lib/core/...}
   * path. Also lists the shared files an earlier version created or replaced and this version no
   * longer ships ({@link SharedFileAction#NOT_SHIPPED}). Before activation it holds what earlier
   * installs recorded, which activation builds on. Empty in receipts written before this was
   * recorded.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, SharedFile> sharedFiles = new LinkedHashMap<>();

  /**
   * Set when an uninstall could not remove every file. The receipt then only lists what is left
   * ({@link #paths} holds the remaining folders, {@link #obsoletePaths} the remaining files), has
   * no version, and does not count as an install. The next install, apply or uninstall removes the
   * rest and deletes the receipt.
   */
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private boolean uninstallPending;

  /** Version this install replaced, or null for a first install. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String previousVersion;

  /** Set when {@code install} returned this receipt without downloading again. Not persisted. */
  @JsonIgnore private boolean alreadyPresent;

  public InstallReceipt() {
    // Jackson
  }

  /** What activating an install did to a shared {@code lib/core} file. */
  public enum SharedFileAction {
    /**
     * The file did not exist; this install created it. When it could not be written because it was
     * in use (Windows), Hop writes it the next time it starts (see {@link PendingChanges}).
     */
    CREATED,
    /**
     * A file with other content existed; it was backed up and replaced. On Windows, where the
     * running Hop keeps the jars on the system classpath locked, Hop replaces it the next time it
     * starts (see {@link PendingChanges}).
     */
    REPLACED,
    /** A file with the same content existed and was left alone. */
    UNCHANGED,
    /** A file with other content existed and was left in place: the copy or the backup failed. */
    KEPT,
    /**
     * An earlier version shipped this file and the marketplace created it, or replaced the client's
     * copy; this version no longer ships it. Shared files are not removed on update, so the entry
     * stays until the file is removed.
     */
    NOT_SHIPPED
  }

  @Getter
  @Setter
  public static class SharedFile {
    private SharedFileAction action;

    /**
     * Where this install backed up the file it replaced, relative to the Hop installation. Only set
     * for {@link SharedFileAction#REPLACED}. Every install backs up into a folder of its own, so a
     * backup is never overwritten.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String backup;

    /**
     * Where the copy the client shipped with was backed up, when a marketplace install replaced it
     * (this install or an earlier one, of this or another artifact). Relative to the Hop
     * installation.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String originalBackup;

    /**
     * Whether the marketplace created this file, in this install or an earlier one (of this or
     * another artifact). A file the client shipped with is never marked.
     */
    private boolean createdByMarketplace;

    public SharedFile() {
      // Jackson
    }

    public SharedFile(
        SharedFileAction action,
        String backup,
        boolean createdByMarketplace,
        String originalBackup) {
      this.action = action;
      this.backup = backup;
      this.createdByMarketplace = createdByMarketplace;
      this.originalBackup = originalBackup;
    }
  }
}
