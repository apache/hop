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

import org.apache.hop.marketplace.resolve.ITransferListener;

/**
 * Progress of a plugin install: which {@link Phase} it is in, plus the byte-level download progress
 * inherited from {@link ITransferListener}.
 *
 * <p>An install is more than a download — resolving a SNAPSHOT costs a round trip, and unzipping a
 * few hundred jars costs real time — so a progress bar driven by bytes alone would sit at 100%
 * while the install looks hung. Phases let a listener spread the bar over the whole operation.
 *
 * <p>Like its parent, callbacks arrive off the UI thread.
 */
public interface IInstallListener extends ITransferListener {

  /** A listener that reports nothing and never cancels. */
  IInstallListener NONE = new IInstallListener() {};

  /** Stages of a single plugin install, in the order they occur. */
  enum Phase {
    /** Contacting repositories, resolving a SNAPSHOT to a unique version. */
    RESOLVE,
    /** Streaming the plugin zip to {@code plugins/.staging/.download/}. */
    DOWNLOAD,
    /** Expanding the zip into {@code plugins/.staging/<artifactId>/}. */
    UNZIP,
    /** Copying staged files into the live Hop installation. */
    ACTIVATE
  }

  /**
   * The install moved to a new phase.
   *
   * @param phase the phase now starting
   * @param detail extra context for the user (repository name, entry count), may be null
   */
  default void phase(Phase phase, String detail) {
    // no-op
  }

  /**
   * One of several artifacts in a batch is starting. Called by {@code EnvironmentApplier} so a
   * listener can show "3 of 12" and scale the bar across the batch. Never called for a single
   * install, which is equivalent to {@code item(label, 0, 1)}.
   *
   * @param label what is being installed
   * @param index zero-based position in the batch
   * @param total batch size
   */
  default void item(String label, int index, int total) {
    // no-op
  }
}
