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

/**
 * Hop's SFTP VFS provider, forked from Apache Commons VFS 2.10.0 ({@code
 * org.apache.commons.vfs2.provider.sftp}).
 *
 * <p>The fork exists for one reason: upstream's {@code SftpFileObject} guards its state with
 * method-level {@code synchronized}, which locks the file object. Every other Commons VFS provider
 * - and {@code AbstractFileObject} itself - locks the <em>file system</em> instead. Mixing the two
 * gives the SFTP provider two monitors that are taken in opposite orders, which deadlocks whenever
 * two threads touch the same remote file at once:
 *
 * <ul>
 *   <li>{@code AbstractFileSystem.resolveFile} (file system) -&gt; {@code refresh} -&gt; {@code
 *       detach} -&gt; {@code SftpFileObject.doDetach} (file object)
 *   <li>{@code SftpFileObject.getPermissions} (file object) -&gt; {@code statSelf} -&gt; {@code
 *       putChannel} -&gt; {@code SftpFileSystem.putChannel} (file system)
 * </ul>
 *
 * <p>Every class here is a verbatim copy of the 2.10.0 source except {@code SftpFileObject}, where
 * those {@code synchronized} methods became {@code synchronized (getFileSystem())} blocks. That
 * costs no concurrency - {@code AbstractFileObject} already holds the file system monitor across
 * attach, detach, getType, getChildren, getContent, createFile, deleteSelf and getParent - and it
 * leaves a single monitor, so there is no order left to invert.
 *
 * <p>The fork tracks the <strong>2.10.0 tag</strong>, not upstream master, because it runs against
 * the {@code AbstractFileObject} / {@code AbstractFileSystem} of the Commons VFS release Hop
 * actually ships. Master's SFTP provider calls {@code resolveFileInternal(FileName)}, added to
 * those base classes for 2.11.0, so it cannot be dropped onto 2.10.0 as-is.
 *
 * <p>Post-2.10.0 upstream fixes carried here, all self-contained:
 *
 * <ul>
 *   <li>{@code 528ac3f0} - {@code executeCommand} restores the thread's interrupt flag
 *   <li>{@code 91eca178} - {@code SftpClientFactory} resolves the current directory on Java 25
 *   <li>{@code 5f773632} - {@code refresh()} clears cached attributes when never attached
 * </ul>
 *
 * <p>Deliberately not carried: {@code ed12c669} (VFS-862, ON_RESOLVE on internal navigation), which
 * needs the 2.11.0 base-class API. Take it when Hop moves to Commons VFS 2.11.0, and re-fork from
 * that tag at the same time.
 *
 * <p>Keep the rest of the fork in step with upstream, and drop it once the fix is released there.
 */
package org.apache.hop.vfs.sftp.provider;
