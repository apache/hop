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

package org.apache.hop.ui.hopgui.vfs.explorer;

import java.util.ArrayList;
import java.util.List;

/**
 * Files and folders copied or cut inside the VFS File Explorer. This is not the operating-system
 * clipboard, so text in a location field is left alone. One clipboard is shared by every explorer
 * in the session, including the window and the bottom panel.
 */
public final class VfsFileClipboard {

  private static List<VfsFileTransfer.Entry> entries = List.of();
  private static VfsFileTransfer.Mode mode = VfsFileTransfer.Mode.COPY;

  private VfsFileClipboard() {}

  public static synchronized void set(
      VfsFileTransfer.Mode mode, List<VfsFileTransfer.Entry> items) {
    VfsFileClipboard.mode = mode == null ? VfsFileTransfer.Mode.COPY : mode;
    entries = List.copyOf(VfsFileTransfer.withoutNested(items));
  }

  public static synchronized List<VfsFileTransfer.Entry> entries() {
    return new ArrayList<>(entries);
  }

  public static synchronized VfsFileTransfer.Mode mode() {
    return mode;
  }

  public static synchronized boolean isEmpty() {
    return entries.isEmpty();
  }

  public static synchronized void clear() {
    entries = List.of();
    mode = VfsFileTransfer.Mode.COPY;
  }
}
