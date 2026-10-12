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
import org.eclipse.swt.widgets.Display;

/**
 * Files and folders copied or cut inside the VFS File Explorer. This is not the operating-system
 * clipboard, so text in a location field is left alone. The window and the bottom panel share one
 * clipboard. It is kept on the display: Hop Web has a display per session, so one user's cut files
 * never show up in another user's paste.
 */
public final class VfsFileClipboard {

  static final String DISPLAY_DATA_KEY = VfsFileClipboard.class.getName();

  private List<VfsFileTransfer.Entry> entries = List.of();
  private VfsFileTransfer.Mode mode = VfsFileTransfer.Mode.COPY;

  VfsFileClipboard() {}

  /**
   * The clipboard of {@code display}. Call on the UI thread. Without a display a new, unshared
   * clipboard is returned.
   */
  public static VfsFileClipboard of(Display display) {
    if (display == null || display.isDisposed()) {
      return new VfsFileClipboard();
    }
    Object existing = display.getData(DISPLAY_DATA_KEY);
    if (existing instanceof VfsFileClipboard clipboard) {
      return clipboard;
    }
    VfsFileClipboard clipboard = new VfsFileClipboard();
    display.setData(DISPLAY_DATA_KEY, clipboard);
    return clipboard;
  }

  public synchronized void set(VfsFileTransfer.Mode mode, List<VfsFileTransfer.Entry> items) {
    this.mode = mode == null ? VfsFileTransfer.Mode.COPY : mode;
    entries = List.copyOf(VfsFileTransfer.withoutNested(items));
  }

  public synchronized List<VfsFileTransfer.Entry> entries() {
    return new ArrayList<>(entries);
  }

  public synchronized VfsFileTransfer.Mode mode() {
    return mode;
  }

  public synchronized boolean isEmpty() {
    return entries.isEmpty();
  }

  /**
   * After a paste of {@code pasted}, keep only {@code remaining}: the moved files are no longer
   * where the clipboard points. Left alone when something else was copied or cut in the meantime.
   */
  public synchronized void retain(
      List<VfsFileTransfer.Entry> pasted, List<VfsFileTransfer.Entry> remaining) {
    if (!entries.equals(pasted)) {
      return;
    }
    entries = remaining == null ? List.of() : List.copyOf(remaining);
    if (entries.isEmpty()) {
      mode = VfsFileTransfer.Mode.COPY;
    }
  }

  public synchronized void clear() {
    entries = List.of();
    mode = VfsFileTransfer.Mode.COPY;
  }
}
