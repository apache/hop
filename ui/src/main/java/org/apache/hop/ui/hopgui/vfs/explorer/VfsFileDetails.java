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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.vfs.VfsFileAttributes;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;

/** Reads one file's details from data the provider already has. Does not open the file. */
public final class VfsFileDetails {

  private VfsFileDetails() {}

  public static VfsFileRow read(FileObject file) throws Exception {
    String name = file.getName().getBaseName();
    boolean folder = file.isFolder();
    long size = VfsFileRow.UNKNOWN;
    long modified = VfsFileRow.UNKNOWN;
    String owner = "";
    String permissions = "";
    try {
      if (!folder) {
        size = file.getContent().getSize();
      }
    } catch (Exception ignored) {
      size = VfsFileRow.UNKNOWN;
    }
    try {
      modified = file.getContent().getLastModifiedTime();
    } catch (Exception ignored) {
      modified = VfsFileRow.UNKNOWN;
    }
    try {
      Map<String, Object> attributes = file.getContent().getAttributes();
      owner = text(attributes.get(VfsFileAttributes.OWNER));
      permissions = text(attributes.get(VfsFileAttributes.PERMISSIONS));
    } catch (Exception ignored) {
      // Provider has no attributes. The cells stay blank.
    }
    return new VfsFileRow(
        name,
        HopVfs.getFilename(file),
        folder,
        folder ? "" : HopFileTypeBase.extractExtension(name),
        size,
        folder ? "" : formatSize(size),
        modified,
        formatDate(modified),
        owner,
        permissions);
  }

  static String formatSize(long size) {
    if (size < 0) {
      return "";
    }
    String[] units = {"", " kB", " MB", " GB", " TB", " PB", " XB", " YB", " ZB"};
    DecimalFormat format = new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ROOT));
    for (int i = 0; i < units.length; i++) {
      double unitSize = Math.pow(1024, i);
      double maxSize = Math.pow(1024, i + 1);
      if (size < maxSize) {
        return format.format(size / unitSize) + units[i];
      }
    }
    return Long.toString(size);
  }

  static String formatDate(long millis) {
    if (millis <= 0) {
      return "";
    }
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.ROOT).format(new Date(millis));
  }

  private static String text(Object value) {
    return value == null ? "" : value.toString();
  }
}
