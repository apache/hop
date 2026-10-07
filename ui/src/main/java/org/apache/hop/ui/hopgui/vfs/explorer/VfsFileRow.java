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

import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;

/** One child of a folder listing. No SWT, so the sort and filter rules can be tested headless. */
@Getter
public final class VfsFileRow {

  /** Size or last-modified value when the provider did not report one. */
  public static final long UNKNOWN = Long.MIN_VALUE;

  private static final AtomicInteger NEXT_ID = new AtomicInteger();

  private final int id = NEXT_ID.incrementAndGet();
  private final String name;
  private final String uri;
  private final boolean folder;
  private final String extension;
  private final long size;
  private final String sizeText;
  private final long lastModified;
  private final String lastModifiedText;
  private final String owner;
  private final String permissions;

  public VfsFileRow(
      String name,
      String uri,
      boolean folder,
      String extension,
      long size,
      String sizeText,
      long lastModified,
      String lastModifiedText,
      String owner,
      String permissions) {
    this.name = name == null ? "" : name;
    this.uri = uri == null ? "" : uri;
    this.folder = folder;
    this.extension = extension == null ? "" : extension;
    this.size = size;
    this.sizeText = sizeText == null ? "" : sizeText;
    this.lastModified = lastModified;
    this.lastModifiedText = lastModifiedText == null ? "" : lastModifiedText;
    this.owner = owner == null ? "" : owner;
    this.permissions = permissions == null ? "" : permissions;
  }
}
