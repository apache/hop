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
 *
 */

package org.apache.hop.core.fileinput;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import lombok.Getter;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.metadata.api.IEnumHasCode;

@Getter
public enum FileTypeFilter implements IEnumHasCode {
  FILES_AND_FOLDERS("all_files", FileType.FILE, FileType.FOLDER),
  ONLY_FILES("only_files", FileType.FILE),
  ONLY_FOLDERS("only_folders", FileType.FOLDER);

  private final String code;
  private final Collection<FileType> allowedFileTypes;

  FileTypeFilter(String name, FileType... allowedFileTypes) {
    this.code = name;
    this.allowedFileTypes = Collections.unmodifiableCollection(Arrays.asList(allowedFileTypes));
  }

  public boolean isFileTypeAllowed(FileType fileType) {
    return allowedFileTypes.contains(fileType);
  }

  @Override
  public String toString() {
    return code;
  }

  public static FileTypeFilter getByOrdinal(int ordinal) {
    for (FileTypeFilter filter : FileTypeFilter.values()) {
      if (filter.ordinal() == ordinal) {
        return filter;
      }
    }
    return ONLY_FILES;
  }

  public static FileTypeFilter getByName(String name) {
    for (FileTypeFilter filter : FileTypeFilter.values()) {
      if (filter.code.equals(name)) {
        return filter;
      }
    }
    return ONLY_FILES;
  }
}
