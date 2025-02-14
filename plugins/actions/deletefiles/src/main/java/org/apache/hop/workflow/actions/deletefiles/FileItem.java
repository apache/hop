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

package org.apache.hop.workflow.actions.deletefiles;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Setter
@Getter
public class FileItem {

  /** File name */
  @HopMetadataProperty(key = "name")
  private String fileName;

  /** Wildcard or file mask (regular expression) */
  @HopMetadataProperty(key = "filemask")
  private String fileMask;

  public FileItem() {}

  public FileItem(String fileName, String fileMask) {
    this.fileName = fileName;
    this.fileMask = fileMask;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileItem item = (FileItem) o;
    return fileName.equals(item.fileName) && java.util.Objects.equals(fileMask, item.fileMask);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(fileName, fileMask);
  }
}
