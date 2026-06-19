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

package org.apache.hop.workflow.actions.copyfiles;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** One source / destination / wildcard row for the Copy files action. */
@Getter
@Setter
public class CopyFilesItem {

  @HopMetadataProperty(key = "source_filefolder")
  private String sourceFileFolder;

  @HopMetadataProperty(key = "destination_filefolder")
  private String destinationFileFolder;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  public CopyFilesItem() {}

  public CopyFilesItem(String sourceFileFolder, String destinationFileFolder, String wildcard) {
    this.sourceFileFolder = sourceFileFolder;
    this.destinationFileFolder = destinationFileFolder;
    this.wildcard = wildcard;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CopyFilesItem that = (CopyFilesItem) o;
    return Objects.equals(sourceFileFolder, that.sourceFileFolder)
        && Objects.equals(destinationFileFolder, that.destinationFileFolder)
        && Objects.equals(wildcard, that.wildcard);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceFileFolder, destinationFileFolder, wildcard);
  }
}
