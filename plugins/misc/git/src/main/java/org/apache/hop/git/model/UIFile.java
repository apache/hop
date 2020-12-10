/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.model;

import org.eclipse.jgit.diff.DiffEntry.ChangeType;

public class UIFile {

  private String name;
  private ChangeType changeType;
  private Boolean isStaged = false;

  @Deprecated
  public UIFile(String name, ChangeType changeType) {
    this.name = name;
    this.changeType = changeType;
  }

  public UIFile(String name, ChangeType changeType, Boolean isStaged) {
    this.name = name;
    this.changeType = changeType;
    this.isStaged = isStaged;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ChangeType getChangeType() {
    return changeType;
  }

  public void setChangeType(ChangeType changeType) {
    this.changeType = changeType;
  }

  public String getImage() {
    final String location = "images/git/";
    switch (changeType) {
      case ADD:
      case COPY:
        return location + "added.svg";
      case MODIFY:
      case RENAME:
        return location + "changed.svg";
      case DELETE:
        return location + "removed.svg";
      default:
        return "";
    }
  }

  public Boolean getIsStaged() {
    return isStaged;
  }

  public void setIsStaged(Boolean isStaged) {
    this.isStaged = isStaged;
  }
}
