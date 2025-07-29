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
 *
 */

package org.apache.hop.workflow.actions.documentation;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/** Table of content for the documentation: keep track of artifacts so we can generate a TOC. */
@Getter
@Setter
public class Toc {
  private List<TocEntry> entries;

  public Toc() {
    this.entries = new ArrayList<>();
  }

  public void sortEntries() {
    entries.sort(
        (e1, e2) -> {
          int folderCmp = e1.relativeFolder().compareTo(e2.relativeFolder());
          if (folderCmp != 0) {
            return folderCmp;
          }
          int typeCmp = e1.type().compareTo(e2.type());
          if (typeCmp != 0) {
            return typeCmp;
          }
          return e1.description().compareTo(e2.description());
        });
  }
}
