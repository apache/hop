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

package org.apache.hop.git.provider;

import lombok.Getter;

/** A repository returned by a Git hosting provider listing API. */
@Getter
public class GitRepositoryInfo {

  private final String name;
  private final String owner;
  private final String description;
  private final boolean privateRepo;
  private final String lastUpdated;

  public GitRepositoryInfo(
      String name, String owner, String description, boolean privateRepo, String lastUpdated) {
    this.name = name;
    this.owner = owner;
    this.description = description;
    this.privateRepo = privateRepo;
    this.lastUpdated = lastUpdated;
  }

  public String getLastUpdatedShort() {
    if (lastUpdated == null || lastUpdated.length() < 10) {
      return lastUpdated != null ? lastUpdated : "";
    }
    return lastUpdated.substring(0, 10);
  }
}
