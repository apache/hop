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

package org.apache.hop.projects.git;

/** Represents a single repository returned by a Git provider's API. */
public class GitRepositoryInfo {

  private final String name;
  private final String owner;
  private final String description;
  private final boolean privateRepo;
  private final String lastUpdated;
  private final String httpsCloneUrl;
  private final String sshCloneUrl;

  public GitRepositoryInfo(
      String name,
      String owner,
      String description,
      boolean privateRepo,
      String lastUpdated,
      String httpsCloneUrl,
      String sshCloneUrl) {
    this.name = name;
    this.owner = owner;
    this.description = description;
    this.privateRepo = privateRepo;
    this.lastUpdated = lastUpdated;
    this.httpsCloneUrl = httpsCloneUrl;
    this.sshCloneUrl = sshCloneUrl;
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public String getDescription() {
    return description;
  }

  public boolean isPrivateRepo() {
    return privateRepo;
  }

  public String getLastUpdated() {
    return lastUpdated;
  }

  public String getHttpsCloneUrl() {
    return httpsCloneUrl;
  }

  public String getSshCloneUrl() {
    return sshCloneUrl;
  }

  /** Short ISO date (first 10 chars) for display, or empty string if null. */
  public String getLastUpdatedShort() {
    if (lastUpdated == null || lastUpdated.length() < 10) {
      return lastUpdated != null ? lastUpdated : "";
    }
    return lastUpdated.substring(0, 10);
  }
}
