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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/** Fetches commits, issues, or pull requests from a Git hosting provider REST API. */
public interface GitResourceClient {

  GitResourceReader openReader(
      GitResourceType resourceType, String owner, String repository, GitListOptions options)
      throws HopException;

  default List<GitResourceRecord> listResources(
      GitResourceType resourceType, String owner, String repository, GitListOptions options)
      throws HopException {
    List<GitResourceRecord> records = new ArrayList<>();
    GitResourceReader reader = openReader(resourceType, owner, repository, options);
    while (reader.hasNext()) {
      records.add(reader.next());
    }
    return records;
  }

  static GitResourceClient forConnection(GitConnection connection, IVariables variables)
      throws HopException {
    GitProvider provider = connection.getGitProvider();
    String apiBaseUrl = connection.getResolvedApiBaseUrl(variables);
    GitAuth auth = connection.toAuth(variables);
    return switch (provider) {
      case GITHUB_CLOUD, GITHUB_ENTERPRISE -> new GitHubResourceClient(apiBaseUrl, auth);
      case GITLAB -> new GitLabResourceClient(apiBaseUrl, auth);
      case BITBUCKET -> new BitbucketResourceClient(apiBaseUrl, auth);
      case FORGEJO, GITEA -> new GiteaResourceClient(apiBaseUrl, auth);
    };
  }
}
