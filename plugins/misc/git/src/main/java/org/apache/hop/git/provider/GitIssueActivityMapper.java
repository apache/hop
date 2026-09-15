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

import org.json.simple.JSONObject;

final class GitIssueActivityMapper {

  private GitIssueActivityMapper() {}

  static GitResourceRecord comment(
      String provider,
      GitResourceType commentType,
      String owner,
      String repository,
      long issueNumber,
      String issueTitle,
      String issueState,
      String id,
      String author,
      String createdAt,
      String updatedAt,
      String url,
      String body,
      JSONObject rawJson) {
    return GitResourceRecord.builder()
        .provider(provider)
        .entityType(commentType.getEntityType())
        .repoOwner(owner)
        .repoName(repository)
        .id(id)
        .number(issueNumber)
        .title(issueTitle)
        .state(issueState)
        .body(body)
        .author(author)
        .authorLogin(author)
        .createdAt(createdAt)
        .updatedAt(updatedAt)
        .url(url)
        .rawJson(rawJson.toJSONString())
        .build();
  }

  static GitResourceRecord event(
      String provider,
      String owner,
      String repository,
      long issueNumber,
      String issueTitle,
      String issueState,
      String id,
      String eventType,
      String eventDetail,
      String actor,
      String createdAt,
      String url,
      JSONObject rawJson) {
    return GitResourceRecord.builder()
        .provider(provider)
        .entityType(GitResourceType.ISSUE_EVENTS.getEntityType())
        .repoOwner(owner)
        .repoName(repository)
        .id(id)
        .number(issueNumber)
        .title(eventType)
        .state(eventDetail)
        .body(eventDetail)
        .author(actor)
        .authorLogin(actor)
        .createdAt(createdAt)
        .url(url)
        .rawJson(rawJson.toJSONString())
        .build();
  }
}
