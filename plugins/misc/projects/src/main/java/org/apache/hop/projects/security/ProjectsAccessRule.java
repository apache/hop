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
 */

package org.apache.hop.projects.security;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import lombok.Setter;

/**
 * One access rule: a subject (user name, Hop role id, or container/LDAP group) may open a set of
 * projects (or all projects).
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProjectsAccessRule {

  public static final String TYPE_USER = "user";
  public static final String TYPE_ROLE = "role";
  public static final String TYPE_GROUP = "group";

  /** {@link #TYPE_USER}, {@link #TYPE_ROLE}, or {@link #TYPE_GROUP}. */
  private String subjectType = TYPE_USER;

  /** Username, Hop role id (admin/user/…), or container/IdP/LDAP group name. */
  private String subject = "";

  /** When true, the subject may open every registered project. */
  private boolean allProjects;

  /** Explicit project names when {@link #allProjects} is false. */
  private List<String> projects = new ArrayList<>();

  public ProjectsAccessRule() {}

  public ProjectsAccessRule(
      String subjectType, String subject, boolean allProjects, List<String> projects) {
    this.subjectType = subjectType;
    this.subject = subject;
    this.allProjects = allProjects;
    this.projects = projects != null ? new ArrayList<>(projects) : new ArrayList<>();
  }

  public String normalizedType() {
    if (subjectType == null || subjectType.isBlank()) {
      return TYPE_USER;
    }
    return subjectType.trim().toLowerCase(Locale.ROOT);
  }

  public String normalizedSubject() {
    return subject == null ? "" : subject.trim();
  }
}
