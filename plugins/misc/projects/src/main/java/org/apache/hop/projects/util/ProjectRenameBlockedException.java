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

package org.apache.hop.projects.util;

import java.util.List;
import lombok.Getter;
import org.apache.hop.core.exception.HopException;

/**
 * A project can't be renamed because the parent project reference in one or more other projects
 * can't be updated. Nothing was changed when this is thrown. The message names every blocking
 * project and the reason, so it can be shown to the user as is.
 */
@Getter
public class ProjectRenameBlockedException extends HopException {

  /** The names of the projects which block the rename */
  private final List<String> blockingProjects;

  /** The user facing explanation, without the decoration {@link HopException} adds */
  private final String userMessage;

  public ProjectRenameBlockedException(String userMessage, List<String> blockingProjects) {
    super(userMessage);
    this.userMessage = userMessage;
    this.blockingProjects = List.copyOf(blockingProjects);
  }
}
