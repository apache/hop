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

package org.apache.hop.projects.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ProjectsGuiPluginZipTest {

  @Test
  public void testRelativeZipPathAsIsUsesProjectHomeAsRoot() {
    String projectHome = "file:///home/user/my-project";
    String file = "file:///home/user/my-project/pipelines/demo.hpl";
    assertEquals("pipelines/demo.hpl", ProjectsGuiPlugin.relativeZipPath(file, projectHome));
  }

  @Test
  public void testRelativeZipPathAsIsWithTrailingSlashOnHome() {
    String projectHome = "file:///home/user/my-project/";
    String file = "file:///home/user/my-project/project-config.json";
    assertEquals("project-config.json", ProjectsGuiPlugin.relativeZipPath(file, projectHome));
  }

  @Test
  public void testRelativeZipPathNormalNestsUnderProjectName() {
    String parent = "file:///home/user";
    String file = "file:///home/user/my-project/pipelines/demo.hpl";
    assertEquals("my-project/pipelines/demo.hpl", ProjectsGuiPlugin.relativeZipPath(file, parent));
  }

  @Test
  public void testRelativeZipPathAlreadyRelativeUnchanged() {
    assertEquals(
        "my-project/hop-config.json",
        ProjectsGuiPlugin.relativeZipPath("my-project/hop-config.json", "file:///home/user"));
  }
}
