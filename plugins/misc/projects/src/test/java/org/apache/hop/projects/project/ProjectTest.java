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

package org.apache.hop.projects.project;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

public class ProjectTest {

  @Test
  public void testEnforcingExecutionInHomeSerialization() throws Exception {
    File tempFile = Files.createTempFile("project-config-test", ".json").toFile();
    tempFile.deleteOnExit();

    try {
      Project project = new Project(tempFile.getAbsolutePath());
      project.setEnforcingExecutionInHome(false);
      project.saveToFile();

      // Read back
      Project readProject = new Project(tempFile.getAbsolutePath());
      readProject.readFromFile();

      assertFalse(readProject.isEnforcingExecutionInHome());

      // Test true
      project.setEnforcingExecutionInHome(true);
      project.saveToFile();

      readProject = new Project(tempFile.getAbsolutePath());
      readProject.readFromFile();

      assertTrue(readProject.isEnforcingExecutionInHome());
    } finally {
      tempFile.delete();
    }
  }
}
