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

package org.apache.hop.core.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * A fat jar build says how far it is: every 10% of the jar files in the log, every jar to a
 * monitor.
 */
class FatJarBuilderProgressTest {

  @TempDir Path folder;

  @Test
  void theBuildLogsItsProgress() throws Exception {
    List<String> jars = jars(20);
    ILogChannel log = mock(ILogChannel.class);
    new FatJarBuilder(log, new Variables(), folder.resolve("fat.jar").toString(), jars)
        .buildTargetJar();

    ArgumentCaptor<String> lines = ArgumentCaptor.forClass(String.class);
    verify(log, atLeastOnce()).logBasic(lines.capture());
    List<String> progress =
        lines.getAllValues().stream().filter(line -> line.startsWith("Fat jar: ")).toList();
    assertEquals(10, progress.size(), String.valueOf(progress));
    assertEquals("Fat jar: 2 of 20 jar files added (10%)", progress.get(0));
    assertEquals("Fat jar: 20 of 20 jar files added (100%)", progress.get(9));
  }

  @Test
  void theBuildReportsEveryJarToTheMonitor() throws Exception {
    IProgressMonitor monitor = mock(IProgressMonitor.class);
    FatJarBuilder builder =
        new FatJarBuilder(
            mock(ILogChannel.class),
            new Variables(),
            folder.resolve("fat.jar").toString(),
            jars(5));
    builder.setProgressMonitor(monitor);
    builder.buildTargetJar();

    verify(monitor).beginTask(anyString(), eq(5));
    verify(monitor, times(5)).worked(1);
    verify(monitor).subTask("lib-0.jar");
    // Marking the work done is up to the caller: the Hop GUI dialog closes on done()
    verify(monitor, never()).done();
  }

  @Test
  void aCanceledBuildStopsAndLeavesNoJar() throws Exception {
    IProgressMonitor monitor = mock(IProgressMonitor.class);
    when(monitor.isCanceled()).thenReturn(false, false, true);
    Path target = folder.resolve("fat.jar");
    FatJarBuilder builder =
        new FatJarBuilder(mock(ILogChannel.class), new Variables(), target.toString(), jars(5));
    builder.setProgressMonitor(monitor);

    HopException e = assertThrows(HopException.class, builder::buildTargetJar);
    assertEquals(FatJarBuilder.CANCELED_MESSAGE, e.getMessage().trim());
    verify(monitor, times(2)).worked(anyInt());
    assertFalse(Files.exists(target));
  }

  private List<String> jars(int count) throws Exception {
    List<String> jars = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Path jar = folder.resolve("lib-" + i + ".jar");
      try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(jar.toFile()))) {
        zip.putNextEntry(new ZipEntry("org/example/C" + i + ".txt"));
        zip.write(new byte[] {1});
        zip.closeEntry();
      }
      jars.add(jar.toString());
    }
    return jars;
  }
}
