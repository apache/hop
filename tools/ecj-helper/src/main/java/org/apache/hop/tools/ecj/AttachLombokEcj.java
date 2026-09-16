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

package org.apache.hop.tools.ecj;

import com.sun.tools.attach.VirtualMachine;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Build-time helper invoked from exec-maven-plugin (same JVM as Maven).
 *
 * <p>Writes {@code .mvn/plexus-eclipse-hint.jar} from the XML descriptor so Debian/Ubuntu Maven
 * (Sisu 0.3.x) can register {@code compilerId=eclipse} without ASM-indexing the Java 17 Eclipse
 * compiler class, then self-attaches the Lombok ECJ agent once per Maven JVM.
 */
public final class AttachLombokEcj {

  private AttachLombokEcj() {}

  public static void main(String[] args) throws Exception {
    String rootPath = System.getProperty("maven.multiModuleProjectDirectory");
    if (rootPath == null || rootPath.isEmpty()) {
      throw new IllegalStateException("maven.multiModuleProjectDirectory is not set");
    }
    File root = new File(rootPath);
    synchronized (System.getProperties()) {
      writeHintJar(root);
      attachLombok(root);
    }
  }

  private static void writeHintJar(File root) throws IOException {
    File hintXml = new File(root, ".mvn/plexus-eclipse-hint/META-INF/plexus/components.xml");
    File hintJar = new File(root, ".mvn/plexus-eclipse-hint.jar");
    if (!hintXml.isFile()) {
      throw new IllegalStateException("Eclipse compiler Plexus descriptor missing: " + hintXml);
    }
    if (hintJar.isFile() && hintJar.lastModified() >= hintXml.lastModified()) {
      return;
    }
    File parent = hintJar.getParentFile();
    if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
      throw new IllegalStateException("Unable to create " + parent);
    }
    try (FileOutputStream fos = new FileOutputStream(hintJar);
        ZipOutputStream zos = new ZipOutputStream(fos);
        FileInputStream in = new FileInputStream(hintXml)) {
      zos.putNextEntry(new ZipEntry("META-INF/plexus/components.xml"));
      byte[] buffer = new byte[4096];
      int n;
      while ((n = in.read(buffer)) >= 0) {
        zos.write(buffer, 0, n);
      }
      zos.closeEntry();
    }
    System.out.println("Wrote Eclipse compiler Plexus descriptor jar: " + hintJar);
  }

  private static File agentMarker() {
    return new File(
        System.getProperty("java.io.tmpdir"),
        "hop-lombok-ecj-agent-" + ProcessHandle.current().pid());
  }

  private static void attachLombok(File root) throws Exception {
    if ("true".equals(String.valueOf(System.getProperty("lombok.ecj.agent.skip")))) {
      return;
    }
    // exec:java restores System properties after main() returns, and loads this
    // class in a new classloader per module, so a pid-scoped file is the durable
    // "already attached" flag for this Maven JVM.
    if (System.getProperty("lombok.ecj.agent.attached") != null || agentMarker().isFile()) {
      return;
    }
    File lombokJar = findLombokJar(root);
    if (!lombokJar.isFile() || lombokJar.length() < 1_000_000L) {
      throw new IllegalStateException(
          "Lombok jar missing or incomplete for the ECJ agent attach, expected " + lombokJar);
    }
    File agentJar = Files.createTempFile("lombok-ecj-agent-", ".jar").toFile();
    agentJar.deleteOnExit();
    Files.copy(lombokJar.toPath(), agentJar.toPath(), StandardCopyOption.REPLACE_EXISTING);
    try {
      VirtualMachine vm = VirtualMachine.attach(String.valueOf(ProcessHandle.current().pid()));
      try {
        vm.loadAgent(agentJar.getAbsolutePath(), "ECJ");
      } finally {
        vm.detach();
      }
    } catch (Throwable t) {
      throw new IllegalStateException(
          "Unable to self-attach the Lombok ECJ agent, ensure -Djdk.attach.allowAttachSelf=true is"
              + " in .mvn/jvm.config and the environment allows self-attach, or skip it with"
              + " -Dlombok.ecj.agent.skip=true",
          t);
    }
    File marker = agentMarker();
    marker.createNewFile();
    marker.deleteOnExit();
    System.setProperty("lombok.ecj.agent.attached", "true");
    System.out.println("Lombok ECJ agent attached (source: " + lombokJar + ")");
  }

  private static File findLombokJar(File root) {
    try {
      URL loc =
          Class.forName("lombok.launch.Agent").getProtectionDomain().getCodeSource().getLocation();
      if (loc != null && "file".equals(loc.getProtocol())) {
        File fromAgent = new File(loc.toURI());
        if (fromAgent.isFile()) {
          return fromAgent;
        }
      }
    } catch (Exception ignored) {
      // fall through to .mvn/lombok-*.jar
    }
    String version = System.getProperty("lombok.version", "");
    return new File(root, ".mvn/lombok-" + version + ".jar");
  }
}
