/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * Run only these probe classes with the extracted distribution's lib/* classpath and plugins.
 * Deliberately has no static reference to Watch Files implementation classes and never registers
 * it.
 */
public class WatchFilesDistributionProbe {
  public static void main(String[] args) throws Exception {
    HopEnvironment.init();
    var plugin =
        PluginRegistry.getInstance().findPluginWithId(TransformPluginType.class, "WatchFiles");
    require(plugin != null, "Plugin was not automatically discovered from the installed ZIP");
    Path work = Path.of(args[0]).toAbsolutePath();
    Files.createDirectories(work);
    run(work.resolve("local"), false);
    if (System.getenv("WATCHFILES_SFTP_HOST") != null) run(work.resolve("sftp"), true);
    System.out.println("PACKAGED_WATCH_FILES_PROBE_PASSED");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void run(Path work, boolean sftp) throws Exception {
    Files.createDirectories(work.resolve("watch-files/input"));
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", work.toString());
    variables.setVariable(
        "SSH_HOST", System.getenv().getOrDefault("WATCHFILES_SFTP_HOST", "localhost"));
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    PipelineMeta metadata =
        new PipelineMeta(
            "config/projects/samples/watch-files/watch-files.hpl", provider, variables);
    Object options = metadata.findTransform("Watch directory").getTransform();
    ClassLoader loader = options.getClass().getClassLoader();
    require(
        !options
            .getClass()
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString()
            .contains("target/classes"),
        "Implementation leaked from a source checkout");
    String uri = work.resolve("watch-files/input").toString();
    if (sftp) {
      var sftpPlugin =
          PluginRegistry.getInstance()
              .findPluginWithId(
                  org.apache.hop.core.vfs.plugin.VfsPluginType.class, "sftp-connection");
      require(sftpPlugin != null, "Packaged SFTP provider missing");
      Class connectionType =
          PluginRegistry.getInstance()
              .getClassLoader(sftpPlugin)
              .loadClass("org.apache.hop.vfs.sftp.metadata.SftpConnection");
      Object connection = connectionType.getConstructor().newInstance();
      for (var property :
          java.util.Map.of(
                  "Name",
                  "packaged-sftp",
                  "ServerName",
                  "${SSH_HOST}",
                  "ServerPort",
                  "22",
                  "Username",
                  System.getenv("WATCHFILES_SFTP_USER"),
                  "Password",
                  System.getenv("WATCHFILES_SFTP_PASSWORD"),
                  "ConnectionTimeout",
                  "2000",
                  "SessionTimeout",
                  "1000")
              .entrySet()) {
        set(connection, property.getKey(), property.getValue());
      }
      connectionType.getMethod("setUserDirIsRoot", boolean.class).invoke(connection, false);
      provider
          .getSerializer(connectionType)
          .save((org.apache.hop.metadata.api.IHopMetadata) connection);
      uri =
          "packaged-sftp://"
              + System.getenv("WATCHFILES_SFTP_ROOT")
              + "/probe-"
              + java.util.UUID.randomUUID();
    }
    variables.setVariable("WATCH_INPUT", uri);
    set(options, "Directory", "${WATCH_INPUT}");
    set(options, "StateDirectory", "${PROJECT_HOME}/watch-files/state");
    set(options, "MinimumAge", "0");
    set(options, "StabilityInterval", "10");
    set(options, "PollingInterval", "50");
    set(options, "CheckpointInterval", "25");
    set(options, "Strategy", sftp ? "AUTO" : "NATIVE");
    Path savedPipeline = work.resolve("saved.hpl");
    Files.writeString(savedPipeline, metadata.getXml(variables));
    metadata = new PipelineMeta(savedPipeline.toString(), provider, variables);
    require(
        metadata
            .findTransform("Watch directory")
            .getTransform()
            .getClass()
            .getName()
            .endsWith("WatchFilesMeta"),
        "Saved pipeline did not reload");
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    // The producer needs a live metadata namespace before the source's prepareExecution()
    // acquires its own. Otherwise an unknown connection alias can be parsed as a local path.
    var producerNamespace =
        org.apache.hop.core.vfs.HopVfsNamespaces.acquire(
            variables, provider, "packaged validation producer");
    var previousNamespace = org.apache.hop.core.vfs.HopVfsNamespaces.bindThread(producerNamespace);
    try {
      LocalPipelineEngine first = engine(metadata, variables);
      try (FileObject root = HopVfs.getFileObject(uri, first)) {
        root.createFolder();
      }
      first.prepareExecution();
      listen(first, rows);
      first.startThreads();
      Path checkpoint = work.resolve("watch-files/state/sample-watch-files.json");
      try {
        write(first, uri, "first.csv");
        expect(first, rows, "first.csv");
        waitCheckpoint(checkpoint, "first.csv");
      } finally {
        stop(first);
      }
      // A genuine v1 fixture, without the new schema fields, verifies upgrade in the installed JAR.
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode legacy = (ObjectNode) mapper.readTree(Files.readString(checkpoint));
      legacy.put("version", 1);
      legacy.remove(List.of("semantics", "generation", "savedAt"));
      Files.writeString(checkpoint, mapper.writeValueAsString(legacy));
      metadata.findTransform("Watch directory").setName("Renamed Watch");
      LocalPipelineEngine second = engine(metadata, variables);
      write(second, uri, "offline.csv");
      second.prepareExecution();
      listen(second, rows);
      second.startThreads();
      try {
        expect(second, rows, "offline.csv");
        require(rows.poll(200, TimeUnit.MILLISECONDS) == null, "Baseline was repeated");
        waitCheckpoint(checkpoint, "offline.csv");
      } finally {
        stop(second);
      }
      require(
          mapper.readTree(Files.readString(checkpoint)).path("version").asInt() == 2,
          "Schema was not upgraded");
      require(
          Files.isDirectory(checkpoint.getParent().resolve("sample-watch-files.history")),
          "Migration backup missing");
      Class<?> managerType =
          loader.loadClass("org.apache.hop.pipeline.transforms.watchfiles.WatchFilesStateManager");
      Object manager =
          managerType
              .getConstructor(Path.class, String.class, int.class)
              .newInstance(checkpoint.getParent(), "sample-watch-files", 100000);
      require(
          ((Number) managerType.getMethod("replay", String.class).invoke(manager, "first\\.csv"))
                  .intValue()
              == 1,
          "Packaged replay did not select exactly one file");
      LocalPipelineEngine replay = engine(metadata, variables);
      replay.prepareExecution();
      listen(replay, rows);
      replay.startThreads();
      try {
        expect(replay, rows, "first.csv");
        require(rows.poll(200, TimeUnit.MILLISECONDS) == null, "Replay included another file");
      } finally {
        stop(replay);
      }
      if (!sftp) gui(metadata, variables, loader);
      else
        try (FileObject root = HopVfs.getFileObject(uri, replay)) {
          root.deleteAll();
        }
      System.out.println(
          "PACKAGED_CASE_PASSED mode=" + (sftp ? "SFTP" : "NATIVE") + " schema=2 replay=1");
    } finally {
      org.apache.hop.core.vfs.HopVfsNamespaces.restoreThread(previousNamespace);
      if (producerNamespace != null) org.apache.hop.core.vfs.HopVfsNamespaces.release(provider);
    }
  }

  private static LocalPipelineEngine engine(PipelineMeta metadata, Variables variables) {
    LocalPipelineEngine pipeline = new LocalPipelineEngine(metadata);
    for (String name : variables.getVariableNames())
      pipeline.setVariable(name, variables.getVariable(name));
    pipeline.setLogLevel(LogLevel.ERROR);
    return pipeline;
  }

  private static void listen(LocalPipelineEngine pipeline, LinkedBlockingQueue<Object[]> rows) {
    pipeline
        .findRunThread("Log changes")
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta meta, Object[] row) {
                rows.add(row.clone());
              }
            });
  }

  private static void write(LocalPipelineEngine pipeline, String uri, String name)
      throws Exception {
    try (FileObject root = HopVfs.getFileObject(uri, pipeline);
        FileObject file = root.resolveFile(name);
        var stream = HopVfs.getOutputStream(file, false)) {
      stream.write("payload".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  private static void expect(
      LocalPipelineEngine pipeline, LinkedBlockingQueue<Object[]> rows, String name)
      throws Exception {
    Object[] row = rows.poll(10, TimeUnit.SECONDS);
    require(row != null, "No output for " + name);
    require(
        row.length == 13 && name.equals(row[1]) && "CREATED".equals(row[4]), "Unexpected output");
    try (FileObject file = HopVfs.getFileObject(row[0].toString(), pipeline);
        var stream = file.getContent().getInputStream()) {
      require(
          "payload"
              .equals(new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)),
          "Emitted filename cannot be read");
    }
  }

  private static void waitCheckpoint(Path file, String name) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (Files.exists(file) && Files.readString(file).contains(name)) return;
      Thread.sleep(10);
    }
    throw new AssertionError("Checkpoint did not include " + name);
  }

  private static void stop(LocalPipelineEngine pipeline) {
    pipeline.stopAll();
    pipeline.waitUntilFinished();
    require(pipeline.getErrors() == 0, "Pipeline errors");
  }

  private static void set(Object object, String field, String value) throws Exception {
    object.getClass().getMethod("set" + field, String.class).invoke(object, value);
  }

  private static void require(boolean valid, String message) {
    if (!valid) throw new AssertionError(message);
  }

  private static void gui(PipelineMeta metadata, Variables variables, ClassLoader loader)
      throws Exception {
    HopGuiEnvironment.init();
    Display display = Display.getDefault();
    PropsUi.getInstance();
    org.apache.hop.ui.core.gui.GuiResource.getInstance();
    Object options = metadata.findTransform("Renamed Watch").getTransform();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    display.timerExec(
        500,
        () -> {
          try {
            Shell dialog =
                java.util.Arrays.stream(display.getShells())
                    .filter(shell -> shell.getText().equals("Watch Files"))
                    .findFirst()
                    .orElseThrow();
            CTabFolder tabs = find(dialog, CTabFolder.class);
            require(tabs != null && tabs.getItemCount() == 6, "Installed GUI tabs missing");
            require(
                find((Composite) tabs.getItem(1).getControl(), org.eclipse.swt.widgets.Combo.class)
                        .getItems()
                        .length
                    == 3,
                "Strategy choices missing");
            Button cancel = findButton(dialog, "Cancel");
            require(cancel != null, "Cancel missing");
            cancel.notifyListeners(SWT.Selection, new org.eclipse.swt.widgets.Event());
          } catch (Throwable error) {
            failure.set(error);
            for (Shell shell : display.getShells()) shell.dispose();
          }
        });
    Shell parent = new Shell(display);
    try {
      Object dialog =
          loader
              .loadClass("org.apache.hop.pipeline.transforms.watchfiles.WatchFilesDialog")
              .getConstructor(
                  Shell.class,
                  org.apache.hop.core.variables.IVariables.class,
                  options.getClass(),
                  PipelineMeta.class)
              .newInstance(parent, variables, options, metadata);
      dialog.getClass().getMethod("open").invoke(dialog);
      if (failure.get() != null) throw new AssertionError("Installed GUI failed", failure.get());
    } finally {
      if (!parent.isDisposed()) parent.dispose();
      display.dispose();
    }
  }

  private static <T> T find(Composite parent, Class<T> type) {
    for (Control child : parent.getChildren()) {
      if (type.isInstance(child)) return type.cast(child);
      if (child instanceof Composite composite) {
        T found = find(composite, type);
        if (found != null) return found;
      }
    }
    return null;
  }

  private static Button findButton(Composite parent, String label) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Button button && button.getText().replace("&", "").trim().equals(label))
        return button;
      if (child instanceof Composite composite) {
        Button found = findButton(composite, label);
        if (found != null) return found;
      }
    }
    return null;
  }
}
