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

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;

/**
 * Runs Text File Input on the real local engine, in one or more copies, and keeps the rows each
 * copy wrote. Unlike the mock-based tests it goes through {@code init()}, the real copy count and
 * the real reader, so it sees what a user's pipeline sees.
 */
final class TextFileInputHarness {

  static final String TRANSFORM = "Text file input";
  static final String FILENAMES = "Filenames";
  static final String FILENAME_FIELD = "filename";

  /** A second field on each filename row, "name-N" for the Nth row, to check pass-through. */
  static final String TAG_FIELD = "tag";

  private TextFileInputHarness() {}

  /** Call once per test class, from a {@code @BeforeAll}. */
  static void initHop() throws HopException {
    HopEnvironment.init();
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
    register(TextFileInputMeta.class);
    register(InjectorMeta.class);
  }

  private static void register(Class<?> metaClass) throws HopException {
    PluginRegistry.getInstance()
        .registerPluginClass(
            metaClass.getClassLoader(),
            new ArrayList<>(),
            null,
            metaClass.getName(),
            TransformPluginType.class,
            org.apache.hop.core.annotations.Transform.class,
            true);
  }

  /** A CSV meta with no header, comma separator and the given String fields. */
  static TextFileInputMeta csvMeta(String... fieldNames) {
    TextFileInputMeta meta = new TextFileInputMeta();
    TextFileInputMeta.Content content = meta.getContent();
    content.setSeparator(",");
    content.setEnclosure("\"");
    content.setHeader(false);
    content.setFileFormat("mixed");
    for (String name : fieldNames) {
      meta.getInputFields().add(new TextFileInputField(name, -1, -1));
    }
    return meta;
  }

  static void addFile(TextFileInputMeta meta, Path file) {
    addFile(meta, file.toString());
  }

  /** Adds a file by its VFS name, like ram:///data/file.csv. */
  static void addFile(TextFileInputMeta meta, String file) {
    InputFile inputFile = new InputFile();
    inputFile.setFileName(file);
    inputFile.setFileRequired(true);
    meta.getFileInput().getInputFiles().add(inputFile);
  }

  static Path write(Path dir, String name, String content) throws IOException {
    return write(dir, name, content, StandardCharsets.UTF_8);
  }

  static Path write(Path dir, String name, String content, Charset charset) throws IOException {
    return Files.write(dir.resolve(name), content.getBytes(charset));
  }

  static Path write(Path dir, String name, byte[] content) throws IOException {
    return Files.write(dir.resolve(name), content);
  }

  /** Runs the transform with its static file list. */
  static Result run(TextFileInputMeta meta, int copies) throws HopException {
    return execute(meta, copies, null);
  }

  /**
   * Runs the transform fed with filenames from an Injector, as "Accept filenames from previous
   * transform" does. The rows are spread over the copies the way the engine distributes them.
   */
  static Result runWithFilenames(TextFileInputMeta meta, int copies, List<Path> files)
      throws HopException {
    return runWithFilenames(meta, copies, files, true);
  }

  /**
   * As {@link #runWithFilenames(TextFileInputMeta, int, List)}, with the Injector either
   * distributing its rows over the copies or copying every row to every copy.
   */
  static Result runWithFilenames(
      TextFileInputMeta meta, int copies, List<Path> files, boolean distribute)
      throws HopException {
    return runWithFilenames(meta, copies, files, distribute, FILENAME_FIELD);
  }

  /** As above, reading the filename from the given field of the filename rows. */
  static Result runWithFilenames(
      TextFileInputMeta meta,
      int copies,
      List<Path> files,
      boolean distribute,
      String filenameField)
      throws HopException {
    meta.getFileInput().setAcceptingFilenames(true);
    meta.getFileInput().setAcceptingTransformName(FILENAMES);
    meta.getFileInput().setAcceptingField(filenameField);
    return execute(meta, copies, files, distribute);
  }

  private static Result execute(TextFileInputMeta meta, int copies, List<Path> filenames)
      throws HopException {
    return execute(meta, copies, filenames, true);
  }

  private static Result execute(
      TextFileInputMeta meta, int copies, List<Path> filenames, boolean distribute)
      throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("Text file input harness");
    pipelineMeta.setMetadataProvider(new MemoryMetadataProvider());

    TransformMeta input = new TransformMeta(TRANSFORM, meta);
    input.setCopies(copies);
    pipelineMeta.addTransform(input);

    if (filenames != null) {
      InjectorMeta injectorMeta = new InjectorMeta();
      injectorMeta.getInjectorFields().add(new InjectorField(FILENAME_FIELD, "String", "-1", "-1"));
      injectorMeta.getInjectorFields().add(new InjectorField(TAG_FIELD, "String", "-1", "-1"));
      TransformMeta injector = new TransformMeta(FILENAMES, injectorMeta);
      injector.setDistributes(distribute);
      pipelineMeta.addTransform(injector);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, input));
    }

    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setLogLevel(LogLevel.ERROR);
    List<List<Object[]>> rowsByCopy = new ArrayList<>();
    IRowMeta[] rowMeta = new IRowMeta[1];
    try {
      pipeline.prepareExecution();
    } catch (HopException e) {
      return Result.failedToStart(log(pipeline), e);
    }
    for (int copy = 0; copy < copies; copy++) {
      List<Object[]> rows = Collections.synchronizedList(new ArrayList<>());
      rowsByCopy.add(rows);
      pipeline
          .getTransform(TRANSFORM, copy)
          .addRowListener(
              new RowAdapter() {
                @Override
                public void rowWrittenEvent(IRowMeta meta, Object[] row)
                    throws HopTransformException {
                  synchronized (rowMeta) {
                    if (rowMeta[0] == null) {
                      rowMeta[0] = meta.clone();
                    }
                  }
                  rows.add(row.clone());
                }
              });
    }
    RowProducer producer = filenames != null ? pipeline.addRowProducer(FILENAMES, 0) : null;
    pipeline.startThreads();
    if (producer != null) {
      IRowMeta filenameMeta = new RowMeta();
      filenameMeta.addValueMeta(new ValueMetaString(FILENAME_FIELD));
      filenameMeta.addValueMeta(new ValueMetaString(TAG_FIELD));
      for (int i = 0; i < filenames.size(); i++) {
        Path file = filenames.get(i);
        producer.putRow(filenameMeta, new Object[] {file.toString(), file.getFileName() + "-" + i});
      }
      producer.finished();
    }
    pipeline.waitUntilFinished();
    return new Result(rowMeta[0], rowsByCopy, pipeline.getErrors(), log(pipeline), null);
  }

  private static String log(LocalPipelineEngine pipeline) {
    return HopLogStore.getAppender().getBuffer(pipeline.getLogChannelId(), false).toString();
  }

  /** What one run produced. */
  record Result(
      IRowMeta rowMeta,
      List<List<Object[]>> rowsByCopy,
      long errors,
      String log,
      HopException startFailure) {

    static Result failedToStart(String log, HopException failure) {
      return new Result(null, List.of(), 1, log, failure);
    }

    /** All rows, copy 0 first, each copy in the order it wrote them. */
    List<Object[]> rows() {
      List<Object[]> all = new ArrayList<>();
      rowsByCopy.forEach(all::addAll);
      return all;
    }

    /** Each row as {@code value|value|...}, nulls as {@code <null>}. */
    List<String> lines() {
      return lines(rows());
    }

    List<String> linesOfCopy(int copy) {
      return lines(rowsByCopy.get(copy));
    }

    private List<String> lines(List<Object[]> rows) {
      List<String> lines = new ArrayList<>();
      for (Object[] row : rows) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < rowMeta.size(); i++) {
          if (i > 0) {
            line.append('|');
          }
          try {
            String value = rowMeta.getString(row, i);
            line.append(value == null ? "<null>" : value);
          } catch (HopException e) {
            throw new IllegalStateException(e);
          }
        }
        lines.add(line.toString());
      }
      return lines;
    }
  }
}
