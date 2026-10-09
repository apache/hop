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

package org.apache.hop.pipeline.transforms.cubeoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cubeinput.CubeInputMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Several cube files written and read by transform copies, and by a filename field. */
class CubeParallelFileTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void twoCopiesWriteAndReadTheirOwnFiles() throws Exception {
    Path base = tempDir.resolve("data.cube");
    writeTwoValues(base);

    List<String> written = new ArrayList<>();
    written.addAll(readValues(tempDir.resolve("data_0.cube")));
    written.addAll(readValues(tempDir.resolve("data_1.cube")));
    assertEquals(Set.of("one", "two"), new HashSet<>(written));
    assertEquals(2, written.size());

    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setDefault();
    inputMeta.setIncludeTransformNr(true);
    inputMeta.getFile().setName(base.toString());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("read copies");
    TransformMeta input = new TransformMeta("read", inputMeta);
    input.setCopies(2);
    pipelineMeta.addTransform(input);

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("read copies"));
    pipeline.prepareExecution();
    List<String> read = new CopyOnWriteArrayList<>();
    for (int copy = 0; copy < 2; copy++) {
      pipeline
          .getTransform("read", copy)
          .addRowListener(
              new RowAdapter() {
                @Override
                public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                  read.add((String) row[0]);
                }
              });
    }
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors(), "the read reported errors");
    assertEquals(Set.of("one", "two"), new HashSet<>(read));
    assertEquals(2, read.size());
  }

  @Test
  void filenamesFromAFieldAreReadInOrder() throws Exception {
    Path first = tempDir.resolve("alpha.cube");
    Path second = tempDir.resolve("beta.cube");
    writeCube(first, "value", "alpha");
    writeCube(second, "value", "beta");

    List<String> read = runFilenameField(first, List.of(first, second));

    assertEquals(List.of("alpha", "beta"), read);
  }

  @Test
  void aDifferentLayoutFailsAndNamesTheFile() throws Exception {
    Path first = tempDir.resolve("same.cube");
    Path different = tempDir.resolve("different.cube");
    writeCube(first, "value", "alpha");
    writeCube(different, "other", "beta");

    InjectorMeta injectorMeta = new InjectorMeta();
    injectorMeta.getInjectorFields().add(new InjectorField("filename", "String", "-1", "-1"));
    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setDefault();
    inputMeta.setFilenameInField(true);
    inputMeta.setFilenameField("filename");
    inputMeta.getFile().setName(first.toString());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("mismatched cubes");
    TransformMeta injector = new TransformMeta("injector", injectorMeta);
    TransformMeta input = new TransformMeta("read", inputMeta);
    pipelineMeta.addTransform(injector);
    pipelineMeta.addTransform(input);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, input));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("mismatch"));
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("injector", 0);
    pipeline.startThreads();
    IRowMeta names = filenameRowMeta();
    producer.putRow(names, new Object[] {first.toString()});
    producer.putRow(names, new Object[] {different.toString()});
    producer.finished();
    pipeline.waitUntilFinished();

    assertTrue(pipeline.getErrors() > 0, "a different layout must fail the transform");
    String log =
        HopLogStore.getAppender()
            .getBuffer(pipeline.getTransform("read", 0).getLogChannel().getLogChannelId(), false)
            .toString();
    assertTrue(log.contains(different.getFileName().toString()), log);
    assertTrue(log.contains(first.getFileName().toString()), log);
  }

  private void writeTwoValues(Path base) throws Exception {
    InjectorMeta injectorMeta = new InjectorMeta();
    injectorMeta.getInjectorFields().add(new InjectorField("value", "String", "-1", "-1"));
    CubeOutputMeta outputMeta = new CubeOutputMeta();
    outputMeta.setDefault();
    outputMeta.setIncludeTransformNr(true);
    outputMeta.setFilename(base.toString());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("write copies");
    TransformMeta injector = new TransformMeta("injector", injectorMeta);
    TransformMeta output = new TransformMeta("write", outputMeta);
    output.setCopies(2);
    pipelineMeta.addTransform(injector);
    pipelineMeta.addTransform(output);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, output));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("write copies"));
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("injector", 0);
    pipeline.startThreads();
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("value"));
    producer.putRow(rowMeta, new Object[] {"one"});
    producer.putRow(rowMeta, new Object[] {"two"});
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors(), "the write reported errors");
  }

  private List<String> runFilenameField(Path sample, List<Path> files) throws Exception {
    InjectorMeta injectorMeta = new InjectorMeta();
    injectorMeta.getInjectorFields().add(new InjectorField("filename", "String", "-1", "-1"));
    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setDefault();
    inputMeta.setFilenameInField(true);
    inputMeta.setFilenameField("filename");
    inputMeta.getFile().setName(sample.toString());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("filenames from a field");
    TransformMeta injector = new TransformMeta("injector", injectorMeta);
    TransformMeta input = new TransformMeta("read", inputMeta);
    pipelineMeta.addTransform(injector);
    pipelineMeta.addTransform(input);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, input));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("field names"));
    pipeline.prepareExecution();
    List<String> read = new CopyOnWriteArrayList<>();
    pipeline
        .getTransform("read", 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                read.add((String) row[0]);
              }
            });
    RowProducer producer = pipeline.addRowProducer("injector", 0);
    pipeline.startThreads();
    IRowMeta names = filenameRowMeta();
    for (Path file : files) {
      producer.putRow(names, new Object[] {file.toString()});
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors(), "reading filenames from a field reported errors");
    return read;
  }

  private static IRowMeta filenameRowMeta() {
    IRowMeta names = new RowMeta();
    names.addValueMeta(new ValueMetaString("filename"));
    return names;
  }

  private static void writeCube(Path file, String fieldName, String value) throws Exception {
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new ValueMetaString(fieldName));
    try (OutputStream os = Files.newOutputStream(file);
        DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(os))) {
      layout.writeMeta(dos);
      layout.writeData(dos, new Object[] {value});
    }
  }

  private static List<String> readValues(Path file) throws Exception {
    List<String> values = new ArrayList<>();
    try (InputStream is = Files.newInputStream(file);
        DataInputStream dis = new DataInputStream(new GZIPInputStream(is))) {
      IRowMeta rowMeta = new RowMeta(dis);
      while (true) {
        try {
          values.add((String) rowMeta.readData(dis)[0]);
        } catch (HopEofException e) {
          return values;
        }
      }
    }
  }
}
