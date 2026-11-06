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

package org.apache.hop.pipeline.transforms.filestoresult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Set files in result in a real pipeline: Injector, then Set files in result, then Dummy. */
class FilesToResultTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String INPUT = "input";
  private static final String FILES_TO_RESULT = "files to result";

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta :
        List.of(new FilesToResultMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  @Test
  void rowsArePassedOnWithOneRowMetaAndEveryFileReachesTheResult() throws Exception {
    List<String> filenames = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Path file = Files.createFile(tempDir.resolve("file-" + i + ".txt"));
      filenames.add(file.toUri().toString());
    }

    List<IRowMeta> passedOn = new ArrayList<>();
    Pipeline pipeline = assertTimeoutPreemptively(TIMEOUT, () -> run(filenames, passedOn));

    assertEquals(5, passedOn.size());
    for (IRowMeta rowMeta : passedOn) {
      assertSame(passedOn.get(0), rowMeta);
    }
    assertEquals(List.of("filename", "other"), List.of(passedOn.get(0).getFieldNames()));

    Set<String> resultFiles = new TreeSet<>();
    for (ResultFile resultFile : pipeline.getResult().getResultFilesList()) {
      resultFiles.add(resultFile.getFile().getName().getBaseName());
    }
    assertEquals(
        Set.of("file-0.txt", "file-1.txt", "file-2.txt", "file-3.txt", "file-4.txt"), resultFiles);
  }

  private static Pipeline run(List<String> filenames, List<IRowMeta> passedOn) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("files-to-result");

    FilesToResultMeta filesToResultMeta = new FilesToResultMeta();
    filesToResultMeta.setDefault();
    filesToResultMeta.setFilenameField("filename");

    TransformMeta input = addTransform(pipelineMeta, INPUT, new InjectorMeta());
    TransformMeta filesToResult = addTransform(pipelineMeta, FILES_TO_RESULT, filesToResultMeta);
    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, filesToResult));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(filesToResult, output));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    pipeline
        .getTransform(FILES_TO_RESULT, 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                passedOn.add(rowMeta);
              }
            });
    RowProducer producer = pipeline.addRowProducer(INPUT, 0);
    pipeline.startThreads();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("filename"));
    rowMeta.addValueMeta(new ValueMetaString("other"));
    for (String filename : filenames) {
      producer.putRow(rowMeta, new Object[] {filename, "x"});
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());
    return pipeline;
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}
