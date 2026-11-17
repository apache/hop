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

package org.apache.hop.pipeline.transforms.csvinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CSV file input reading the file a row names, run by the {@link SingleThreadedPipelineExecutor}
 * for one iteration, as the single-threaded engine runs it. The transform reads one line per call,
 * so it needs calls after its input has ended.
 */
class CsvInputSingleThreadedTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  @TempDir Path folder;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta :
        List.of(new CsvInputMeta(), new DummyMeta(), new FileNameSourceMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  @Test
  void theFileARowNamesIsReadToTheEnd() throws Exception {
    Path file = folder.resolve("orders.csv");
    StringBuilder text = new StringBuilder("id;name\n");
    for (int id = 1; id <= 5; id++) {
      text.append(id).append(";name ").append(id).append('\n');
    }
    Files.writeString(file, text.toString());

    List<Object[]> rows = new ArrayList<>();
    IRowMeta[] rowMeta = new IRowMeta[1];
    assertTimeoutPreemptively(TIMEOUT, () -> readSingleThreaded(file.toString(), rows, rowMeta));

    assertEquals(5, rows.size());
    int id = rowMeta[0].indexOfValue("id");
    assertEquals(5L, rowMeta[0].getInteger(rows.get(4), id));
  }

  private static void readSingleThreaded(String filename, List<Object[]> rows, IRowMeta[] rowMeta)
      throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("csv-input-single-threaded");

    CsvInputMeta csvInputMeta = new CsvInputMeta();
    csvInputMeta.setFilenameField("filename");
    csvInputMeta.setDelimiter(";");
    csvInputMeta.setHeaderPresent(true);
    CsvInputField idField = new CsvInputField("id");
    idField.setType(IValueMeta.TYPE_INTEGER);
    idField.setFormat("#");
    csvInputMeta.getInputFields().add(idField);
    csvInputMeta.getInputFields().add(new CsvInputField("name"));

    FileNameSourceMeta fileNameSourceMeta = new FileNameSourceMeta();
    fileNameSourceMeta.filename = filename;
    TransformMeta files = addTransform(pipelineMeta, "files", fileNameSourceMeta);
    TransformMeta csv = addTransform(pipelineMeta, "csv", csvInputMeta);
    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());
    pipelineMeta.addPipelineHop(new PipelineHopMeta(files, csv));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(csv, output));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipeline.prepareExecution();

    ITransform csvTransform = pipeline.getTransform("csv", 0);
    csvTransform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta meta, Object[] row) {
            rowMeta[0] = meta;
            rows.add(row);
          }
        });

    pipeline.startThreads();

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
    assertTrue(executor.init());
    try {
      // One iteration, as the single-threaded engine runs
      executor.oneIteration();
    } finally {
      executor.dispose();
    }
    assertEquals(0, csvTransform.getErrors());
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }

  /** A source giving one row with a file name, all in its first call, as a Data grid does. */
  @Transform(id = "CsvInputSingleThreadedTestFileName", name = "File name source")
  public static class FileNameSourceMeta
      extends BaseTransformMeta<FileNameSource, FileNameSourceData> {
    String filename;

    @Override
    public void getFields(
        IRowMeta rowMeta,
        String origin,
        IRowMeta[] info,
        TransformMeta nextTransform,
        IVariables variables,
        IHopMetadataProvider metadataProvider) {
      rowMeta.addValueMeta(new ValueMetaString("filename"));
    }
  }

  public static class FileNameSourceData extends BaseTransformData implements ITransformData {}

  public static class FileNameSource extends BaseTransform<FileNameSourceMeta, FileNameSourceData> {
    public FileNameSource(
        TransformMeta transformMeta,
        FileNameSourceMeta meta,
        FileNameSourceData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public boolean processRow() throws HopException {
      IRowMeta rowMeta = new RowMeta();
      meta.getFields(rowMeta, getTransformName(), null, null, this, null);
      putRow(rowMeta, new Object[] {meta.filename});
      setOutputDone();
      return false;
    }
  }
}
