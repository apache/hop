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

package org.apache.hop.pipeline.transforms.checksum;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CheckSumTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static Object previousHopDefaultNumberFormat;

  @BeforeClass
  public static void setUpBeforeClass()
      throws HopException, NoSuchFieldException, IllegalAccessException {
    System.setProperty("file.encoding", "UTF-8");
    previousHopDefaultNumberFormat =
        System.getProperties().put(Const.HOP_DEFAULT_NUMBER_FORMAT, "0.0;-0.0");
    java.lang.reflect.Field charset = Charset.class.getDeclaredField("defaultCharset");
    charset.setAccessible(true);
    charset.set(null, null);
    HopEnvironment.init();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    if (previousHopDefaultNumberFormat == null) {
      System.getProperties().remove(Const.HOP_DEFAULT_NUMBER_FORMAT);
    } else {
      System.getProperties().put(Const.HOP_DEFAULT_NUMBER_FORMAT, previousHopDefaultNumberFormat);
    }
  }

  private Pipeline buildHexadecimalChecksumPipeline(CheckSumMeta.CheckSumType checkSumType)
      throws Exception {
    // Create a new pipeline...
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(getClass().getName());

    // Create a CheckSum Transform
    String checkSumTransformName = "CheckSum";
    CheckSumMeta meta = new CheckSumMeta();

    // Set required fields
    meta.setResultFieldName("hex");
    meta.setCheckSumType(checkSumType);
    meta.setResultType(CheckSumMeta.ResultType.HEXADECIMAL);
    meta.setFields(Arrays.asList(new Field("test")));

    String checkSumPluginPid =
        PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta checkSumTransform =
        new TransformMeta(checkSumPluginPid, checkSumTransformName, meta);
    pipelineMeta.addTransform(checkSumTransform);

    // Create a Dummy transform
    String dummyTransformName = "Output";
    DummyMeta dummyMeta = new DummyMeta();
    String dummyTransformPid =
        PluginRegistry.getInstance().getPluginId(TransformPluginType.class, dummyMeta);
    TransformMeta dummyTransform =
        new TransformMeta(dummyTransformPid, dummyTransformName, dummyMeta);
    pipelineMeta.addTransform(dummyTransform);

    // Create a hop from CheckSum to Output
    PipelineHopMeta hop = new PipelineHopMeta(checkSumTransform, dummyTransform);
    pipelineMeta.addPipelineHop(hop);

    return new LocalPipelineEngine(pipelineMeta);
  }

  private RowMeta createStringRowMeta(IValueMeta meta) throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(meta);
    return rowMeta;
  }

  private class MockRowListener extends RowAdapter {
    private List<Object[]> written;

    private List<Object[]> read;

    private List<Object[]> error;

    public MockRowListener() {
      written = new ArrayList<>();
      read = new ArrayList<>();
      error = new ArrayList<>();
    }

    public List<Object[]> getWritten() {
      return written;
    }

    @Override
    public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      written.add(row);
    }

    @Override
    public void rowReadEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      read.add(row);
    }

    @Override
    public void errorRowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      error.add(row);
    }
  }

  /**
   * Create, execute, and return the row listener attached to the output transform with complete
   * results from the execution.
   *
   * @param checkSumType Type of checksum to use
   * @param input String to calculate checksum for
   * @param meta meta to be used
   * @return IRowListener with results.
   */
  private MockRowListener executeHexTest(
      CheckSumMeta.CheckSumType checkSumType, Object input, IValueMeta meta) throws Exception {
    Pipeline pipeline = buildHexadecimalChecksumPipeline(checkSumType);

    pipeline.prepareExecution();

    ITransform output = pipeline.getRunThread("Output", 0);
    MockRowListener listener = new MockRowListener();
    output.addRowListener(listener);

    RowProducer rp = pipeline.addRowProducer("CheckSum", 0);
    RowMeta inputRowMeta = createStringRowMeta(meta);
    ((BaseTransform) pipeline.getRunThread("CheckSum", 0)).setInputRowMeta(inputRowMeta);

    pipeline.startThreads();

    rp.putRow(inputRowMeta, new Object[] {input});
    rp.finished();

    pipeline.waitUntilFinished();
    pipeline.stopAll();
    pipeline.cleanup();
    return listener;
  }

  @Test
  public void testHexOutput_md5() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.MD5, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("d16fb36f0911f878998c136191af705e", results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.MD5, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("372df98e33ac1bf6b26d225361ba7eb5", results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.MD5, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("68b142f87143c917f29d178aa1715957", results.getWritten().get(0)[1]);

    byte[] input = IOUtils.toByteArray(getFile("/checksum.svg").getContent().getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.MD5, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("056c290e3639d0f15db479810973d601", results.getWritten().get(0)[1]);
  }

  @Test
  public void testHexOutput_sha1() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.SHA1, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("66b27417d37e024c46526c2f6d358a754fc552f3", results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA1, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("78aef53da0b8d7a80656c80aa35ad6d410b7f068", results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA1, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("749f3d4c2db67c9f3186563a72ef5da9461f0496", results.getWritten().get(0)[1]);

    byte[] input = IOUtils.toByteArray(getFile("/checksum.svg").getContent().getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.SHA1, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals("801a72c0d7551fba06b880e7976c9e477f57d00c", results.getWritten().get(0)[1]);
  }

  @Test
  public void testHexOutput_sha256() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.SHA256, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "3608bca1e44ea6c4d268eb6db02260269892c0b42b86bbf1e77a6fa16c3c9282",
        results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA256, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "b52b603f9ec86c382a8483cad4f788f2f927535a76ad1388caedcef5e3c3c813",
        results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA256, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "45cbb96ff9625490cd675a7a39fecad6c167c1ed9b8957f53224fcb3e4a1e4a1",
        results.getWritten().get(0)[1]);

    byte[] input =
        IOUtils.toByteArray(
            getFile("/org/apache/hop/pipeline/transforms/loadfileinput/files/hop.jpg")
                .getContent()
                .getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.SHA256, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "51165cf63a5b08470272cbf75f6bfb439fad977a451866a25b5ebb3767f31872",
        results.getWritten().get(0)[1]);
  }

  @Test
  public void testHexOutput_sha384() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.SHA384, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "edcb0f4721e6578d900e4c24ad4b19e194ab6c87f8243bfc6b11754dd8b0bbde4f30b1d18197932b6376da004dcd97c4",
        results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA384, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "bf42c2b293b7562deca2acccc99f85b33aa150603608d610495dc45e0fb55b60c808ce466213edcf6ca184d97305b20d",
        results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA384, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "96adb3d911a5b02f7604b9f8159d5baf21a6719162887792b7232f91fe19fefeaf9438dc1e09685a33c998897a7e76e2",
        results.getWritten().get(0)[1]);

    byte[] input = IOUtils.toByteArray(getFile("/checksum.svg").getContent().getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.SHA384, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "ef97e467e4fc1894abcdfd589481ff9c269322f7f2f42fde41468d7ac1f2bcd10db6f797f58e531419c886a43d1d8cd0",
        results.getWritten().get(0)[1]);
  }

  @Test
  public void testHexOutput_sha512() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.SHA512, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "4a3ed8147e37876adc8f76328e5abcc1b470e6acfc18efea0135f983604953a58e183c1a6086e91ba3e821d926f5fdeb37761c7ca0328a963f5e92870675b728",
        results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA512, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "74556b99e709669b45e2079c4d760a81fd2a8dddfc5ca4762af63ce502b569c65bf6fa066a37ac205f4537df4eacdb9081783e101765c65581d1afef83e19447",
        results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.SHA512, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "29a7264df217dfe5ef7bef68888376e355199cfc9f30d488281ed7710ca2190b7aa311336bfa4440b7c7202d5a5d67c7dd521915476c3f6d48ecfacc637296bf",
        results.getWritten().get(0)[1]);

    byte[] input = IOUtils.toByteArray(getFile("/checksum.svg").getContent().getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.SHA512, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(
        "bbd4c05a92d9b541b653f5135d7076d722631394616b142e5655f075fb2f75166a2dfcf488bf3d6519c8eaf0a7eacaf7d81224ffd5a7e4cc4f126369840e239f",
        results.getWritten().get(0)[1]);
  }

  @Test
  public void testHexOutput_adler32() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.ADLER32, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("47645036"), results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.ADLER32, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("32243912"), results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.ADLER32, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("48627962"), results.getWritten().get(0)[1]);

    byte[] input = IOUtils.toByteArray(getFile("/checksum.svg").getContent().getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.ADLER32, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("3864809454"), results.getWritten().get(0)[1]);
  }

  @Test
  public void testHexOutputCrc32() throws Exception {
    MockRowListener results =
        executeHexTest(CheckSumMeta.CheckSumType.CRC32, "xyz", new ValueMetaString("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("3951999591"), results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.CRC32, 10.8, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("1857885434"), results.getWritten().get(0)[1]);

    results = executeHexTest(CheckSumMeta.CheckSumType.CRC32, 10.82, new ValueMetaNumber("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("1205016603"), results.getWritten().get(0)[1]);

    byte[] input = IOUtils.toByteArray(getFile("/checksum.svg").getContent().getInputStream());
    results = executeHexTest(CheckSumMeta.CheckSumType.CRC32, input, new ValueMetaBinary("test"));
    assertEquals(1, results.getWritten().size());
    assertEquals(Long.valueOf("790584415"), results.getWritten().get(0)[1]);
  }

  private FileObject getFile(final String filepath) {
    try {
      return VFS.getManager().resolveFile(this.getClass().getResource(filepath));
    } catch (Exception e) {
      throw new RuntimeException("fail. " + e.getMessage(), e);
    }
  }
}
