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
package org.apache.hop.pipeline;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.ITransformMetaChangeListener;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class PipelineMetaTest {
  public static final String TRANSFORM_NAME = "Any transform name";

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private PipelineMeta pipelineMeta;
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;

  @Before
  public void setUp() throws Exception {
    pipelineMeta = new PipelineMeta();
    variables = new Variables();
    metadataProvider = new MemoryMetadataProvider();
  }

  @Test
  public void testGetMinimum() {
    final Point minimalCanvasPoint = new Point(0, 0);

    // for test goal should content coordinate more than NotePadMetaPoint
    final Point transformPoint = new Point(500, 500);

    // empty Pipeline return 0 coordinate point
    Point point = pipelineMeta.getMinimum();
    assertEquals(minimalCanvasPoint.x, point.x);
    assertEquals(minimalCanvasPoint.y, point.y);

    // when Pipeline  content Transform  than pipeline should return minimal coordinate of transform
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getLocation()).thenReturn(transformPoint);
    pipelineMeta.addTransform(transformMeta);
    Point actualTransformPoint = pipelineMeta.getMinimum();
    assertEquals(transformPoint.x - PipelineMeta.BORDER_INDENT, actualTransformPoint.x);
    assertEquals(transformPoint.y - PipelineMeta.BORDER_INDENT, actualTransformPoint.y);
  }

  @Test
  public void testContentChangeListener() throws Exception {
    IContentChangedListener listener = mock(IContentChangedListener.class);
    pipelineMeta.addContentChangedListener(listener);

    pipelineMeta.setChanged();
    pipelineMeta.setChanged(true);

    verify(listener, times(2)).contentChanged(same(pipelineMeta));

    pipelineMeta.clearChanged();
    pipelineMeta.setChanged(false);

    verify(listener, times(2)).contentSafe(same(pipelineMeta));

    pipelineMeta.removeContentChangedListener(listener);
    pipelineMeta.setChanged();
    pipelineMeta.setChanged(true);

    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testCompare() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setNameSynchronizedWithFilename(false);
    pipelineMeta.setFilename("aFile");
    pipelineMeta.setName("aName");
    PipelineMeta pipelineMeta2 = new PipelineMeta();
    pipelineMeta2.setNameSynchronizedWithFilename(false);
    pipelineMeta2.setFilename("aFile");
    pipelineMeta2.setName("aName");
    assertEquals(0, pipelineMeta.compare(pipelineMeta, pipelineMeta2));
    pipelineMeta2.setFilename(null);
    assertEquals(1, pipelineMeta.compare(pipelineMeta, pipelineMeta2));
    assertEquals(-1, pipelineMeta.compare(pipelineMeta2, pipelineMeta));
    pipelineMeta2.setFilename("aFile");
    pipelineMeta2.setName(null);
    assertEquals(1, pipelineMeta.compare(pipelineMeta, pipelineMeta2));
    assertEquals(-1, pipelineMeta.compare(pipelineMeta2, pipelineMeta));
    pipelineMeta2.setFilename("aFile2");
    pipelineMeta2.setName("aName");
    assertEquals(-1, pipelineMeta.compare(pipelineMeta, pipelineMeta2));
    assertEquals(1, pipelineMeta.compare(pipelineMeta2, pipelineMeta));
    pipelineMeta2.setFilename("aFile");
    pipelineMeta2.setName("aName2");
    assertEquals(-1, pipelineMeta.compare(pipelineMeta, pipelineMeta2));
    assertEquals(1, pipelineMeta.compare(pipelineMeta2, pipelineMeta));
    pipelineMeta.setFilename(null);
    pipelineMeta2.setFilename(null);
    pipelineMeta2.setName("aName");
    assertEquals(0, pipelineMeta.compare(pipelineMeta, pipelineMeta2));
  }

  @Test
  public void testEquals() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename("1");
    pipelineMeta.setName("2");
    assertFalse(pipelineMeta.equals("somethingelse"));
    PipelineMeta pipelineMeta2 = new PipelineMeta();
    pipelineMeta2.setFilename("1");
    pipelineMeta2.setName("2");
    assertTrue(pipelineMeta.equals(pipelineMeta2));
  }

  @Test
  public void testPipelineHops() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename("pipelineFile");
    pipelineMeta.setName("myPipeline");
    TransformMeta transform1 = new TransformMeta("name1", null);
    TransformMeta transform2 = new TransformMeta("name2", null);
    TransformMeta transform3 = new TransformMeta("name3", null);
    TransformMeta transform4 = new TransformMeta("name4", null);
    PipelineHopMeta hopMeta1 = new PipelineHopMeta(transform1, transform2, true);
    PipelineHopMeta hopMeta2 = new PipelineHopMeta(transform2, transform3, true);
    PipelineHopMeta hopMeta3 = new PipelineHopMeta(transform3, transform4, false);
    pipelineMeta.addPipelineHop(0, hopMeta1);
    pipelineMeta.addPipelineHop(1, hopMeta2);
    pipelineMeta.addPipelineHop(2, hopMeta3);
    List<TransformMeta> hops = pipelineMeta.getPipelineHopTransforms(true);
    assertSame(transform1, hops.get(0));
    assertSame(transform2, hops.get(1));
    assertSame(transform3, hops.get(2));
    assertSame(transform4, hops.get(3));
    assertEquals(hopMeta2, pipelineMeta.findPipelineHop("name2 --> name3 (enabled)"));
    assertEquals(hopMeta3, pipelineMeta.findPipelineHopFrom(transform3));
    assertEquals(hopMeta2, pipelineMeta.findPipelineHop(hopMeta2));
    assertEquals(hopMeta1, pipelineMeta.findPipelineHop(transform1, transform2));
    assertEquals(null, pipelineMeta.findPipelineHop(transform3, transform4, false));
    assertEquals(hopMeta3, pipelineMeta.findPipelineHop(transform3, transform4, true));
    assertEquals(hopMeta2, pipelineMeta.findPipelineHopTo(transform3));
    pipelineMeta.removePipelineHop(0);
    hops = pipelineMeta.getPipelineHopTransforms(true);
    assertSame(transform2, hops.get(0));
    assertSame(transform3, hops.get(1));
    assertSame(transform4, hops.get(2));
    pipelineMeta.removePipelineHop(hopMeta2);
    hops = pipelineMeta.getPipelineHopTransforms(true);
    assertSame(transform3, hops.get(0));
    assertSame(transform4, hops.get(1));
  }

  @Test
  public void testGetAllPipelineHops() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename("pipelineFile");
    pipelineMeta.setName("myPipeline");
    TransformMeta transform1 = new TransformMeta("name1", null);
    TransformMeta transform2 = new TransformMeta("name2", null);
    TransformMeta transform3 = new TransformMeta("name3", null);
    TransformMeta transform4 = new TransformMeta("name4", null);
    PipelineHopMeta hopMeta1 = new PipelineHopMeta(transform1, transform2, true);
    PipelineHopMeta hopMeta2 = new PipelineHopMeta(transform2, transform3, true);
    PipelineHopMeta hopMeta3 = new PipelineHopMeta(transform2, transform4, true);
    pipelineMeta.addPipelineHop(0, hopMeta1);
    pipelineMeta.addPipelineHop(1, hopMeta2);
    pipelineMeta.addPipelineHop(2, hopMeta3);
    List<PipelineHopMeta> allPipelineHopFrom = pipelineMeta.findAllPipelineHopFrom(transform2);
    assertEquals(transform3, allPipelineHopFrom.get(0).getToTransform());
    assertEquals(transform4, allPipelineHopFrom.get(1).getToTransform());
  }

  @Test
  public void testAddTransformWithChangeListenerInterface() {
    TransformMeta transformMeta = mock(TransformMeta.class);
    TransformMetaChangeListenerInterfaceMock metaInterface =
        mock(TransformMetaChangeListenerInterfaceMock.class);
    when(transformMeta.getTransform()).thenReturn(metaInterface);
    assertEquals(0, pipelineMeta.transforms.size());
    assertEquals(0, pipelineMeta.transformChangeListeners.size());
    // should not throw exception if there are no transforms in transform meta
    pipelineMeta.addTransform(0, transformMeta);
    assertEquals(1, pipelineMeta.transforms.size());
    assertEquals(1, pipelineMeta.transformChangeListeners.size());

    pipelineMeta.addTransform(0, transformMeta);
    assertEquals(2, pipelineMeta.transforms.size());
    assertEquals(2, pipelineMeta.transformChangeListeners.size());
  }

  @Test
  public void testIsAnySelectedTransformUsedInPipelineHopsNothingSelectedCase() {
    List<TransformMeta> selectedTransforms =
        asList(new TransformMeta(), new TransformMeta(), new TransformMeta());
    pipelineMeta.getTransforms().addAll(selectedTransforms);

    assertFalse(pipelineMeta.isAnySelectedTransformUsedInPipelineHops());
  }

  @Test
  public void testIsAnySelectedTransformUsedInPipelineHopsAnySelectedCase() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(TRANSFORM_NAME);
    PipelineHopMeta pipelineHopMeta = new PipelineHopMeta();
    transformMeta.setSelected(true);
    List<TransformMeta> selectedTransforms =
        asList(new TransformMeta(), transformMeta, new TransformMeta());

    pipelineHopMeta.setToTransform(transformMeta);
    pipelineHopMeta.setFromTransform(transformMeta);
    pipelineMeta.getTransforms().addAll(selectedTransforms);
    pipelineMeta.addPipelineHop(pipelineHopMeta);

    assertTrue(pipelineMeta.isAnySelectedTransformUsedInPipelineHops());
  }

  @Test
  public void testCloneWithParam() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename("pipelineFile");
    pipelineMeta.setName("myPipeline");
    pipelineMeta.addParameterDefinition("key", "defValue", "description");
    Object clone = pipelineMeta.realClone(true);
    assertNotNull(clone);
  }

  private static TransformMeta mockTransformMeta(String name) {
    TransformMeta meta = mock(TransformMeta.class);
    when(meta.getName()).thenReturn(name);
    return meta;
  }

  public abstract static class TransformMetaChangeListenerInterfaceMock
      implements ITransformMeta, ITransformMetaChangeListener {
    @Override
    public abstract Object clone();
  }

  @Test
  public void testLoadXml() throws HopException {
    String directory = "/home/admin";
    Node workflowNode = Mockito.mock(Node.class);
    NodeList nodeList =
        new NodeList() {
          ArrayList<Node> nodes = new ArrayList<>();

          {
            Node nodeInfo = Mockito.mock(Node.class);
            Mockito.when(nodeInfo.getNodeName()).thenReturn(PipelineMeta.XML_TAG_INFO);
            Mockito.when(nodeInfo.getChildNodes()).thenReturn(this);

            Node nodeDirectory = Mockito.mock(Node.class);
            Mockito.when(nodeDirectory.getNodeName()).thenReturn("directory");
            Node child = Mockito.mock(Node.class);
            Mockito.when(nodeDirectory.getFirstChild()).thenReturn(child);
            Mockito.when(child.getNodeValue()).thenReturn(directory);

            nodes.add(nodeDirectory);
            nodes.add(nodeInfo);
          }

          @Override
          public Node item(int index) {
            return nodes.get(index);
          }

          @Override
          public int getLength() {
            return nodes.size();
          }
        };

    Mockito.when(workflowNode.getChildNodes()).thenReturn(nodeList);

    PipelineMeta meta = new PipelineMeta();

    IVariables variables = Mockito.mock(IVariables.class);
    Mockito.when(variables.getVariableNames()).thenReturn(new String[0]);

    meta.loadXml(workflowNode, null, metadataProvider, variables);
    meta.setInternalHopVariables(variables);
  }

  @Test
  public void infoTransformFieldsAreNotIncludedInGetTransformFields() throws HopTransformException {
    // validates that the fields from info transforms are not included in the resulting transform
    // fields for a transformMeta.
    //  This is important with transforms like StreamLookup and Append, where the previous
    // transforms may or may not
    //  have their fields included in the current transform.

    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta toBeAppended1 =
        testTransform(
            "toBeAppended1",
            emptyList(), // no info transforms
            asList("field1", "field2") // names of fields from this transform
            );
    TransformMeta toBeAppended2 =
        testTransform("toBeAppended2", emptyList(), asList("field1", "field2"));

    TransformMeta append =
        testTransform(
            "append",
            asList("toBeAppended1", "toBeAppended2"), // info transform names
            singletonList("outputField") // output field of this transform
            );
    TransformMeta after = new TransformMeta("after", new DummyMeta());

    wireUpTestPipelineMeta(pipelineMeta, toBeAppended1, toBeAppended2, append, after);

    IRowMeta results =
        pipelineMeta.getTransformFields(variables, append, after, mock(IProgressMonitor.class));

    assertThat(1, equalTo(results.size()));
    assertThat("outputField", equalTo(results.getFieldNames()[0]));
  }

  @Test
  public void prevTransformFieldsAreIncludedInGetTransformFields() throws HopTransformException {

    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta prevTransform1 =
        testTransform("prevTransform1", emptyList(), asList("field1", "field2"));
    TransformMeta prevTransform2 =
        testTransform("prevTransform2", emptyList(), asList("field3", "field4", "field5"));

    TransformMeta someTransform =
        testTransform("transform", asList("prevTransform1"), asList("outputField"));

    TransformMeta after = new TransformMeta("after", new DummyMeta());

    wireUpTestPipelineMeta(pipelineMeta, prevTransform1, prevTransform2, someTransform, after);

    IRowMeta results =
        pipelineMeta.getTransformFields(
            variables, someTransform, after, mock(IProgressMonitor.class));

    assertThat(4, equalTo(results.size()));
    assertThat(
        new String[] {"field3", "field4", "field5", "outputField"},
        equalTo(results.getFieldNames()));
  }

  @Test
  public void findPreviousTransformsNullMeta() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    List<TransformMeta> result = pipelineMeta.findPreviousTransforms(null, false);

    assertThat(0, equalTo(result.size()));
    assertThat(result, equalTo(new ArrayList<>()));
  }

  private void wireUpTestPipelineMeta(
      PipelineMeta pipelineMeta,
      TransformMeta toBeAppended1,
      TransformMeta toBeAppended2,
      TransformMeta append,
      TransformMeta after) {
    pipelineMeta.addTransform(append);
    pipelineMeta.addTransform(after);
    pipelineMeta.addTransform(toBeAppended1);
    pipelineMeta.addTransform(toBeAppended2);

    pipelineMeta.addPipelineHop(new PipelineHopMeta(toBeAppended1, append));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(toBeAppended2, append));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(append, after));
  }

  private TransformMeta testTransform(
      String name, List<String> infoTransformNames, List<String> fieldNames)
      throws HopTransformException {
    ITransformMeta smi =
        transformMetaInterfaceWithFields(new DummyMeta(), infoTransformNames, fieldNames);
    return new TransformMeta(name, smi);
  }

  private ITransformMeta transformMetaInterfaceWithFields(
      ITransformMeta smi, List<String> infoTransformNames, List<String> fieldNames)
      throws HopTransformException {
    RowMeta rowMetaWithFields = new RowMeta();
    TransformIOMeta transformIOMeta = mock(TransformIOMeta.class);
    when(transformIOMeta.getInfoTransformNames())
        .thenReturn(infoTransformNames.toArray(new String[0]));
    fieldNames.stream()
        .forEach(field -> rowMetaWithFields.addValueMeta(new ValueMetaString(field)));
    ITransformMeta newSmi = spy(smi);
    when(newSmi.getTransformIOMeta()).thenReturn(transformIOMeta);

    doAnswer(
            (Answer<Void>)
                invocationOnMock -> {
                  IRowMeta passedRmi = (IRowMeta) invocationOnMock.getArguments()[0];
                  passedRmi.addRowMeta(rowMetaWithFields);
                  return null;
                })
        .when(newSmi)
        .getFields(any(), any(), any(), any(), any(), any());

    return newSmi;
  }

  private TransformMeta createTransformMeta(String name) {
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getName()).thenReturn(name);
    return transformMeta;
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    PipelineMeta pipelineMetaTest = new PipelineMeta();
    pipelineMetaTest.setFilename("hasFilename");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory");
    pipelineMetaTest.setInternalEntryCurrentDirectory(variables);

    assertEquals(
        "file:///C:/SomeFilenameDirectory",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    PipelineMeta pipelineMetaTest = new PipelineMeta();
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory");
    pipelineMetaTest.setInternalEntryCurrentDirectory(variables);

    assertEquals(
        "Original value defined at run execution",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  public void testSerialization1() throws Exception {
    pipelineMeta.setName("testSerialization1");
    pipelineMeta.setDescription("description of testSerialization1");
    pipelineMeta.setExtendedDescription("extended description of testSerialization1");
    pipelineMeta.addNote(new NotePadMeta("Test note", 50, 50, 300, 20));

    TransformMeta one = new TransformMeta("one", new DummyMeta());
    one.setLocation(100, 200);
    TransformMeta two = new TransformMeta("two", new DummyMeta());
    one.setLocation(200, 200);

    pipelineMeta.addTransform(one);
    pipelineMeta.addTransform(two);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(one, two));

    String xml = pipelineMeta.getXml(variables);

    // Re-inflate from XML
    //
    PipelineMeta copy =
        new PipelineMeta(XmlHandler.loadXmlString(xml, PipelineMeta.XML_TAG), metadataProvider);

    assertEquals(xml, copy.getXml(variables));
  }
}
