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
package org.apache.hop.pipeline.transforms.xml.xmljoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XmlJoinMetaGetFieldsTest {

  XmlJoinMeta xmlJoinMeta;
  PipelineMeta pipelineMeta;

  @BeforeEach
  void setup() throws Exception {
    HopClientEnvironment.init();
    xmlJoinMeta = new XmlJoinMeta();
    pipelineMeta = mock(PipelineMeta.class);
  }

  @Test
  void testGetFieldsReturnTargetTransformFieldsPlusResultXmlField() throws Exception {
    String sourceXmlTransform = "source xml transform name";
    String sourceTransformField = "source field test name";
    String targetTransformField = "target field test name";
    String resultXmlFieldName = "result xml field name";
    RowMeta rowMetaPreviousTransforms = new RowMeta();
    rowMetaPreviousTransforms.addValueMeta(new ValueMetaString(sourceTransformField));
    xmlJoinMeta.setSourceXmlTransform(sourceXmlTransform);
    xmlJoinMeta.setValueXmlField("result xml field name");
    TransformMeta sourceTransformMeta = new TransformMeta();
    sourceTransformMeta.setName(sourceXmlTransform);

    doReturn(sourceTransformMeta).when(pipelineMeta).findTransform(sourceXmlTransform);
    doReturn(rowMetaPreviousTransforms)
        .when(pipelineMeta)
        .getTransformFields(new Variables(), sourceTransformMeta, null, null);

    RowMeta rowMeta = new RowMeta();
    ValueMetaString keepValueMeta = new ValueMetaString(targetTransformField);
    ValueMetaString removeValueMeta = new ValueMetaString(sourceTransformField);
    rowMeta.addValueMeta(keepValueMeta);
    rowMeta.addValueMeta(removeValueMeta);

    xmlJoinMeta.getFields(
        rowMeta,
        "testTransformName",
        new IRowMeta[] {rowMeta, rowMetaPreviousTransforms},
        null,
        new Variables(),
        null);
    assertEquals(3, rowMeta.size());
    String[] strings = rowMeta.getFieldNames();
    assertEquals(targetTransformField, strings[0]);
    assertEquals(sourceTransformField, strings[1]);
    assertEquals(resultXmlFieldName, strings[2]);
  }

  @Test
  void testGetFieldsReturnTargetTransformFieldsWithDuplicates() throws Exception {
    // Source Transform
    String sourceXmlTransform = "source xml transform name";
    String sourceTransformField1 = "a";
    String sourceTransformField2 = "b";

    // Target Transform
    String targetXmlTransform = "target xml transform name";
    String targetTransformField1 = "b";
    String targetTransformField2 = "c";

    // XML Join Result
    String resultXmlFieldName = "result xml field name";

    // Source Row Meta
    RowMeta rowMetaPreviousSourceTransform = new RowMeta();
    rowMetaPreviousSourceTransform.addValueMeta(new ValueMetaString(sourceTransformField1));
    rowMetaPreviousSourceTransform.addValueMeta(new ValueMetaString(sourceTransformField2));

    // Set source transform in XML Join transform.
    xmlJoinMeta.setSourceXmlTransform(sourceXmlTransform);
    TransformMeta sourceTransformMeta = new TransformMeta();
    sourceTransformMeta.setName(sourceXmlTransform);

    doReturn(sourceTransformMeta).when(pipelineMeta).findTransform(sourceXmlTransform);
    doReturn(rowMetaPreviousSourceTransform)
        .when(pipelineMeta)
        .getTransformFields(new Variables(), sourceTransformMeta, null, null);

    // Target Row Meta
    RowMeta rowMetaPreviousTargetTransform = new RowMeta();
    rowMetaPreviousTargetTransform.addValueMeta(new ValueMetaString(targetTransformField1));
    rowMetaPreviousTargetTransform.addValueMeta(new ValueMetaString(targetTransformField2));

    // Set target transform in XML Join transform.
    xmlJoinMeta.setTargetXmlTransform(targetXmlTransform);
    TransformMeta targetTransformMeta = new TransformMeta();
    targetTransformMeta.setName(targetXmlTransform);

    doReturn(targetTransformMeta).when(pipelineMeta).findTransform(targetXmlTransform);
    doReturn(rowMetaPreviousTargetTransform)
        .when(pipelineMeta)
        .getTransformFields(new Variables(), targetTransformMeta, null, null);

    // Set result field name
    xmlJoinMeta.setValueXmlField(resultXmlFieldName);

    RowMeta rowMeta = new RowMeta();
    ValueMetaString removeValueMeta1 = new ValueMetaString("a");
    rowMeta.addValueMeta(removeValueMeta1);
    ValueMetaString keepValueMeta1 = new ValueMetaString("b");
    rowMeta.addValueMeta(keepValueMeta1);
    ValueMetaString keepValueMeta2 = new ValueMetaString("c");
    rowMeta.addValueMeta(keepValueMeta2);

    // Get output fields
    xmlJoinMeta.getFields(
        rowMeta,
        "testTransformName",
        new IRowMeta[] {rowMetaPreviousTargetTransform, rowMeta},
        null,
        new Variables(),
        null);
    assertEquals(3, rowMeta.size());
    String[] strings = rowMeta.getFieldNames();
    assertEquals("b", strings[0]);
    assertEquals("c", strings[1]);
    assertEquals("result xml field name", strings[2]);
  }

  /**
   * The previous fields are merged in the order of the hops: with the source's hop first, a field
   * both streams have keeps the source's type, and the fields come in the source's order. The
   * transform writes the target's row, so that is what it declares.
   */
  @Test
  void testGetFieldsGiveTheTargetRowWhateverTheOrderOfThePreviousFields() throws Exception {
    RowMeta source = new RowMeta();
    source.addValueMeta(new ValueMetaInteger("c"));
    source.addValueMeta(new ValueMetaString("a"));
    RowMeta target = new RowMeta();
    target.addValueMeta(new ValueMetaString("a"));
    target.addValueMeta(new ValueMetaString("c"));
    // As PipelineMeta merges the fields of the previous transforms, the source's first
    RowMeta rowMeta = source.clone();
    xmlJoinMeta.setValueXmlField("xml");

    xmlJoinMeta.getFields(
        rowMeta, "join", new IRowMeta[] {target, source}, null, new Variables(), null);

    assertEquals(List.of("a", "c", "xml"), List.of(rowMeta.getFieldNames()));
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(1).getType());
  }

  /**
   * At design time Hop gives an empty row for the target stream: the info streams of the XML join
   * have no transform set. The fields are those of the target transform, found by name, plus the
   * XML field, whatever the order of the hops.
   */
  @Test
  void testGetFieldsAtDesignTimeGiveTheTargetRow() throws Exception {
    PipelineMeta designTime = new PipelineMeta();
    TransformMeta target =
        new TransformMeta(
            "Fields",
            "target",
            new DeclaresFields(new ValueMetaString("a"), new ValueMetaString("xml")));
    TransformMeta source =
        new TransformMeta("Fields", "source", new DeclaresFields(new ValueMetaInteger("b")));
    xmlJoinMeta.setTargetXmlTransform("target");
    xmlJoinMeta.setSourceXmlTransform("source");
    xmlJoinMeta.setTargetXmlField("xml");
    xmlJoinMeta.setValueXmlField("out");
    TransformMeta join = new TransformMeta("XMLJoin", "join", xmlJoinMeta);
    designTime.addTransform(target);
    designTime.addTransform(source);
    designTime.addTransform(join);
    // The source's hop first: its fields come first in the merged previous fields
    designTime.addPipelineHop(new PipelineHopMeta(source, join));
    designTime.addPipelineHop(new PipelineHopMeta(target, join));
    xmlJoinMeta.searchInfoAndTargetTransforms(designTime.getTransforms());

    IRowMeta fields = designTime.getTransformFields(new Variables(), join);

    assertEquals(List.of("a", "xml", "out"), List.of(fields.getFieldNames()));
  }

  /** A transform that only declares the fields it is given. */
  private static class DeclaresFields extends BaseTransformMeta {
    private final IValueMeta[] fields;

    DeclaresFields(IValueMeta... fields) {
      this.fields = fields;
    }

    @Override
    public void getFields(
        IRowMeta row,
        String name,
        IRowMeta[] info,
        TransformMeta nextTransform,
        IVariables variables,
        IHopMetadataProvider metadataProvider) {
      for (IValueMeta field : fields) {
        IValueMeta copy = field.clone();
        copy.setOrigin(name);
        row.addValueMeta(copy);
      }
    }
  }
}
