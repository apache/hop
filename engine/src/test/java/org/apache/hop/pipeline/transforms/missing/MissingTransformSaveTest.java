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

package org.apache.hop.pipeline.transforms.missing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.transforms.FakeMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A pipeline referencing a transform plugin which isn't installed must survive a load/save round
 * trip: the {@link Missing} placeholder keeps the XML of the original transform untouched and
 * writes it back out again, so that saving the file doesn't destroy the configuration of that
 * transform for anyone who does have the plugin installed.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class MissingTransformSaveTest {

  private static final String MISSING_PLUGIN_ID = "ThisTransformPluginIsNotInstalled";
  private static final String TRANSFORM_NAME = "Transform with a missing plugin";

  private static final String PIPELINE_XML =
      """
      <pipeline>
        <info>
          <name>missing-plugin-pipeline</name>
        </info>
        <transform>
          <name>Transform with a missing plugin</name>
          <type>ThisTransformPluginIsNotInstalled</type>
          <distribute>Y</distribute>
          <copies>1</copies>
          <connection>my-precious-connection</connection>
          <schema>my-precious-schema</schema>
          <table>my-precious-table</table>
          <commit_size>5000</commit_size>
          <fields>
            <field>
              <column_name>id</column_name>
              <stream_name>id</stream_name>
            </field>
            <field>
              <column_name>name</column_name>
              <stream_name>name</stream_name>
            </field>
          </fields>
          <attributes/>
          <GUI>
            <xloc>240</xloc>
            <yloc>128</yloc>
          </GUI>
        </transform>
      </pipeline>
      """;

  private PipelineMeta loadPipeline(String xml) throws Exception {
    IVariables variables = new Variables();
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    return new PipelineMeta(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        metadataProvider,
        variables);
  }

  /** A pipeline referencing an unknown transform plugin still loads, using a placeholder. */
  @Test
  void unknownTransformPluginLoadsAsMissingPlaceholder() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_XML);

    assertEquals(1, pipelineMeta.nrTransforms());
    TransformMeta transformMeta = pipelineMeta.getTransform(0);

    assertEquals(TRANSFORM_NAME, transformMeta.getName());
    assertTrue(transformMeta.isMissing());
    Missing missing = assertInstanceOf(Missing.class, transformMeta.getTransform());

    assertEquals(MISSING_PLUGIN_ID, missing.getMissingPluginId());
    assertEquals(MISSING_PLUGIN_ID, transformMeta.getTransformPluginId());
  }

  /** The XML of the transform we can't de-serialize is kept, element by element. */
  @Test
  void theOriginalXmlOfTheTransformIsPreserved() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_XML);

    Missing missing = (Missing) pipelineMeta.getTransform(0).getTransform();
    String preserved = String.join("\n", missing.getPreservedXml());

    assertTrue(preserved.contains("<name>" + TRANSFORM_NAME + "</name>"));
    assertTrue(preserved.contains("<type>" + MISSING_PLUGIN_ID + "</type>"));
    assertTrue(preserved.contains("<connection>my-precious-connection</connection>"));
    assertTrue(preserved.contains("<schema>my-precious-schema</schema>"));
    assertTrue(preserved.contains("<table>my-precious-table</table>"));
    assertTrue(preserved.contains("<commit_size>5000</commit_size>"));
    assertTrue(preserved.contains("<column_name>id</column_name>"));
    assertTrue(preserved.contains("<stream_name>name</stream_name>"));
    assertTrue(preserved.contains("<xloc>240</xloc>"));
  }

  /** Saving the pipeline writes the settings of the missing plugin back out. */
  @Test
  void savingAPipelineWithAMissingTransformPluginKeepsItsConfiguration() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_XML);

    String savedXml = pipelineMeta.getXml(new Variables());

    assertTrue(savedXml.contains("<name>" + TRANSFORM_NAME + "</name>"));
    assertTrue(savedXml.contains("<type>" + MISSING_PLUGIN_ID + "</type>"));
    assertTrue(savedXml.contains("<xloc>240</xloc>"));

    // Everything the plugin itself owned is still there.
    assertTrue(savedXml.contains("<connection>my-precious-connection</connection>"));
    assertTrue(savedXml.contains("<schema>my-precious-schema</schema>"));
    assertTrue(savedXml.contains("<table>my-precious-table</table>"));
    assertTrue(savedXml.contains("<commit_size>5000</commit_size>"));
    assertTrue(savedXml.contains("<column_name>id</column_name>"));
    assertTrue(savedXml.contains("<stream_name>name</stream_name>"));
  }

  /**
   * The elements which the pipeline serializes itself are not duplicated: what {@link
   * TransformMeta} writes wins, since that is where renames and moves on the canvas end up.
   */
  @Test
  void theTransformOwnedElementsAreNotDuplicated() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_XML);
    pipelineMeta.getTransform(0).setName("Renamed while the plugin was missing");
    pipelineMeta.getTransform(0).setLocation(500, 600);

    String savedXml = transformElement(pipelineMeta.getXml(new Variables()));

    assertEquals(1, countOccurrences(savedXml, "<name>"));
    assertEquals(1, countOccurrences(savedXml, "<type>"));
    assertEquals(1, countOccurrences(savedXml, "<GUI>"));
    assertEquals(1, countOccurrences(savedXml, "<distribute>"));

    assertTrue(savedXml.contains("<name>Renamed while the plugin was missing</name>"));
    assertTrue(savedXml.contains("<xloc>500</xloc>"));
    assertTrue(savedXml.contains("<yloc>600</yloc>"));
    // ...and the settings of the missing plugin came along.
    assertTrue(savedXml.contains("<table>my-precious-table</table>"));
  }

  /** Repeated load/save round trips are stable: nothing is lost and nothing is added. */
  @Test
  void theRoundTripIsStable() throws Exception {
    String savedOnce = loadPipeline(PIPELINE_XML).getXml(new Variables());
    String savedTwice = loadPipeline(savedOnce).getXml(new Variables());
    String savedThrice = loadPipeline(savedTwice).getXml(new Variables());

    assertEquals(savedOnce, savedTwice);
    assertEquals(savedTwice, savedThrice);
    assertTrue(savedThrice.contains("<table>my-precious-table</table>"));
    assertFalse(savedThrice.contains("<created_hop_version/>"));
    assertFalse(savedThrice.contains("<modified_hop_version/>"));
  }

  /** Copy/paste of a transform with a missing plugin keeps its settings as well. */
  @Test
  void theLegacyTransformXmlRoundTripKeepsTheConfiguration() throws Exception {
    TransformMeta transformMeta = loadPipeline(PIPELINE_XML).getTransform(0);

    String copied = transformMeta.getXml();
    assertTrue(copied.contains("<table>my-precious-table</table>"));
    assertEquals(1, countOccurrences(copied, "<name>"));

    TransformMeta pasted = TransformMeta.fromXml(copied);
    assertTrue(pasted.isMissing());
    assertEquals(MISSING_PLUGIN_ID, pasted.getTransformPluginId());
    assertTrue(pasted.getXml().contains("<table>my-precious-table</table>"));
  }

  /**
   * The point of it all: a pipeline saved while the plugin was missing still works once the plugin
   * is installed again.
   */
  @Test
  void theConfigurationIsRestoredOnceThePluginIsInstalled() throws Exception {
    String xml =
        """
        <pipeline>
          <info>
            <name>missing-plugin-pipeline</name>
          </info>
          <transform>
            <name>A fake transform</name>
            <type>fake</type>
            <distribute>Y</distribute>
            <copies>1</copies>
            <fake>this setting must survive</fake>
            <attributes/>
            <GUI>
              <xloc>240</xloc>
              <yloc>128</yloc>
            </GUI>
          </transform>
        </pipeline>
        """;

    // The "fake" plugin isn't registered yet, so the pipeline loads with a placeholder...
    //
    PipelineMeta withoutPlugin = loadPipeline(xml);
    assertTrue(withoutPlugin.getTransform(0).isMissing());

    // ...and it gets saved in that state.
    //
    String savedXml = withoutPlugin.getXml(new Variables());

    // Now install the plugin and open the saved file again.
    //
    PluginRegistry.getInstance()
        .registerPluginClass(FakeMeta.class.getName(), TransformPluginType.class, Transform.class);

    PipelineMeta withPlugin = loadPipeline(savedXml);
    TransformMeta transformMeta = withPlugin.getTransform(0);

    assertFalse(transformMeta.isMissing());
    FakeMeta fakeMeta = assertInstanceOf(FakeMeta.class, transformMeta.getTransform());
    assertEquals("this setting must survive", fakeMeta.getFake());
  }

  /** The pipeline reports the missing plugins so the user can be warned about them. */
  @Test
  void theMissingPluginIsReported() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_XML);

    assertTrue(pipelineMeta.hasMissingPlugins());
    assertEquals(1, pipelineMeta.getMissingPipeline().size());
    assertEquals(MISSING_PLUGIN_ID, pipelineMeta.getMissingPipeline().get(0).getMissingPluginId());
  }

  /** The {@code <transform>} element of a serialized pipeline, without the surrounding pipeline. */
  private static String transformElement(String pipelineXml) {
    return pipelineXml.substring(
        pipelineXml.indexOf("<transform>"), pipelineXml.indexOf("</transform>"));
  }

  private static int countOccurrences(String xml, String tag) {
    int count = 0;
    int index = xml.indexOf(tag);
    while (index >= 0) {
      count++;
      index = xml.indexOf(tag, index + tag.length());
    }
    return count;
  }
}
