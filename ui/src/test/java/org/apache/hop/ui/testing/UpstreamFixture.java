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

package org.apache.hop.ui.testing;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Builds a pipeline around the transform a dialog test opens, with one upstream transform feeding
 * it. The upstream either describes a fixed set of String fields or fails to describe its fields at
 * all, the way a Table Input with broken SQL does. That lets a dialog test check what happens to
 * the configured values when the incoming fields can't be loaded.
 *
 * <pre>{@code
 * PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream("json", meta);
 * withDialog(parent -> new JsonInputDialog(parent, new Variables(), meta, pipelineMeta).open(), ...);
 * }</pre>
 */
public final class UpstreamFixture {

  /** Name of the upstream transform in the pipelines built here. */
  public static final String UPSTREAM_NAME = "upstream";

  /** The message of the exception the failing upstream throws. */
  public static final String FAILURE_MESSAGE = "Simulated upstream failure (e.g. broken SQL)";

  private UpstreamFixture() {
    // static helper
  }

  /**
   * A pipeline {@code upstream -> transformName} where the upstream can't describe its fields:
   * {@code pipelineMeta.getPrevTransformFields(variables, transformName)} throws.
   */
  public static PipelineMeta failingUpstream(String transformName, ITransformMeta meta) {
    return pipeline(transformName, meta, new UpstreamMeta(null));
  }

  /**
   * A pipeline {@code upstream -> transformName} where the upstream delivers the given String
   * fields.
   */
  public static PipelineMeta upstreamWithFields(
      String transformName, ITransformMeta meta, String... fieldNames) {
    return pipeline(transformName, meta, new UpstreamMeta(fieldNames));
  }

  /** A pipeline holding only the transform under test, so it has no incoming fields at all. */
  public static PipelineMeta withoutUpstream(String transformName, ITransformMeta meta) {
    PipelineMeta pipelineMeta = newPipelineMeta();
    pipelineMeta.addTransform(transformMeta(transformName, meta));
    return pipelineMeta;
  }

  private static PipelineMeta pipeline(
      String transformName, ITransformMeta meta, UpstreamMeta upstreamMeta) {
    PipelineMeta pipelineMeta = newPipelineMeta();
    TransformMeta upstream = new TransformMeta("UpstreamFixture", UPSTREAM_NAME, upstreamMeta);
    TransformMeta target = transformMeta(transformName, meta);
    pipelineMeta.addTransform(upstream);
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(upstream, target));
    return pipelineMeta;
  }

  /**
   * An empty pipeline with an in-memory metadata provider, so dialogs that list connections or
   * other metadata can open. Register what a test needs through {@code
   * pipelineMeta.getMetadataProvider().getSerializer(DatabaseMeta.class).save(...)}.
   */
  private static PipelineMeta newPipelineMeta() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setMetadataProvider(new MemoryMetadataProvider());
    return pipelineMeta;
  }

  private static TransformMeta transformMeta(String transformName, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    if (pluginId == null) {
      pluginId = meta.getClass().getSimpleName();
    }
    return new TransformMeta(pluginId, transformName, meta);
  }

  /** Stand-in for the upstream transform: fixed String fields, or a failure when null. */
  @SuppressWarnings("java:S2160") // equals() is not relevant for a test stub
  public static class UpstreamMeta extends BaseTransformMeta {
    private final String[] fieldNames;

    public UpstreamMeta(String[] fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public void getFields(
        IRowMeta inputRowMeta,
        String name,
        IRowMeta[] info,
        TransformMeta nextTransform,
        IVariables variables,
        IHopMetadataProvider metadataProvider)
        throws HopTransformException {
      if (fieldNames == null) {
        throw new HopTransformException(FAILURE_MESSAGE);
      }
      for (String fieldName : fieldNames) {
        ValueMetaString valueMeta = new ValueMetaString(fieldName);
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);
      }
    }
  }
}
