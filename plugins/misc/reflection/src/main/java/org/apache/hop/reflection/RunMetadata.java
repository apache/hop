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

package org.apache.hop.reflection;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * The metadata objects of a type as they were when a run first needed them.
 *
 * <p>Pipeline logs, workflow logs and pipeline probes are looked at every time a pipeline or
 * workflow starts. A run that starts thousands of child pipelines (a Pipeline executor, a mapping,
 * a loop in a workflow) would read the same metadata from disk thousands of times. Here the objects
 * are read once per run and type: the run is the top-level pipeline or workflow, and everything it
 * starts shares what was read. A change to this metadata while the run is going on takes effect
 * with the next run.
 *
 * <p>What a run read is dropped when its top-level pipeline or workflow finishes: an engine can run
 * more than once, a pipeline that Hop Server starts again for one, and every run reads again.
 */
public final class RunMetadata {

  /**
   * Per top-level pipeline or workflow, until it finishes. The weak keys are a backstop for an
   * engine that never does.
   */
  private static final Map<Object, Map<Class<?>, List<?>>> RUNS =
      Collections.synchronizedMap(new WeakHashMap<>());

  private RunMetadata() {}

  /**
   * @param engine the pipeline or workflow that starts
   * @param metadataProvider where to read the objects when the run does not have them yet
   * @param type the metadata type
   * @return all objects of the type, read once per run; do not change the list or its objects
   */
  @SuppressWarnings("unchecked")
  public static <T extends IHopMetadata> List<T> loadAll(
      Object engine, IHopMetadataProvider metadataProvider, Class<T> type) throws HopException {
    Map<Class<?>, List<?>> run = run(root(engine));
    List<?> objects = run.get(type);
    if (objects == null) {
      // Two children starting at the same moment may both read: harmless, the first one stays
      List<T> loaded = List.copyOf(metadataProvider.getSerializer(type).loadAll());
      List<?> earlier = run.putIfAbsent(type, loaded);
      objects = earlier == null ? loaded : earlier;
    }
    return (List<T>) objects;
  }

  /** What the run of this top-level pipeline or workflow read so far. */
  private static Map<Class<?>, List<?>> run(Object root) {
    Map<Class<?>, List<?>> run;
    synchronized (RUNS) {
      run = RUNS.get(root);
      if (run != null) {
        return run;
      }
      run = new ConcurrentHashMap<>();
      RUNS.put(root, run);
    }
    forgetWhenFinished(root, run);
    return run;
  }

  /**
   * Drops what the run read when it finishes. Only this run's entry goes: the listener stays on an
   * engine that runs again, and must not drop what the next run read.
   */
  private static void forgetWhenFinished(Object root, Map<Class<?>, List<?>> run) {
    if (root instanceof IPipelineEngine<?> pipeline) {
      pipeline.addExecutionFinishedListener(finished -> RUNS.remove(root, run));
    } else if (root instanceof IWorkflowEngine<?> workflow) {
      workflow.addExecutionFinishedListener(finished -> RUNS.remove(root, run));
    }
  }

  /** The top-level pipeline or workflow that the engine runs in. */
  static Object root(Object engine) {
    Object current = engine;
    while (true) {
      Object parent = parent(current);
      if (parent == null) {
        return current;
      }
      current = parent;
    }
  }

  private static Object parent(Object engine) {
    if (engine instanceof IPipelineEngine<?> pipeline) {
      return pipeline.getParentPipeline() != null
          ? pipeline.getParentPipeline()
          : pipeline.getParentWorkflow();
    }
    if (engine instanceof IWorkflowEngine<?> workflow) {
      return workflow.getParentWorkflow() != null
          ? workflow.getParentWorkflow()
          : workflow.getParentPipeline();
    }
    return null;
  }
}
