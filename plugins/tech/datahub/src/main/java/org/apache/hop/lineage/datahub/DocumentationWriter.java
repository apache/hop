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

package org.apache.hop.lineage.datahub;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.documentation.DocMarkdown;
import org.apache.hop.lineage.model.DocumentationLineagePayload;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * The documentation events of the pipelines and workflows {@link DataHubPublisher} publishes, with
 * what {@code hop doc} writes for them. It needs the documentation plugin.
 */
final class DocumentationWriter {

  private final DocMarkdown markdown;

  /**
   * @param includingParameters list the parameters, as {@code hop doc -ip} does
   * @param includingNotes add the text of the notes, as {@code hop doc -in} does
   * @throws LinkageError when the documentation plugin is not installed
   */
  DocumentationWriter(
      IVariables variables, ILogChannel log, boolean includingParameters, boolean includingNotes) {
    this.markdown = new DocMarkdown(log, variables, includingParameters, includingNotes);
  }

  /**
   * @param definition an event of the pipeline's definition, whose context the documentation gets
   * @param relativeFile the file of the pipeline, relative to the folder published
   * @param project the name of the project, or null
   */
  LineageEvent pipeline(
      LineageEvent definition, PipelineMeta meta, String relativeFile, String project)
      throws HopException {
    List<String> parts = new ArrayList<>();
    for (TransformMeta transform : meta.getTransforms()) {
      parts.add(transform.getName());
    }
    return LineageEvent.of(
        LineageEventKind.DOCUMENTATION,
        definition.getContext(),
        new DocumentationLineagePayload(
            "Pipeline " + meta.getName(), markdown.pipeline(meta, relativeFile), project, parts));
  }

  /**
   * @param definition an event of the workflow's definition, whose context the documentation gets
   * @param relativeFile the file of the workflow, relative to the folder published
   * @param project the name of the project, or null
   */
  LineageEvent workflow(
      LineageEvent definition, WorkflowMeta meta, String relativeFile, String project)
      throws HopException {
    List<String> parts = new ArrayList<>();
    for (ActionMeta action : meta.getActions()) {
      parts.add(action.getName());
    }
    return LineageEvent.of(
        LineageEventKind.DOCUMENTATION,
        definition.getContext(),
        new DocumentationLineagePayload(
            "Workflow " + meta.getName(), markdown.workflow(meta, relativeFile), project, parts));
  }
}
