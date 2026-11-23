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

package org.apache.hop.documentation;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;

/**
 * What {@code hop doc} writes for a pipeline or a workflow, as Markdown text, for a plugin that
 * publishes it elsewhere (a data catalog). It leaves out the title header, which such a place keeps
 * apart, and the image of the graph, which only works next to the files {@code hop doc} writes.
 */
public final class DocMarkdown {

  private final DocBuilder builder;

  /**
   * @param includingParameters list the parameters, as {@code hop doc -ip} does
   * @param includingNotes add the text of the notes, as {@code hop doc -in} does
   */
  public DocMarkdown(
      ILogChannel log, IVariables variables, boolean includingParameters, boolean includingNotes) {
    this.builder =
        new DocBuilder(
            log,
            variables,
            null,
            null,
            null,
            null,
            includingParameters,
            includingNotes,
            false,
            false,
            false);
  }

  /**
   * @param relativeFile the file of the pipeline as the documentation names it, for example
   *     relative to the project folder
   */
  public String pipeline(PipelineMeta meta, String relativeFile) throws HopException {
    return new PipelineDocDelegate(builder).markdown(meta, relativeFile);
  }

  /**
   * @param relativeFile the file of the workflow as the documentation names it, for example
   *     relative to the project folder
   */
  public String workflow(WorkflowMeta meta, String relativeFile) throws HopException {
    return new WorkflowDocDelegate(builder).markdown(meta, relativeFile);
  }
}
