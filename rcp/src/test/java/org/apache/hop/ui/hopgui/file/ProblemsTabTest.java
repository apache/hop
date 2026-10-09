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
package org.apache.hop.ui.hopgui.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineCheckDelegate;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowCheckDelegate;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Problems tab of a pipeline or workflow.
 *
 * <p>Detaching the execution results builds the tab again, and it came back empty. Remarks about
 * the pipeline as a whole were listed between two transforms' groups, as if they belonged to one.
 *
 * @see <a href="https://github.com/apache/hop/issues/8736">#8736</a>
 */
@Tag("uitest")
class ProblemsTabTest extends SwtBotTestBase {

  private static TransformMeta transform(String name) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(name);
    transformMeta.setTransformPluginId("Dummy");
    return transformMeta;
  }

  private static ICheckResultSource action(String name) {
    ICheckResultSource source = mock(ICheckResultSource.class);
    when(source.getName()).thenReturn(name);
    return source;
  }

  private static ICheckResult warning(String text, ICheckResultSource source) {
    return new CheckResult(ICheckResult.TYPE_RESULT_WARNING, text, source);
  }

  /** The top-level rows of the tab, with the first child of each, as "group > first remark". */
  private static List<String> rows(Composite parent) {
    List<String> rows = new ArrayList<>();
    Tree tree = findTree(parent);
    for (TreeItem item : tree.getItems()) {
      rows.add(
          item.getItemCount() == 0
              ? item.getText()
              : item.getText() + " > " + item.getItem(0).getText());
    }
    return rows;
  }

  private static Tree findTree(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Tree tree && !tree.isDisposed()) {
        return tree;
      }
      if (child instanceof Composite composite) {
        Tree tree = findTree(composite);
        if (tree != null) {
          return tree;
        }
      }
    }
    return null;
  }

  @Test
  void pipelineRemarksAreOnTopAndSurviveADetach() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("load-customers");
    HopGuiPipelineGraph graph = mock(HopGuiPipelineGraph.class);
    when(graph.getPipelineMeta()).thenReturn(pipelineMeta);
    List<ICheckResult> remarks =
        List.of(
            warning("No description", transform("Dummy (do nothing) 2")),
            warning("[STRUCT-003] The pipeline has disabled hops", null),
            warning("No description", transform("Dummy (do nothing)")));
    List<String> expected =
        List.of(
            "load-customers > [STRUCT-003] The pipeline has disabled hops",
            "Dummy (do nothing) 2 > No description",
            "Dummy (do nothing) > No description");

    List<List<String>> seen = new ArrayList<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          graph.extraViewTabFolder = new CTabFolder(shell, SWT.NONE);
          HopGuiPipelineCheckDelegate delegate = new HopGuiPipelineCheckDelegate(null, graph);
          delegate.addPipelineCheck();
          delegate.refresh(remarks);
          seen.add(rows(shell));

          graph.extraViewTabFolder.dispose();
          graph.extraViewTabFolder = new CTabFolder(shell, SWT.NONE);
          delegate.addPipelineCheck();
          seen.add(rows(shell));
        },
        bot -> {});

    assertEquals(expected, seen.get(0), "as listed");
    assertEquals(expected, seen.get(1), "after the results view was detached");
  }

  @Test
  void workflowRemarksAreOnTopAndSurviveADetach() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("nightly");
    HopGuiWorkflowGraph graph = mock(HopGuiWorkflowGraph.class);
    when(graph.getWorkflowMeta()).thenReturn(workflowMeta);
    List<ICheckResult> remarks =
        List.of(
            warning("No server", action("Mail")),
            warning("[STRUCT-003] The workflow has disabled hops", null));
    List<String> expected =
        List.of("nightly > [STRUCT-003] The workflow has disabled hops", "Mail > No server");

    List<List<String>> seen = new ArrayList<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          graph.extraViewTabFolder = new CTabFolder(shell, SWT.NONE);
          HopGuiWorkflowCheckDelegate delegate = new HopGuiWorkflowCheckDelegate(null, graph);
          delegate.addWorkflowCheck();
          delegate.refresh(remarks);
          seen.add(rows(shell));

          graph.extraViewTabFolder.dispose();
          graph.extraViewTabFolder = new CTabFolder(shell, SWT.NONE);
          delegate.addWorkflowCheck();
          seen.add(rows(shell));
        },
        bot -> {});

    assertEquals(expected, seen.get(0), "as listed");
    assertEquals(expected, seen.get(1), "after the results view was detached");
  }
}
