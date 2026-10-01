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

package org.apache.hop.ai.ui;

import java.util.function.Supplier;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiLogBrowser;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.widgets.Display;

public final class AiAdvisorLogSupport {

  private AiAdvisorLogSupport() {}

  public static String readPipelineLog(HopGuiPipelineGraph pipelineGraph) {
    if (pipelineGraph == null || pipelineGraph.pipelineLogDelegate == null) {
      return null;
    }
    return readTextOnUiThread(
        pipelineGraph.getDisplay(),
        () -> {
          if (pipelineGraph.pipelineLogDelegate == null) {
            return null;
          }
          return pipelineGraph.pipelineLogDelegate.getLoggingText();
        });
  }

  public static String readWorkflowLog(HopGuiWorkflowGraph workflowGraph) {
    if (workflowGraph == null || workflowGraph.workflowLogDelegate == null) {
      return null;
    }
    return readTextOnUiThread(
        workflowGraph.getDisplay(),
        () -> {
          HopGuiLogBrowser logBrowser = workflowGraph.workflowLogDelegate.getLogBrowser();
          if (logBrowser == null
              || logBrowser.getText() == null
              || logBrowser.getText().isDisposed()) {
            return null;
          }
          return logBrowser.getText().getText();
        });
  }

  /**
   * StyledText / log widgets may only be read on the UI thread. Callers that already run there
   * (Send) take the fast path; a background {@code logSupplier.get()} is marshaled with {@code
   * Display.syncExec}.
   */
  static String readTextOnUiThread(Display display, Supplier<String> read) {
    if (display == null || display.isDisposed() || read == null) {
      return null;
    }
    if (display.getThread() == Thread.currentThread()) {
      return safeRead(read);
    }
    String[] holder = new String[1];
    display.syncExec(
        () -> {
          if (!display.isDisposed()) {
            holder[0] = safeRead(read);
          }
        });
    return holder[0];
  }

  private static String safeRead(Supplier<String> read) {
    try {
      return read.get();
    } catch (SWTException e) {
      return null;
    }
  }
}
