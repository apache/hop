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

package org.apache.hop.ui.hopgui.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.SearchResult;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.refactor.MetadataObjectReference;
import org.apache.hop.metadata.refactor.MetadataReferenceResult;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.terminal.HopGuiBottomDock;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.widgets.Control;

/**
 * Turns the output of {@link org.apache.hop.metadata.refactor.MetadataReferenceFinder} into {@link
 * ISearchResult}s that the shared search-results UI ({@link HopGuiSearchResultsPanel}) can render
 * and open. Each referencing pipeline/workflow file or metadata object is wrapped in the existing
 * searchable for that object type, so double-clicking a result opens it with no new open logic.
 */
public final class ReferenceSearchResults {

  private static final String EXT_HPL = ".hpl";
  private static final String EXT_HWF = ".hwf";
  private static final String LOCATION_PIPELINE = "Project pipeline file";
  private static final String LOCATION_WORKFLOW = "Project workflow file";

  private ReferenceSearchResults() {}

  /**
   * Builds openable search results for the given references. Objects that fail to load (corrupt,
   * missing plugin, ...) are skipped so the feature stays best-effort.
   *
   * @param hopGui the active HopGui (for metadata provider, variables and logging)
   * @param fileReferences referencing pipeline/workflow files (from {@code findFileReferences})
   * @param metadataReferences referencing metadata objects (from {@code
   *     findFilePathReferencesInMetadata})
   * @return the search results, in file-first then metadata order
   */
  public static List<ISearchResult> build(
      HopGui hopGui,
      List<MetadataReferenceResult> fileReferences,
      List<MetadataObjectReference> metadataReferences) {
    List<ISearchResult> results = new ArrayList<>();
    if (fileReferences != null) {
      for (MetadataReferenceResult fileReference : fileReferences) {
        ISearchable<?> searchable = toFileSearchable(hopGui, fileReference.getFilePath());
        if (searchable != null) {
          results.add(asObjectResult(searchable));
        }
      }
    }
    if (metadataReferences != null) {
      for (MetadataObjectReference reference : metadataReferences) {
        ISearchable<?> searchable = toMetadataSearchable(hopGui, reference);
        if (searchable != null) {
          results.add(asObjectResult(searchable));
        }
      }
    }
    return results;
  }

  /**
   * Opens the given reference results in a bottom-dock tab hosting the shared search-results panel,
   * so each result can be opened by double-clicking it. Does nothing when the dock is unavailable.
   *
   * @param hopGui the active HopGui
   * @param tabTitle the dock tab title
   * @param label a short label describing what the results relate to (shown in the search field)
   * @param results the (non-empty) results to display
   */
  public static void showInBottomDock(
      HopGui hopGui, String tabTitle, String label, List<ISearchResult> results) {
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return;
    }
    Control content =
        dock.openToolTab(
            tabTitle,
            GuiResource.getInstance().getImage("ui/images/search.svg", 16, 16),
            true,
            container -> new HopGuiSearchResultsPanel(container, hopGui));
    if (content instanceof HopGuiSearchResultsPanel panel) {
      panel.showReferenceResults(label, results);
    }
  }

  /**
   * A result whose matching string equals the object's name, so the shared results tree renders it
   * as a plain object node (no inner field/transform match rows) and no component is selected on
   * open.
   */
  private static ISearchResult asObjectResult(ISearchable<?> searchable) {
    return new SearchResult(searchable, searchable.getName(), null, null, null);
  }

  private static ISearchable<?> toFileSearchable(HopGui hopGui, String path) {
    if (path == null) {
      return null;
    }
    String lower = path.toLowerCase(Locale.ROOT);
    try {
      if (lower.endsWith(EXT_HPL)) {
        PipelineMeta pipelineMeta =
            new PipelineMeta(path, hopGui.getMetadataProvider(), hopGui.getVariables());
        return new HopGuiPipelineSearchable(LOCATION_PIPELINE, pipelineMeta);
      } else if (lower.endsWith(EXT_HWF)) {
        WorkflowMeta workflowMeta =
            new WorkflowMeta(hopGui.getVariables(), path, hopGui.getMetadataProvider());
        return new HopGuiWorkflowSearchable(LOCATION_WORKFLOW, workflowMeta);
      }
    } catch (Exception e) {
      hopGui.getLog().logBasic("Find references: could not load " + path + ": " + e.getMessage());
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static ISearchable<?> toMetadataSearchable(
      HopGui hopGui, MetadataObjectReference reference) {
    try {
      IHopMetadataProvider provider = hopGui.getMetadataProvider();
      Class<? extends IHopMetadata> containerClass =
          provider.getMetadataClassForKey(reference.getContainerMetadataKey());
      if (containerClass == null) {
        return null;
      }
      IHopMetadataSerializer<IHopMetadata> serializer =
          (IHopMetadataSerializer<IHopMetadata>) provider.getSerializer(containerClass);
      IHopMetadata object = serializer.load(reference.getContainerObjectName());
      if (object == null) {
        return null;
      }
      return new HopGuiMetadataSearchable(
          provider, serializer, object, (Class<IHopMetadata>) containerClass);
    } catch (Exception e) {
      hopGui
          .getLog()
          .logBasic(
              "Find references: could not load metadata object "
                  + reference.getContainerObjectName()
                  + ": "
                  + e.getMessage());
      return null;
    }
  }
}
