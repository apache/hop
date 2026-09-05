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

package org.apache.hop.pipeline.transforms.mapping;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.input.MappingInputMeta;
import org.apache.hop.pipeline.transforms.multimapping.MultiMappingMeta;
import org.apache.hop.pipeline.transforms.output.MappingOutputMeta;

/**
 * Extracts a sequential chain of transforms into a mapping pipeline (Mapping Input → selection →
 * Mapping Output) and replaces that chain in the parent pipeline with a Simple Mapping transform.
 */
public final class CreateMappingFromSelection {

  private static final Class<?> PKG = CreateMappingFromSelection.class;

  static final String PLUGIN_MAPPING_INPUT = "MappingInput";
  static final String PLUGIN_MAPPING_OUTPUT = "MappingOutput";
  static final String PLUGIN_SIMPLE_MAPPING = "SimpleMapping";
  static final String PLUGIN_MULTI_MAPPING = "MultiMapping";

  static final String DEFAULT_MAPPING_INPUT_NAME = "Mapping Input";
  static final String DEFAULT_MAPPING_OUTPUT_NAME = "Mapping Output";
  static final String DEFAULT_SIMPLE_MAPPING_NAME = "Simple mapping";

  static final int MAPPING_IO_X_OFFSET = 150;

  public static final String KEY_EMPTY = "CreateMapping.Error.EmptySelection";
  public static final String KEY_MAPPING_TRANSFORM = "CreateMapping.Error.MappingTransform";
  public static final String KEY_NOT_CONNECTED = "CreateMapping.Error.NotConnected";
  public static final String KEY_NOT_A_PATH = "CreateMapping.Error.NotAPath";
  public static final String KEY_MULTIPLE_INPUTS = "CreateMapping.Error.MultipleInputs";
  public static final String KEY_MULTIPLE_OUTPUTS = "CreateMapping.Error.MultipleOutputs";
  public static final String KEY_ERROR_HOP = "CreateMapping.Error.ErrorHopOut";
  public static final String KEY_INFO_HOP = "CreateMapping.Error.InfoHopIn";
  public static final String KEY_CYCLE = "CreateMapping.Error.Cycle";

  private CreateMappingFromSelection() {
    // utility
  }

  /**
   * When the context transform is part of a multi-selection, operate on the whole selection;
   * otherwise just the clicked transform.
   */
  public static List<TransformMeta> resolveSelectedTransforms(
      PipelineMeta pipelineMeta, TransformMeta contextTransform) {
    if (contextTransform == null) {
      return List.of();
    }
    List<TransformMeta> selected = pipelineMeta.getSelectedTransforms();
    if (Utils.isEmpty(selected) || !selected.contains(contextTransform)) {
      return List.of(contextTransform);
    }
    return new ArrayList<>(selected);
  }

  public static Result analyze(PipelineMeta parent, List<TransformMeta> selectedTransforms) {
    if (Utils.isEmpty(selectedTransforms)) {
      return Result.error(KEY_EMPTY);
    }

    Set<TransformMeta> selected = new HashSet<>(selectedTransforms);
    for (TransformMeta transformMeta : selectedTransforms) {
      if (isMappingPlugin(transformMeta)) {
        return Result.error(KEY_MAPPING_TRANSFORM, transformMeta.getName());
      }
    }

    List<PipelineHopMeta> internalHops = new ArrayList<>();
    List<PipelineHopMeta> externalIncoming = new ArrayList<>();
    List<PipelineHopMeta> externalOutgoing = new ArrayList<>();
    boolean infoIncoming = false;
    boolean boundaryErrorHop = false;

    for (PipelineHopMeta hop : parent.getPipelineHops()) {
      TransformMeta from = hop.getFromTransform();
      TransformMeta to = hop.getToTransform();
      if (from == null || to == null) {
        continue;
      }
      boolean fromSel = selected.contains(from);
      boolean toSel = selected.contains(to);
      if (!fromSel && !toSel) {
        continue;
      }
      if (isErrorHop(hop, from, to) && fromSel != toSel) {
        boundaryErrorHop = true;
      }
      if (fromSel && toSel) {
        internalHops.add(hop);
      } else if (!fromSel && toSel) {
        if (to.getTransform() != null && parent.isTransformInformative(to, from)) {
          infoIncoming = true;
        }
        externalIncoming.add(hop);
      } else {
        externalOutgoing.add(hop);
      }
    }

    for (TransformMeta transformMeta : selectedTransforms) {
      TransformErrorMeta errorMeta = transformMeta.getTransformErrorMeta();
      if (errorMeta != null
          && errorMeta.getTargetTransform() != null
          && !selected.contains(errorMeta.getTargetTransform())) {
        boundaryErrorHop = true;
      }
    }

    if (boundaryErrorHop) {
      return Result.error(KEY_ERROR_HOP);
    }
    if (infoIncoming) {
      return Result.error(KEY_INFO_HOP);
    }
    if (externalIncoming.size() > 1) {
      return Result.error(KEY_MULTIPLE_INPUTS);
    }
    if (externalOutgoing.size() > 1) {
      return Result.error(KEY_MULTIPLE_OUTPUTS);
    }

    Map<TransformMeta, Integer> selectedPred = new HashMap<>();
    Map<TransformMeta, Integer> selectedSucc = new HashMap<>();
    Map<TransformMeta, List<TransformMeta>> undirected = new HashMap<>();
    for (TransformMeta transformMeta : selectedTransforms) {
      selectedPred.put(transformMeta, 0);
      selectedSucc.put(transformMeta, 0);
      undirected.put(transformMeta, new ArrayList<>());
    }
    for (PipelineHopMeta hop : internalHops) {
      TransformMeta from = hop.getFromTransform();
      TransformMeta to = hop.getToTransform();
      selectedSucc.merge(from, 1, Integer::sum);
      selectedPred.merge(to, 1, Integer::sum);
      undirected.get(from).add(to);
      undirected.get(to).add(from);
    }
    for (TransformMeta transformMeta : selectedTransforms) {
      if (selectedPred.get(transformMeta) > 1 || selectedSucc.get(transformMeta) > 1) {
        return Result.error(KEY_NOT_A_PATH);
      }
    }

    if (selectedTransforms.size() > 1) {
      if (internalHops.isEmpty()) {
        return Result.error(KEY_NOT_CONNECTED);
      }
      Set<TransformMeta> visited = new HashSet<>();
      ArrayDeque<TransformMeta> queue = new ArrayDeque<>();
      TransformMeta start = selectedTransforms.get(0);
      queue.add(start);
      visited.add(start);
      while (!queue.isEmpty()) {
        TransformMeta current = queue.removeFirst();
        for (TransformMeta neighbor : undirected.get(current)) {
          if (visited.add(neighbor)) {
            queue.add(neighbor);
          }
        }
      }
      if (visited.size() != selected.size()) {
        return Result.error(KEY_NOT_CONNECTED);
      }
    }

    List<TransformMeta> entries = new ArrayList<>();
    List<TransformMeta> exits = new ArrayList<>();
    for (TransformMeta transformMeta : selectedTransforms) {
      if (selectedPred.get(transformMeta) == 0) {
        entries.add(transformMeta);
      }
      if (selectedSucc.get(transformMeta) == 0) {
        exits.add(transformMeta);
      }
    }
    if (entries.size() != 1 || exits.size() != 1) {
      return Result.error(KEY_CYCLE);
    }

    TransformMeta entry = entries.get(0);
    TransformMeta exit = exits.get(0);
    PipelineHopMeta incomingHop = externalIncoming.isEmpty() ? null : externalIncoming.get(0);
    PipelineHopMeta outgoingHop = externalOutgoing.isEmpty() ? null : externalOutgoing.get(0);

    PipelineMeta mappingPipeline =
        buildMappingPipeline(parent, selectedTransforms, internalHops, entry, exit);

    Point location =
        entry.getLocation() != null ? new Point(entry.getLocation()) : new Point(50, 50);

    return new Result(
        null,
        null,
        mappingPipeline,
        entry,
        exit,
        incomingHop != null ? incomingHop.getFromTransform() : null,
        outgoingHop != null ? outgoingHop.getToTransform() : null,
        incomingHop == null || incomingHop.isEnabled(),
        outgoingHop == null || outgoingHop.isEnabled(),
        location,
        new ArrayList<>(selectedTransforms));
  }

  public static TransformMeta replaceSelection(
      PipelineMeta parent, Result result, String mappingFilename) {
    if (result == null || !result.isValid()) {
      return null;
    }

    List<TransformMeta> selected = result.getSelected();
    Set<TransformMeta> selectedSet = new HashSet<>(selected);

    List<PipelineHopMeta> hopsToRemove = new ArrayList<>();
    for (PipelineHopMeta hop : parent.getPipelineHops()) {
      TransformMeta from = hop.getFromTransform();
      TransformMeta to = hop.getToTransform();
      if ((from != null && selectedSet.contains(from))
          || (to != null && selectedSet.contains(to))) {
        hopsToRemove.add(hop);
      }
    }

    for (PipelineHopMeta hop : hopsToRemove) {
      TransformMeta from = hop.getFromTransform();
      TransformMeta to = hop.getToTransform();
      if (to != null && !selectedSet.contains(to) && to.getTransform() != null) {
        to.getTransform().cleanAfterHopToRemove(from);
      }
      if (from != null && !selectedSet.contains(from) && from.getTransform() != null) {
        from.getTransform().cleanAfterHopFromRemove(to);
      }
    }

    for (TransformMeta transformMeta : selected) {
      int index = parent.indexOfTransform(transformMeta);
      if (index >= 0) {
        parent.removeTransform(index);
      }
    }

    SimpleMappingMeta mappingMeta = new SimpleMappingMeta();
    mappingMeta.setDefault();
    mappingMeta.setFilename(mappingFilename);

    String name = parent.getAlternativeTransformName(transformNameFromFilename(mappingFilename));
    TransformMeta simpleMapping = new TransformMeta(PLUGIN_SIMPLE_MAPPING, name, mappingMeta);
    Point location = result.getSimpleMappingLocation();
    if (location != null) {
      simpleMapping.setLocation(location.x, location.y);
    }
    parent.addTransform(simpleMapping);

    TransformMeta incomingFrom = result.getIncomingFrom();
    if (incomingFrom != null && parent.findTransform(incomingFrom.getName()) != null) {
      PipelineHopMeta inHop =
          new PipelineHopMeta(incomingFrom, simpleMapping, result.isIncomingHopEnabled());
      parent.addPipelineHop(inHop);
      retargetTargetStreams(incomingFrom, result.getEntry(), simpleMapping);
    }

    TransformMeta outgoingTo = result.getOutgoingTo();
    if (outgoingTo != null && parent.findTransform(outgoingTo.getName()) != null) {
      PipelineHopMeta outHop =
          new PipelineHopMeta(simpleMapping, outgoingTo, result.isOutgoingHopEnabled());
      parent.addPipelineHop(outHop);
      retargetInfoStreams(outgoingTo, result.getExit(), simpleMapping);
    }

    if (incomingFrom != null && incomingFrom.getTransform() != null) {
      incomingFrom.getTransform().searchInfoAndTargetTransforms(parent.getTransforms());
    }
    if (outgoingTo != null && outgoingTo.getTransform() != null) {
      outgoingTo.getTransform().searchInfoAndTargetTransforms(parent.getTransforms());
    }

    parent.unselectAll();
    simpleMapping.setSelected(true);
    return simpleMapping;
  }

  /**
   * Expresses {@code path} relative to {@code PROJECT_HOME} as {@code ${PROJECT_HOME}/…} when
   * possible. Returns the original path when it cannot be relativized.
   */
  public static String toProjectRelativePath(String path, IVariables variables) {
    if (StringUtils.isEmpty(path)) {
      return path;
    }
    if (path.startsWith(Const.VAR_PROJECT_HOME) || path.startsWith("${PROJECT_HOME}")) {
      return path;
    }
    if (variables == null) {
      return path;
    }
    String projectHome = variables.resolve(Const.VAR_PROJECT_HOME);
    if (StringUtils.isEmpty(projectHome) || Const.VAR_PROJECT_HOME.equals(projectHome)) {
      return path;
    }
    String home = projectHome;
    while (home.endsWith("/") || home.endsWith("\\")) {
      home = home.substring(0, home.length() - 1);
    }
    String normalized = path.replace('\\', '/');
    String homeNorm = home.replace('\\', '/');
    if (normalized.equals(homeNorm)) {
      return Const.VAR_PROJECT_HOME;
    }
    if (normalized.startsWith(homeNorm + "/")) {
      return Const.VAR_PROJECT_HOME + normalized.substring(homeNorm.length());
    }
    return path;
  }

  /** Base filename without extension, used as the Simple Mapping transform name. */
  public static String transformNameFromFilename(String mappingFilename) {
    if (StringUtils.isEmpty(mappingFilename)) {
      return DEFAULT_SIMPLE_MAPPING_NAME;
    }
    String baseName = FilenameUtils.getBaseName(mappingFilename.replace('\\', '/'));
    return StringUtils.isEmpty(baseName) ? DEFAULT_SIMPLE_MAPPING_NAME : baseName;
  }

  /**
   * Resolves variables in a path. Returns {@code null} when the result still contains {@code
   * ${...}} so callers do not treat an expression as a real folder (e.g. {@code
   * ./target/hop/${PROJECT_HOME}}).
   */
  public static String resolveFilesystemPath(String path, IVariables variables) {
    if (StringUtils.isEmpty(path)) {
      return path;
    }
    String resolved = variables != null ? variables.resolve(path) : path;
    if (resolved.contains("${")) {
      return null;
    }
    return resolved;
  }

  public static String suggestFilename(
      PipelineMeta parent, TransformMeta entry, IVariables variables) {
    String folder = null;
    String parentBase = "pipeline";
    if (parent != null && StringUtils.isNotEmpty(parent.getFilename())) {
      String parentFilename = resolveFilesystemPath(parent.getFilename(), variables);
      if (StringUtils.isNotEmpty(parentFilename)) {
        try {
          FileObject file = HopVfs.getFileObject(parentFilename, variables);
          FileObject parentFolder = file.getParent();
          if (parentFolder != null) {
            folder = parentFolder.getName().getURI();
          }
          String baseName = file.getName().getBaseName();
          if (baseName.toLowerCase().endsWith(PipelineMeta.PIPELINE_EXTENSION)) {
            parentBase =
                baseName.substring(0, baseName.length() - PipelineMeta.PIPELINE_EXTENSION.length());
          } else if (StringUtils.isNotEmpty(baseName)) {
            parentBase = baseName;
          }
        } catch (Exception e) {
          folder = null;
        }
      } else {
        String baseName = parent.getFilename().replace('\\', '/');
        int slash = baseName.lastIndexOf('/');
        if (slash >= 0) {
          baseName = baseName.substring(slash + 1);
        }
        if (baseName.toLowerCase().endsWith(PipelineMeta.PIPELINE_EXTENSION)) {
          parentBase =
              baseName.substring(0, baseName.length() - PipelineMeta.PIPELINE_EXTENSION.length());
        } else if (StringUtils.isNotEmpty(baseName)) {
          parentBase = baseName;
        }
      }
    }
    if (folder == null && variables != null) {
      folder = resolveFilesystemPath(Const.VAR_PROJECT_HOME, variables);
    }
    if (folder == null) {
      folder = "";
    }
    String transformPart =
        entry != null && StringUtils.isNotEmpty(entry.getName())
            ? entry.getName().replaceAll("[^A-Za-z0-9._-]+", "-")
            : "mapping";
    String separator = folder.endsWith("/") || folder.endsWith("\\") || folder.isEmpty() ? "" : "/";
    return folder + separator + parentBase + "-" + transformPart + "-mapping.hpl";
  }

  private static PipelineMeta buildMappingPipeline(
      PipelineMeta parent,
      List<TransformMeta> selectedTransforms,
      List<PipelineHopMeta> internalHops,
      TransformMeta entry,
      TransformMeta exit) {
    PipelineMeta mapping = new PipelineMeta();
    mapping.setMetadataProvider(parent.getMetadataProvider());
    mapping.setNameSynchronizedWithFilename(true);
    copyParameters(parent, mapping);

    Map<TransformMeta, TransformMeta> clones = new HashMap<>();
    for (TransformMeta transformMeta : selectedTransforms) {
      TransformMeta clone = (TransformMeta) transformMeta.clone();
      clone.setSelected(false);
      mapping.addTransform(clone);
      clones.put(transformMeta, clone);
    }

    for (PipelineHopMeta hop : internalHops) {
      TransformMeta from = clones.get(hop.getFromTransform());
      TransformMeta to = clones.get(hop.getToTransform());
      if (from == null || to == null) {
        continue;
      }
      PipelineHopMeta clonedHop = new PipelineHopMeta(from, to, hop.isEnabled());
      clonedHop.setErrorHop(hop.isErrorHop());
      mapping.addPipelineHop(clonedHop);
    }

    for (TransformMeta original : selectedTransforms) {
      TransformMeta clone = clones.get(original);
      TransformErrorMeta errorMeta = clone.getTransformErrorMeta();
      if (errorMeta == null) {
        continue;
      }
      errorMeta.setSourceTransform(clone);
      TransformMeta originalTarget = errorMeta.getTargetTransform();
      if (originalTarget != null && clones.containsKey(originalTarget)) {
        errorMeta.setTargetTransform(clones.get(originalTarget));
      } else {
        clone.setTransformErrorMeta(null);
      }
    }

    for (TransformMeta clone : clones.values()) {
      ITransformMeta meta = clone.getTransform();
      if (meta != null) {
        meta.searchInfoAndTargetTransforms(mapping.getTransforms());
      }
    }

    TransformMeta mappingInput =
        new TransformMeta(
            PLUGIN_MAPPING_INPUT,
            mapping.getAlternativeTransformName(DEFAULT_MAPPING_INPUT_NAME),
            new MappingInputMeta());
    Point entryLocation = entry.getLocation() != null ? entry.getLocation() : new Point(200, 100);
    mappingInput.setLocation(entryLocation.x - MAPPING_IO_X_OFFSET, entryLocation.y);
    mapping.addTransform(mappingInput);
    mapping.addPipelineHop(new PipelineHopMeta(mappingInput, clones.get(entry)));

    TransformMeta mappingOutput =
        new TransformMeta(
            PLUGIN_MAPPING_OUTPUT,
            mapping.getAlternativeTransformName(DEFAULT_MAPPING_OUTPUT_NAME),
            new MappingOutputMeta());
    Point exitLocation = exit.getLocation() != null ? exit.getLocation() : entryLocation;
    mappingOutput.setLocation(exitLocation.x + MAPPING_IO_X_OFFSET, exitLocation.y);
    mapping.addTransform(mappingOutput);
    mapping.addPipelineHop(new PipelineHopMeta(clones.get(exit), mappingOutput));

    return mapping;
  }

  private static void copyParameters(PipelineMeta parent, PipelineMeta mapping) {
    for (String param : parent.listParameters()) {
      try {
        mapping.addParameterDefinition(
            param, parent.getParameterDefault(param), parent.getParameterDescription(param));
      } catch (DuplicateParamException e) {
        // already defined
      } catch (Exception e) {
        // skip a parameter we cannot copy
      }
    }
  }

  private static void retargetTargetStreams(
      TransformMeta from, TransformMeta oldTarget, TransformMeta newTarget) {
    if (from == null || from.getTransform() == null || oldTarget == null || newTarget == null) {
      return;
    }
    ITransformIOMeta io = from.getTransform().getTransformIOMeta();
    if (io == null) {
      return;
    }
    for (IStream stream : io.getTargetStreams()) {
      if (stream.getTransformMeta() != null && stream.getTransformMeta().equals(oldTarget)) {
        stream.setTransformMeta(newTarget);
        stream.setSubject(newTarget.getName());
        from.getTransform().handleStreamSelection(stream);
      }
    }
  }

  private static void retargetInfoStreams(
      TransformMeta to, TransformMeta oldSource, TransformMeta newSource) {
    if (to == null || to.getTransform() == null || oldSource == null || newSource == null) {
      return;
    }
    ITransformIOMeta io = to.getTransform().getTransformIOMeta();
    if (io == null) {
      return;
    }
    for (IStream stream : io.getInfoStreams()) {
      if (stream.getTransformMeta() != null && stream.getTransformMeta().equals(oldSource)) {
        stream.setTransformMeta(newSource);
        stream.setSubject(newSource.getName());
        to.getTransform().handleStreamSelection(stream);
      }
    }
  }

  private static boolean isErrorHop(PipelineHopMeta hop, TransformMeta from, TransformMeta to) {
    if (hop.isErrorHop()) {
      return true;
    }
    return from != null && from.isSendingErrorRowsToTransform(to);
  }

  private static boolean isMappingPlugin(TransformMeta transformMeta) {
    if (transformMeta == null) {
      return false;
    }
    ITransformMeta meta = transformMeta.getTransform();
    if (meta instanceof MappingInputMeta
        || meta instanceof MappingOutputMeta
        || meta instanceof SimpleMappingMeta
        || meta instanceof MultiMappingMeta) {
      return true;
    }
    String pluginId = transformMeta.getPluginId();
    return PLUGIN_MAPPING_INPUT.equals(pluginId)
        || PLUGIN_MAPPING_OUTPUT.equals(pluginId)
        || PLUGIN_SIMPLE_MAPPING.equals(pluginId)
        || PLUGIN_MULTI_MAPPING.equals(pluginId);
  }

  @Getter
  public static final class Result {
    private final String validationKey;
    private final Object[] validationArgs;
    private final PipelineMeta mappingPipeline;
    private final TransformMeta entry;
    private final TransformMeta exit;
    private final TransformMeta incomingFrom;
    private final TransformMeta outgoingTo;
    private final boolean incomingHopEnabled;
    private final boolean outgoingHopEnabled;
    private final Point simpleMappingLocation;
    private final List<TransformMeta> selected;

    private Result(
        String validationKey,
        Object[] validationArgs,
        PipelineMeta mappingPipeline,
        TransformMeta entry,
        TransformMeta exit,
        TransformMeta incomingFrom,
        TransformMeta outgoingTo,
        boolean incomingHopEnabled,
        boolean outgoingHopEnabled,
        Point simpleMappingLocation,
        List<TransformMeta> selected) {
      this.validationKey = validationKey;
      this.validationArgs = validationArgs == null ? new Object[0] : validationArgs;
      this.mappingPipeline = mappingPipeline;
      this.entry = entry;
      this.exit = exit;
      this.incomingFrom = incomingFrom;
      this.outgoingTo = outgoingTo;
      this.incomingHopEnabled = incomingHopEnabled;
      this.outgoingHopEnabled = outgoingHopEnabled;
      this.simpleMappingLocation = simpleMappingLocation;
      this.selected = selected == null ? List.of() : selected;
    }

    static Result error(String key, Object... args) {
      return new Result(key, args, null, null, null, null, null, true, true, null, List.of());
    }

    public boolean isValid() {
      return validationKey == null;
    }

    public String getValidationError() {
      if (validationKey == null) {
        return null;
      }
      return BaseMessages.getString(PKG, validationKey, validationArgs);
    }
  }
}
