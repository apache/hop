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

package org.apache.hop.metadata.refactor;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.api.MetadataRefactorUtil;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Finds and replaces references to metadata elements across three scopes:
 *
 * <ol>
 *   <li><b>Pipeline/workflow XML files</b> — scans {@code .hpl}/{@code .hwf} files for XML tags
 *       whose names match {@link HopMetadataProperty} fields annotated with the relevant {@link
 *       HopMetadataPropertyType}. Tag names are collected from all registered transform and action
 *       plugins.
 *   <li><b>Other metadata objects</b> — scans loaded metadata instances (e.g. a {@code
 *       PipelineRunConfiguration}) for String fields annotated with the target property type (e.g.
 *       an {@code execution-info-location} reference). Nested engine run-configuration objects are
 *       also scanned.
 *   <li><b>File-path references in metadata objects</b> — finds metadata objects that store a
 *       pipeline/workflow file path (fields annotated with {@code PIPELINE_FILE}, {@code
 *       WORKFLOW_FILE}, or {@code HOP_FILE}), used when a file is renamed/moved.
 * </ol>
 */
public class MetadataReferenceFinder {

  private static final String EXT_HPL = ".hpl";
  private static final String EXT_HWF = ".hwf";

  /**
   * (metadata key, field name) for a metadata class field that references another metadata type.
   *
   * <p>When {@code nestedField} is non-null the field lives inside a nested sub-object: first
   * navigate to {@code nestedField} on the top-level object, then access {@code fieldName} on the
   * result (e.g. PipelineRunConfiguration → engineRunConfiguration → hopServerName).
   *
   * <p>When {@code nestedFieldClass} is non-null, the nested object must be an instance of that
   * class before {@code fieldName} is accessed. This prevents calling plugin-specific getters on
   * unrelated plugin implementations.
   *
   * <p>When {@code listFieldName} is non-null the reference lives inside items of a {@code
   * List<T>}: first get the list from {@code listFieldName}, then access {@code fieldName} on each
   * list item (e.g. PipelineLog → pipelinesToLog → pipelineToLogFilename).
   */
  private static class MetadataClassField {
    final String metadataKey;

    /** Nullable: field name on the top-level metadata object that leads to the sub-object. */
    final String nestedField;

    /** Nullable: concrete class the nested object must be an instance of. */
    final Class<?> nestedFieldClass;

    /**
     * Nullable: when non-null, the actual value is inside each item of a {@code List} stored in
     * this field on the (possibly resolved) target object.
     */
    final String listFieldName;

    final String fieldName;

    MetadataClassField(String metadataKey, String fieldName) {
      this(metadataKey, null, null, null, fieldName);
    }

    MetadataClassField(
        String metadataKey, String nestedField, Class<?> nestedFieldClass, String fieldName) {
      this(metadataKey, nestedField, nestedFieldClass, null, fieldName);
    }

    private MetadataClassField(
        String metadataKey,
        String nestedField,
        Class<?> nestedFieldClass,
        String listFieldName,
        String fieldName) {
      this.metadataKey = metadataKey;
      this.nestedField = nestedField;
      this.nestedFieldClass = nestedFieldClass;
      this.listFieldName = listFieldName;
      this.fieldName = fieldName;
    }

    static MetadataClassField forList(String metadataKey, String listFieldName, String fieldName) {
      return new MetadataClassField(metadataKey, null, null, listFieldName, fieldName);
    }

    /**
     * Creates an entry for a {@code List<String>} field where each element IS the file path. {@code
     * fieldName} is {@code null} to signal that the list item itself is compared directly.
     */
    static MetadataClassField forDirectList(String metadataKey, String listFieldName) {
      return new MetadataClassField(metadataKey, null, null, listFieldName, null);
    }
  }

  private final IHopMetadataProvider metadataProvider;
  private final Map<HopMetadataPropertyType, Set<String>> propertyTypeToTagNames;
  private final Map<HopMetadataPropertyType, List<MetadataClassField>> propertyTypeToMetadataFields;

  public MetadataReferenceFinder(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
    this.propertyTypeToTagNames = buildPropertyTypeToTagNames();
    this.propertyTypeToMetadataFields = buildPropertyTypeToMetadataFields();
  }

  /**
   * Builds a map from each HopMetadataPropertyType to the list of (metadata key, field name) where
   * that metadata type has a field referencing another metadata element of that property type.
   *
   * <p>Scans two layers:
   *
   * <ol>
   *   <li>Top-level metadata classes from {@link IHopMetadataProvider#getMetadataClasses()} for
   *       direct {@link HopMetadataProperty} fields with a property type.
   *   <li>Engine run configuration plugin implementations (pipeline and workflow engines) for
   *       nested fields with a property type. Those live inside the top-level metadata class via
   *       the {@code engineRunConfiguration} field, so they are recorded with a {@code nestedField}
   *       pointing to that accessor.
   * </ol>
   */
  private Map<HopMetadataPropertyType, List<MetadataClassField>>
      buildPropertyTypeToMetadataFields() {
    Map<HopMetadataPropertyType, List<MetadataClassField>> map =
        new EnumMap<>(HopMetadataPropertyType.class);
    try {
      // Layer 1: direct fields on top-level metadata classes
      for (Class<? extends IHopMetadata> metadataClass : metadataProvider.getMetadataClasses()) {
        HopMetadata annotation = metadataClass.getAnnotation(HopMetadata.class);
        if (annotation == null) {
          continue;
        }
        String metadataKey = annotation.key();
        for (Field field : ReflectionUtil.findAllFields(metadataClass)) {
          HopMetadataProperty prop = field.getAnnotation(HopMetadataProperty.class);
          if (prop == null) {
            continue;
          }
          HopMetadataPropertyType fieldType = prop.hopMetadataPropertyType();
          if (fieldType != HopMetadataPropertyType.NONE && field.getType() == String.class) {
            // Direct String field with a file-path type annotation
            map.computeIfAbsent(fieldType, k -> new ArrayList<>())
                .add(new MetadataClassField(metadataKey, field.getName()));
          } else if (List.class.isAssignableFrom(field.getType())
              && field.getGenericType() instanceof ParameterizedType) {
            // List<T>: scan T for file-path references.
            // When the outer field carries a specific file-path type (e.g. PIPELINE_FILE on
            // PipelineLog.pipelinesToLog) scan T only for that type. When the outer field has no
            // type (NONE, e.g. PipelineProbe.dataProbeLocations) scan T for ALL file-path types
            // so that annotated String fields inside T (e.g.
            // DataProbeLocation.sourcePipelineFilename)
            // are still discovered.
            Type[] typeArgs = ((ParameterizedType) field.getGenericType()).getActualTypeArguments();
            if (typeArgs.length == 1 && typeArgs[0] instanceof Class) {
              Class<?> itemClass = (Class<?>) typeArgs[0];
              if (fieldType != HopMetadataPropertyType.NONE) {
                scanListItemClassForType(map, metadataKey, field.getName(), itemClass, fieldType);
              } else {
                for (HopMetadataPropertyType fileType : FILE_PATH_PROPERTY_TYPES) {
                  scanListItemClassForType(map, metadataKey, field.getName(), itemClass, fileType);
                }
              }
            }
          }
          // Other non-String, non-List fields are handled by Layer 2 (engine run configs)
        }
      }

      // Layer 2: fields inside engine run configuration plugins.
      // These are embedded under the "engineRunConfiguration" field of PipelineRunConfiguration
      // and WorkflowRunConfiguration respectively.
      PluginRegistry registry = PluginRegistry.getInstance();
      scanEngineRunConfigPlugins(
          map,
          registry,
          PipelineEnginePluginType.class,
          IPipelineEngine.class,
          "pipeline-run-configuration",
          "engineRunConfiguration");
      scanEngineRunConfigPlugins(
          map,
          registry,
          WorkflowEnginePluginType.class,
          IWorkflowEngine.class,
          "workflow-run-configuration",
          "engineRunConfiguration");

    } catch (Exception e) {
      // Skip if provider or reflection fails
    }
    return map;
  }

  /**
   * Scans all plugins of the given engine plugin type, instantiates a default run configuration for
   * each, and registers any fields annotated with a non-NONE {@link HopMetadataPropertyType} as
   * nested-field entries in {@code map}.
   */
  private <E> void scanEngineRunConfigPlugins(
      Map<HopMetadataPropertyType, List<MetadataClassField>> map,
      PluginRegistry registry,
      Class<? extends org.apache.hop.core.plugins.IPluginType> pluginType,
      Class<E> engineClass,
      String containerMetadataKey,
      String nestedFieldName) {
    for (IPlugin plugin : registry.getPlugins(pluginType)) {
      try {
        E engine = registry.loadClass(plugin, engineClass);
        Object runConfig;
        if (engine instanceof IPipelineEngine) {
          runConfig = ((IPipelineEngine<?>) engine).createDefaultPipelineEngineRunConfiguration();
        } else if (engine instanceof IWorkflowEngine) {
          runConfig = ((IWorkflowEngine<?>) engine).createDefaultWorkflowEngineRunConfiguration();
        } else {
          continue;
        }
        if (runConfig == null) {
          continue;
        }
        Class<?> runConfigClass = runConfig.getClass();
        for (Field field : ReflectionUtil.findAllFields(runConfigClass)) {
          HopMetadataProperty prop = field.getAnnotation(HopMetadataProperty.class);
          if (prop == null || prop.hopMetadataPropertyType() == HopMetadataPropertyType.NONE) {
            continue;
          }
          map.computeIfAbsent(prop.hopMetadataPropertyType(), k -> new ArrayList<>())
              .add(
                  new MetadataClassField(
                      containerMetadataKey, nestedFieldName, runConfigClass, field.getName()));
        }
      } catch (Exception ignored) {
        // Plugin may not be available in this environment; skip it
      }
    }
  }

  /**
   * Scans {@code itemClass} for {@code String} fields annotated with {@code targetType}. For each
   * match, adds a list-item {@link MetadataClassField} keyed under {@code listFieldName} on the
   * container metadata object with key {@code metadataKey}.
   *
   * <p>Special case: when {@code itemClass} is {@code String.class} the items themselves ARE the
   * file paths (e.g. {@code List<String>} directly holding filenames). A direct-list entry is added
   * so that each String element is compared/replaced without sub-field navigation. This covers
   * cases like {@code WorkflowLog.workflowToLog} ({@code WORKFLOW_FILE}) and any {@code
   * List<String>} typed as {@code HOP_FILE} (which can hold either a pipeline or workflow
   * filename).
   */
  private static void scanListItemClassForType(
      Map<HopMetadataPropertyType, List<MetadataClassField>> map,
      String metadataKey,
      String listFieldName,
      Class<?> itemClass,
      HopMetadataPropertyType targetType) {
    if (itemClass == String.class) {
      // List<String>: each element IS the file path directly — no sub-field needed.
      map.computeIfAbsent(targetType, k -> new ArrayList<>())
          .add(MetadataClassField.forDirectList(metadataKey, listFieldName));
      return;
    }
    for (Field itemField : ReflectionUtil.findAllFields(itemClass)) {
      HopMetadataProperty itemProp = itemField.getAnnotation(HopMetadataProperty.class);
      if (itemProp != null
          && itemProp.hopMetadataPropertyType() == targetType
          && itemField.getType() == String.class) {
        map.computeIfAbsent(targetType, k -> new ArrayList<>())
            .add(MetadataClassField.forList(metadataKey, listFieldName, itemField.getName()));
      }
    }
  }

  /**
   * Returns {@code true} when the stored field value equals {@code filePath}, optionally resolving
   * variables (e.g. {@code ${PROJECT_HOME}/pipeline.hpl}) before comparing.
   */
  private static boolean matchesFilePath(Object value, String filePath, IVariables variables) {
    if (value == null) {
      return false;
    }
    String stored = value.toString().trim();
    if (filePath.equals(stored)) {
      return true;
    }
    if (variables != null && !stored.isEmpty()) {
      try {
        String resolved = variables.resolve(stored);
        return resolved != null && resolved.trim().equals(filePath);
      } catch (Exception ignored) {
        // keep false
      }
    }
    return false;
  }

  /**
   * Computes the value to write back when replacing a file-path reference. When the stored value
   * uses a {@code ${PROJECT_HOME}} prefix and {@code newPath} lives under that directory, the
   * returned value preserves the variable style (e.g. {@code ${PROJECT_HOME}/new.hpl}).
   */
  private static String computeNewFilePath(String stored, String newPath, IVariables variables) {
    if (variables != null && stored != null && stored.contains(Const.VAR_PROJECT_HOME)) {
      String projectHome = variables.resolve(Const.VAR_PROJECT_HOME);
      if (projectHome != null && !projectHome.isEmpty() && newPath.startsWith(projectHome)) {
        String rel = newPath.substring(projectHome.length());
        if (!rel.startsWith("/")) {
          rel = "/" + rel;
        }
        return Const.VAR_PROJECT_HOME + rel;
      }
    }
    return newPath;
  }

  /**
   * Builds a map from each HopMetadataPropertyType to the set of XML tag names that store a
   * reference of that type (from @HopMetadataProperty on transform and action metaclasses).
   */
  private static Map<HopMetadataPropertyType, Set<String>> buildPropertyTypeToTagNames() {
    Map<HopMetadataPropertyType, Set<String>> map = new EnumMap<>(HopMetadataPropertyType.class);
    PluginRegistry registry = PluginRegistry.getInstance();

    for (HopMetadataPropertyType type : HopMetadataPropertyType.values()) {
      if (type == HopMetadataPropertyType.NONE) {
        continue;
      }
      Set<String> tags = new HashSet<>();
      collectTagsFromTransformPlugins(registry, tags, type);
      collectTagsFromActionPlugins(registry, tags, type);
      if (!tags.isEmpty()) {
        map.put(type, tags);
      }
    }
    return map;
  }

  private static void collectTagsFromTransformPlugins(
      PluginRegistry registry, Set<String> tags, HopMetadataPropertyType targetType) {
    collectTagsFromPluginList(
        registry, registry.getPlugins(TransformPluginType.class), tags, targetType);
  }

  private static void collectTagsFromActionPlugins(
      PluginRegistry registry, Set<String> tags, HopMetadataPropertyType targetType) {
    collectTagsFromPluginList(
        registry, registry.getPlugins(ActionPluginType.class), tags, targetType);
  }

  private static void collectTagsFromPluginList(
      PluginRegistry registry,
      List<IPlugin> plugins,
      Set<String> tags,
      HopMetadataPropertyType targetType) {
    for (IPlugin plugin : plugins) {
      try {
        String className = plugin.getClassMap().get(plugin.getMainType());
        if (className == null) {
          continue;
        }
        Class<?> metaClass = registry.getClassLoader(plugin).loadClass(className);
        if (metaClass == null) {
          continue;
        }
        collectTagsFromClass(metaClass, tags, targetType, new HashSet<>());
      } catch (Exception ignore) {
        // Skip plugin if we can't load or reflect
      }
    }
  }

  /**
   * Collects XML tag names for fields annotated with {@code targetType} on {@code clazz} and its
   * hierarchy. When a field is a {@code List<T>}, recursively scans {@code T} as well — this covers
   * cases where connection names live inside nested list-item objects (e.g. {@code
   * ActionCheckDbConnections.CDConnection}).
   */
  private static void collectTagsFromClass(
      Class<?> clazz, Set<String> tags, HopMetadataPropertyType targetType, Set<Class<?>> visited) {
    if (clazz == null || clazz == Object.class || !visited.add(clazz)) {
      return;
    }
    for (Field field : ReflectionUtil.findAllFields(clazz)) {
      HopMetadataProperty prop = field.getAnnotation(HopMetadataProperty.class);
      if (prop != null && prop.hopMetadataPropertyType() == targetType) {
        String tag = StringUtils.isEmpty(prop.key()) ? field.getName() : prop.key();
        tags.add(tag);
      }
      // If the field is a List<T>, recurse into T so nested list-item classes are scanned
      if (List.class.isAssignableFrom(field.getType())
          && field.getGenericType() instanceof ParameterizedType) {
        Type[] typeArgs = ((ParameterizedType) field.getGenericType()).getActualTypeArguments();
        if (typeArgs.length == 1 && typeArgs[0] instanceof Class) {
          collectTagsFromClass((Class<?>) typeArgs[0], tags, targetType, visited);
        }
      }
    }
  }

  /**
   * Finds all pipeline and workflow files under the given root paths that contain a reference to
   * the given file path in any file-reference tag (PIPELINE_FILE, WORKFLOW_FILE, HOP_FILE). Used
   * when renaming or moving a file so references in other pipelines/workflows can be updated.
   *
   * @param searchRootPaths root paths to search recursively for .hpl and .hwf files
   * @param filePath the path to search for (e.g. the old path of a renamed/moved file, resolved)
   * @return list of files with at least one reference, and the count per file
   */
  public List<MetadataReferenceResult> findFileReferences(
      List<String> searchRootPaths, String filePath) throws HopException {
    return findFileReferences(searchRootPaths, filePath, null);
  }

  /**
   * Same as {@link #findFileReferences(List, String)} but when variables is not null, tag values
   * are resolved (e.g. ${PROJECT_HOME}/file.hpl) before comparing to filePath, so variable-style
   * references match the resolved path.
   */
  public List<MetadataReferenceResult> findFileReferences(
      List<String> searchRootPaths, String filePath, IVariables variables) throws HopException {
    if (StringUtils.isEmpty(filePath)) {
      return Collections.emptyList();
    }
    Set<String> tagNames = getFileReferenceTagNames();
    if (tagNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> filesToScan = collectPipelineAndWorkflowFiles(searchRootPaths);
    List<MetadataReferenceResult> results = new ArrayList<>();
    for (String path : filesToScan) {
      int count = countReferencesInFile(path, tagNames, filePath, variables);
      if (count > 0) {
        results.add(new MetadataReferenceResult(path, count));
      }
    }
    return results;
  }

  /**
   * Replaces all references to oldPath with newPath in the given result set. Only tags that store
   * file paths (PIPELINE_FILE, WORKFLOW_FILE, HOP_FILE) are updated.
   */
  public void replaceFileReferences(
      List<MetadataReferenceResult> results, String oldPath, String newPath) throws HopException {
    replaceFileReferences(results, oldPath, newPath, null);
  }

  /**
   * Same as {@link #replaceFileReferences(List, String, String)} but when variables is not null,
   * references that use variables (e.g. ${PROJECT_HOME}/file.hpl) are replaced with the same style
   * (e.g. ${PROJECT_HOME}/newfile.hpl) using the relative part of newPath under PROJECT_HOME.
   */
  public void replaceFileReferences(
      List<MetadataReferenceResult> results, String oldPath, String newPath, IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(oldPath) || StringUtils.isEmpty(newPath) || oldPath.equals(newPath)) {
      return;
    }
    Set<String> tagNames = getFileReferenceTagNames();
    if (tagNames.isEmpty()) {
      return;
    }
    for (MetadataReferenceResult result : results) {
      replaceInFile(result.getFilePath(), tagNames, oldPath, newPath, variables);
    }
  }

  /**
   * Replaces multiple file path references in the given files. Used when moving/renaming a folder
   * so each old path is replaced by its corresponding new path in one pass per file.
   */
  public void replaceFileReferences(
      List<MetadataReferenceResult> results, Map<String, String> oldPathToNewPath)
      throws HopException {
    replaceFileReferences(results, oldPathToNewPath, null);
  }

  /**
   * Same as {@link #replaceFileReferences(List, Map)} but when variables is not null, references
   * that use variables (e.g. ${PROJECT_HOME}/file.hpl) are replaced with the same style.
   */
  public void replaceFileReferences(
      List<MetadataReferenceResult> results,
      Map<String, String> oldPathToNewPath,
      IVariables variables)
      throws HopException {
    if (oldPathToNewPath == null || oldPathToNewPath.isEmpty()) {
      return;
    }
    Set<String> tagNames = getFileReferenceTagNames();
    if (tagNames.isEmpty()) {
      return;
    }
    for (MetadataReferenceResult result : results) {
      replaceInFileMultiple(result.getFilePath(), tagNames, oldPathToNewPath, variables);
    }
  }

  /** Tags that store file paths (pipeline, workflow, or generic file references). */
  private Set<String> getFileReferenceTagNames() {
    Set<String> tags = new HashSet<>();
    if (propertyTypeToTagNames.containsKey(HopMetadataPropertyType.PIPELINE_FILE)) {
      tags.addAll(propertyTypeToTagNames.get(HopMetadataPropertyType.PIPELINE_FILE));
    }
    if (propertyTypeToTagNames.containsKey(HopMetadataPropertyType.WORKFLOW_FILE)) {
      tags.addAll(propertyTypeToTagNames.get(HopMetadataPropertyType.WORKFLOW_FILE));
    }
    if (propertyTypeToTagNames.containsKey(HopMetadataPropertyType.HOP_FILE)) {
      tags.addAll(propertyTypeToTagNames.get(HopMetadataPropertyType.HOP_FILE));
    }
    return tags;
  }

  /**
   * Finds all pipeline and workflow files under the given root paths that contain a reference to
   * the given metadata element name (for the given metadata type key).
   *
   * @param metadataKey metadata type key (e.g. "rdbms", "restconnection")
   * @param elementName the metadata element name to search for (e.g. connection name)
   * @param searchRootPaths root paths to search recursively for .hpl and .hwf files
   * @return list of files with at least one reference, and the count per file
   */
  public List<MetadataReferenceResult> findReferences(
      String metadataKey, String elementName, List<String> searchRootPaths) throws HopException {
    HopMetadataPropertyType propertyType =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(metadataProvider, metadataKey);
    if (propertyType == HopMetadataPropertyType.NONE) {
      return Collections.emptyList();
    }
    Set<String> tagNames = propertyTypeToTagNames.get(propertyType);
    if (tagNames == null || tagNames.isEmpty()) {
      return Collections.emptyList();
    }
    if (StringUtils.isEmpty(elementName)) {
      return Collections.emptyList();
    }

    List<String> filesToScan = collectPipelineAndWorkflowFiles(searchRootPaths);
    List<MetadataReferenceResult> results = new ArrayList<>();
    for (String filePath : filesToScan) {
      int count = countReferencesInFile(filePath, tagNames, elementName);
      if (count > 0) {
        results.add(new MetadataReferenceResult(filePath, count));
      }
    }
    return results;
  }

  /**
   * Replaces all references to oldName with newName in the given result set. Only tags for the
   * given metadata type are updated. The metadata element itself must be renamed separately (e.g.
   * via MetadataManager.rename).
   *
   * @param metadataKey metadata type key (e.g. "rdbms") so only relevant XML tags are updated
   */
  public void replaceReferences(
      String metadataKey, List<MetadataReferenceResult> results, String oldName, String newName)
      throws HopException {
    if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName) || oldName.equals(newName)) {
      return;
    }
    HopMetadataPropertyType propertyType =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(metadataProvider, metadataKey);
    Set<String> tagNames =
        propertyType != HopMetadataPropertyType.NONE
                && propertyTypeToTagNames.containsKey(propertyType)
            ? propertyTypeToTagNames.get(propertyType)
            : Collections.emptySet();
    if (tagNames.isEmpty()) {
      return;
    }
    for (MetadataReferenceResult result : results) {
      replaceInFile(result.getFilePath(), tagNames, oldName, newName, null);
    }
  }

  /**
   * Property types that represent file-path references (pipeline, workflow, or generic hop file).
   * Used when searching metadata objects for references to a renamed pipeline/workflow file.
   */
  private static final List<HopMetadataPropertyType> FILE_PATH_PROPERTY_TYPES =
      Arrays.asList(
          HopMetadataPropertyType.PIPELINE_FILE,
          HopMetadataPropertyType.WORKFLOW_FILE,
          HopMetadataPropertyType.HOP_FILE);

  /**
   * Finds all metadata objects that have a field typed as {@link
   * HopMetadataPropertyType#PIPELINE_FILE}, {@link HopMetadataPropertyType#WORKFLOW_FILE}, or
   * {@link HopMetadataPropertyType#HOP_FILE} whose value equals {@code filePath}. This is used when
   * a pipeline/workflow file is renamed so that metadata objects referencing that file can be
   * updated. Variable references (e.g. {@code ${PROJECT_HOME}/file.hpl}) are resolved before
   * comparing when {@code variables} is provided.
   *
   * @param filePath the resolved absolute path to search for (e.g. old path of the renamed file)
   * @param variables optional; when provided, stored values are resolved before comparing
   * @return list of (container metadata key, container object name) that reference this file path
   */
  public List<MetadataObjectReference> findFilePathReferencesInMetadata(
      String filePath, IVariables variables) throws HopException {
    if (StringUtils.isEmpty(filePath)) {
      return Collections.emptyList();
    }
    List<MetadataObjectReference> results = new ArrayList<>();
    Set<MetadataObjectReference> seen = new HashSet<>();
    try {
      for (HopMetadataPropertyType fileType : FILE_PATH_PROPERTY_TYPES) {
        List<MetadataClassField> fields = propertyTypeToMetadataFields.get(fileType);
        if (fields == null || fields.isEmpty()) {
          continue;
        }
        for (MetadataClassField cf : fields) {
          Class<? extends IHopMetadata> containerClass =
              metadataProvider.getMetadataClassForKey(cf.metadataKey);
          IHopMetadataSerializer<? extends IHopMetadata> serializer =
              metadataProvider.getSerializer(containerClass);
          for (IHopMetadata instance : serializer.loadAll()) {
            Object target = resolveTarget(instance, cf);
            if (target == null) {
              continue;
            }
            boolean found;
            if (cf.listFieldName != null) {
              found = matchesFilePathInList(target, cf, filePath, variables);
            } else {
              Object value = ReflectionUtil.getFieldValue(target, cf.fieldName, false);
              found = matchesFilePath(value, filePath, variables);
            }
            if (found) {
              MetadataObjectReference ref =
                  new MetadataObjectReference(cf.metadataKey, instance.getName());
              if (seen.add(ref)) {
                results.add(ref);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error finding file path references in metadata", e);
    }
    return results;
  }

  /** Backward-compatible overload without variable resolution. */
  public List<MetadataObjectReference> findFilePathReferencesInMetadata(String filePath)
      throws HopException {
    return findFilePathReferencesInMetadata(filePath, null);
  }

  /**
   * Returns {@code true} when any item in the list stored at {@code cf.listFieldName} on {@code
   * target} matches {@code filePath} (with optional variable resolution).
   *
   * <p>When {@code cf.fieldName} is {@code null} (direct-list case) the item itself is treated as
   * the value (i.e. {@code List<String>} where each element is a file path). Otherwise the named
   * sub-field on each list item is accessed via reflection.
   */
  private boolean matchesFilePathInList(
      Object target, MetadataClassField cf, String filePath, IVariables variables)
      throws HopException {
    Object listObj = ReflectionUtil.getFieldValue(target, cf.listFieldName, false);
    if (!(listObj instanceof List)) {
      return false;
    }
    for (Object item : (List<?>) listObj) {
      if (item == null) {
        continue;
      }
      Object value =
          (cf.fieldName == null) ? item : ReflectionUtil.getFieldValue(item, cf.fieldName, false);
      if (matchesFilePath(value, filePath, variables)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Replaces {@code oldPath} with {@code newPath} in all metadata objects listed in {@code refs}.
   * Only fields typed as PIPELINE_FILE, WORKFLOW_FILE, or HOP_FILE are updated. When {@code
   * variables} is provided, stored values that use variable notation (e.g. {@code
   * ${PROJECT_HOME}/old.hpl}) are matched via resolution and written back using the same style.
   */
  public void replaceFilePathReferencesInMetadata(
      String oldPath, String newPath, List<MetadataObjectReference> refs, IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(oldPath)
        || StringUtils.isEmpty(newPath)
        || oldPath.equals(newPath)
        || refs == null
        || refs.isEmpty()) {
      return;
    }
    try {
      for (MetadataObjectReference ref : refs) {
        Class<? extends IHopMetadata> containerClass =
            metadataProvider.getMetadataClassForKey(ref.getContainerMetadataKey());
        IHopMetadataSerializer<IHopMetadata> serializer =
            (IHopMetadataSerializer<IHopMetadata>) metadataProvider.getSerializer(containerClass);
        IHopMetadata obj = serializer.load(ref.getContainerObjectName());
        if (obj == null) {
          continue;
        }
        boolean updated = false;
        for (HopMetadataPropertyType fileType : FILE_PATH_PROPERTY_TYPES) {
          List<MetadataClassField> fields = propertyTypeToMetadataFields.get(fileType);
          if (fields == null) {
            continue;
          }
          for (MetadataClassField cf : fields) {
            if (!cf.metadataKey.equals(ref.getContainerMetadataKey())) {
              continue;
            }
            Object target = resolveTarget(obj, cf);
            if (target == null) {
              continue;
            }
            if (cf.listFieldName != null) {
              if (replaceFilePathInList(target, cf, oldPath, newPath, variables)) {
                updated = true;
              }
            } else {
              Object value = ReflectionUtil.getFieldValue(target, cf.fieldName, false);
              if (matchesFilePath(value, oldPath, variables)) {
                String stored = value != null ? value.toString() : null;
                ReflectionUtil.setFieldValue(
                    target,
                    cf.fieldName,
                    String.class,
                    computeNewFilePath(stored, newPath, variables));
                updated = true;
              }
            }
          }
        }
        if (updated) {
          serializer.save(obj);
        }
      }
    } catch (Exception e) {
      throw new HopException("Error replacing file path references in metadata", e);
    }
  }

  /** Backward-compatible overload without variable resolution. */
  public void replaceFilePathReferencesInMetadata(
      String oldPath, String newPath, List<MetadataObjectReference> refs) throws HopException {
    replaceFilePathReferencesInMetadata(oldPath, newPath, refs, null);
  }

  /**
   * Replaces any list item value that matches {@code oldPath} with the computed new path. Returns
   * {@code true} when at least one replacement was made.
   *
   * <p>When {@code cf.fieldName} is {@code null} (direct-list / {@code List<String>} case) the
   * replacement is done via {@code List.set(i, newValue)}. Otherwise the named sub-field on the
   * item is updated via reflection.
   */
  @SuppressWarnings("unchecked")
  private boolean replaceFilePathInList(
      Object target, MetadataClassField cf, String oldPath, String newPath, IVariables variables)
      throws HopException {
    Object listObj = ReflectionUtil.getFieldValue(target, cf.listFieldName, false);
    if (!(listObj instanceof List)) {
      return false;
    }
    List<Object> items = (List<Object>) listObj;
    boolean changed = false;
    for (int i = 0; i < items.size(); i++) {
      Object item = items.get(i);
      if (item == null) {
        continue;
      }
      if (cf.fieldName == null) {
        // Direct List<String>: the item itself is the file path
        if (matchesFilePath(item, oldPath, variables)) {
          items.set(i, computeNewFilePath(item.toString(), newPath, variables));
          changed = true;
        }
      } else {
        Object value = ReflectionUtil.getFieldValue(item, cf.fieldName, false);
        if (matchesFilePath(value, oldPath, variables)) {
          String stored = value != null ? value.toString() : null;
          ReflectionUtil.setFieldValue(
              item, cf.fieldName, String.class, computeNewFilePath(stored, newPath, variables));
          changed = true;
        }
      }
    }
    return changed;
  }

  /**
   * Finds references to the given metadata element name in other metadata objects (e.g.
   * PipelineRunConfiguration referencing an ExecutionInfoLocation by name). Uses the same property
   * type as pipeline/workflow references.
   *
   * @param metadataKey metadata type key being renamed (e.g. "execution-info-location")
   * @param elementName the metadata element name to search for
   * @return list of (container metadata key, container object name) that reference this element
   */
  public List<MetadataObjectReference> findReferencesInMetadata(
      String metadataKey, String elementName) throws HopException {
    HopMetadataPropertyType propertyType =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(metadataProvider, metadataKey);
    if (propertyType == HopMetadataPropertyType.NONE) {
      return Collections.emptyList();
    }
    List<MetadataClassField> fields = propertyTypeToMetadataFields.get(propertyType);
    if (fields == null || fields.isEmpty()) {
      return Collections.emptyList();
    }
    if (StringUtils.isEmpty(elementName)) {
      return Collections.emptyList();
    }
    List<MetadataObjectReference> results = new ArrayList<>();
    Set<MetadataObjectReference> seen = new HashSet<>();
    try {
      for (MetadataClassField cf : fields) {
        Class<? extends IHopMetadata> containerClass =
            metadataProvider.getMetadataClassForKey(cf.metadataKey);
        IHopMetadataSerializer<? extends IHopMetadata> serializer =
            metadataProvider.getSerializer(containerClass);
        for (IHopMetadata instance : serializer.loadAll()) {
          Object target = resolveTarget(instance, cf);
          if (target == null) {
            continue;
          }
          boolean found = false;
          if (cf.listFieldName != null) {
            found = containsNameInList(target, cf.listFieldName, cf.fieldName, elementName);
          } else {
            Object value = ReflectionUtil.getFieldValue(target, cf.fieldName, false);
            found = elementName.equals(value != null ? value.toString().trim() : null);
          }
          if (found) {
            MetadataObjectReference ref =
                new MetadataObjectReference(cf.metadataKey, instance.getName());
            if (seen.add(ref)) {
              results.add(ref);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error finding references in metadata", e);
    }
    return results;
  }

  /**
   * Returns true if any item in the named list field of {@code target} matches the given name. When
   * {@code itemFieldName} is null the list items are Strings and compared directly; otherwise the
   * named sub-field on each item is compared.
   */
  @SuppressWarnings("unchecked")
  private boolean containsNameInList(
      Object target, String listFieldName, String itemFieldName, String name) throws HopException {
    Object listObj = ReflectionUtil.getFieldValue(target, listFieldName, false);
    if (!(listObj instanceof List)) {
      return false;
    }
    List<Object> list = (List<Object>) listObj;
    for (Object item : list) {
      if (item == null) {
        continue;
      }
      String value;
      if (itemFieldName == null) {
        value = item.toString().trim();
      } else {
        Object fieldVal = ReflectionUtil.getFieldValue(item, itemFieldName, false);
        value = fieldVal != null ? fieldVal.toString().trim() : null;
      }
      if (name.equals(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Replaces all occurrences of {@code oldName} with {@code newName} in the named list field of
   * {@code target}. When {@code itemFieldName} is null the list items are Strings replaced via
   * {@link List#set}; otherwise the named sub-field on each matching item is updated via
   * reflection. Returns true if at least one replacement was made.
   */
  @SuppressWarnings("unchecked")
  private boolean replaceNameInList(
      Object target, String listFieldName, String itemFieldName, String oldName, String newName)
      throws HopException {
    Object listObj = ReflectionUtil.getFieldValue(target, listFieldName, false);
    if (!(listObj instanceof List)) {
      return false;
    }
    List<Object> list = (List<Object>) listObj;
    boolean changed = false;
    for (int i = 0; i < list.size(); i++) {
      Object item = list.get(i);
      if (item == null) {
        continue;
      }
      if (itemFieldName == null) {
        if (oldName.equals(item.toString().trim())) {
          list.set(i, newName);
          changed = true;
        }
      } else {
        Object fieldVal = ReflectionUtil.getFieldValue(item, itemFieldName, false);
        if (oldName.equals(fieldVal != null ? fieldVal.toString().trim() : null)) {
          ReflectionUtil.setFieldValue(item, itemFieldName, String.class, newName);
          changed = true;
        }
      }
    }
    return changed;
  }

  /**
   * Replaces references to oldName with newName in the given metadata objects. Loads each container
   * object, updates any field that references the renamed element, and saves.
   */
  public void replaceReferencesInMetadata(
      String metadataKey, String oldName, String newName, List<MetadataObjectReference> refs)
      throws HopException {
    if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName) || oldName.equals(newName)) {
      return;
    }
    if (refs == null || refs.isEmpty()) {
      return;
    }
    HopMetadataPropertyType propertyType =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(metadataProvider, metadataKey);
    List<MetadataClassField> fields =
        propertyType != null ? propertyTypeToMetadataFields.get(propertyType) : null;
    if (fields == null || fields.isEmpty()) {
      return;
    }
    try {
      for (MetadataObjectReference ref : refs) {
        Class<? extends IHopMetadata> containerClass =
            metadataProvider.getMetadataClassForKey(ref.getContainerMetadataKey());
        IHopMetadataSerializer<IHopMetadata> serializer =
            (IHopMetadataSerializer<IHopMetadata>) metadataProvider.getSerializer(containerClass);
        IHopMetadata obj = serializer.load(ref.getContainerObjectName());
        if (obj == null) {
          continue;
        }
        boolean updated = false;
        for (MetadataClassField cf : fields) {
          if (!cf.metadataKey.equals(ref.getContainerMetadataKey())) {
            continue;
          }
          Object target = resolveTarget(obj, cf);
          if (target == null) {
            continue;
          }
          if (cf.listFieldName != null) {
            updated |= replaceNameInList(target, cf.listFieldName, cf.fieldName, oldName, newName);
          } else {
            Object value = ReflectionUtil.getFieldValue(target, cf.fieldName, false);
            if (oldName.equals(value != null ? value.toString().trim() : null)) {
              ReflectionUtil.setFieldValue(target, cf.fieldName, String.class, newName);
              updated = true;
            }
          }
        }
        if (updated) {
          serializer.save(obj);
        }
      }
    } catch (Exception e) {
      throw new HopException("Error replacing references in metadata", e);
    }
  }

  /**
   * Returns the object on which {@code cf.fieldName} should be read or written. When {@code
   * cf.nestedField} is non-null, the top-level object's nested field is resolved first (e.g. {@code
   * PipelineRunConfiguration.getEngineRunConfiguration()}). Returns {@code null} when the nested
   * field value is null (no engine run configuration set) or when {@code cf.nestedFieldClass} is
   * set and the nested object is not an instance of that class (e.g. the engine run config is
   * Local, not Remote, so it doesn't have {@code hopServerName}).
   */
  private Object resolveTarget(Object topLevel, MetadataClassField cf) throws HopException {
    if (cf.nestedField == null) {
      return topLevel;
    }
    Object nested = ReflectionUtil.getFieldValue(topLevel, cf.nestedField, false);
    if (nested == null) {
      return null;
    }
    if (cf.nestedFieldClass != null && !cf.nestedFieldClass.isInstance(nested)) {
      return null;
    }
    return nested;
  }

  private List<String> collectPipelineAndWorkflowFiles(List<String> searchRootPaths)
      throws HopException {
    List<String> collected = new ArrayList<>();
    FileSelector selector =
        new FileSelector() {
          @Override
          public boolean includeFile(FileSelectInfo info) {
            String name = info.getFile().getName().getBaseName();
            return name.endsWith(EXT_HPL) || name.endsWith(EXT_HWF);
          }

          @Override
          public boolean traverseDescendents(FileSelectInfo info) {
            return true;
          }
        };
    for (String root : searchRootPaths) {
      if (StringUtils.isEmpty(root)) {
        continue;
      }
      try {
        FileObject rootFile = HopVfs.getFileObject(root);
        if (!rootFile.exists()) {
          continue;
        }
        if (rootFile.getType() == FileType.FILE) {
          String name = rootFile.getName().getBaseName();
          if (name.endsWith(EXT_HPL) || name.endsWith(EXT_HWF)) {
            collected.add(HopVfs.getFilename(rootFile));
          }
          continue;
        }
        FileObject[] children = rootFile.findFiles(selector);
        if (children != null) {
          for (FileObject child : children) {
            collected.add(HopVfs.getFilename(child));
          }
        }
      } catch (Exception e) {
        throw new HopException("Error scanning for pipeline/workflow files under: " + root, e);
      }
    }
    return collected;
  }

  private int countReferencesInFile(
      String filePath, Set<String> tagNames, String elementName, IVariables variables) {
    try {
      Document doc = XmlHandler.loadXmlFile(filePath);
      if (doc == null) {
        return 0;
      }
      final int[] count = {0};
      walkElements(doc.getDocumentElement(), tagNames, elementName, null, count, false, variables);
      return count[0];
    } catch (Exception e) {
      return 0;
    }
  }

  private int countReferencesInFile(String filePath, Set<String> tagNames, String elementName) {
    return countReferencesInFile(filePath, tagNames, elementName, null);
  }

  private void replaceInFile(
      String filePath, Set<String> tagNames, String oldName, String newName, IVariables variables)
      throws HopException {
    try {
      Document doc = XmlHandler.loadXmlFile(filePath);
      if (doc == null) {
        throw new HopException("Could not load XML: " + filePath);
      }
      walkElements(
          doc.getDocumentElement(), tagNames, oldName, newName, new int[1], true, variables);
      String xml = XmlHandler.getXmlString(doc, false, true);
      try (OutputStream out = HopVfs.getOutputStream(HopVfs.getFileObject(filePath), false)) {
        out.write(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
    } catch (HopXmlException e) {
      throw new HopException("Error updating references in file: " + filePath, e);
    } catch (Exception e) {
      throw new HopException("Error updating references in file: " + filePath, e);
    }
  }

  private void replaceInFileMultiple(
      String filePath, Set<String> tagNames, Map<String, String> oldToNew, IVariables variables)
      throws HopException {
    try {
      Document doc = XmlHandler.loadXmlFile(filePath);
      if (doc == null) {
        throw new HopException("Could not load XML: " + filePath);
      }
      for (Map.Entry<String, String> e : oldToNew.entrySet()) {
        walkElements(
            doc.getDocumentElement(),
            tagNames,
            e.getKey(),
            e.getValue(),
            new int[1],
            true,
            variables);
      }
      String xml = XmlHandler.getXmlString(doc, false, true);
      try (OutputStream out = HopVfs.getOutputStream(HopVfs.getFileObject(filePath), false)) {
        out.write(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
    } catch (HopXmlException ex) {
      throw new HopException("Error updating references in file: " + filePath, ex);
    } catch (Exception ex) {
      throw new HopException("Error updating references in file: " + filePath, ex);
    }
  }

  /**
   * Recursively walks the XML tree counting (and optionally replacing) element values that match
   * {@code elementName}. Only elements whose tag name is in {@code tagNames} are inspected. When
   * {@code variables} is non-null, stored values are resolved before comparison so that
   * variable-style references (e.g. {@code ${PROJECT_HOME}/file.hpl}) match the resolved path. When
   * replacing and the stored value uses a {@code ${PROJECT_HOME}} prefix, the replacement is
   * written back in the same variable style.
   */
  private void walkElements(
      Node node,
      Set<String> tagNames,
      String elementName,
      String newNameForReplace,
      int[] count,
      boolean replace,
      IVariables variables) {
    if (node == null) {
      return;
    }
    if (node.getNodeType() == Node.ELEMENT_NODE) {
      String tagName = node.getNodeName();
      if (tagNames.contains(tagName)) {
        String value = XmlHandler.getNodeValue(node);
        if (value != null) {
          String trimmed = value.trim();
          boolean matches = trimmed.equals(elementName);
          if (!matches && variables != null) {
            try {
              String resolved = variables.resolve(trimmed);
              matches = resolved != null && resolved.trim().equals(elementName);
            } catch (Exception ignored) {
              // keep matches false
            }
          }
          if (matches) {
            count[0]++;
            if (replace && node instanceof Element el && newNameForReplace != null) {
              el.setTextContent(computeNewFilePath(trimmed, newNameForReplace, variables));
            }
          }
        }
      }
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++) {
        walkElements(
            children.item(i), tagNames, elementName, newNameForReplace, count, replace, variables);
      }
    } else {
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++) {
        walkElements(
            children.item(i), tagNames, elementName, newNameForReplace, count, replace, variables);
      }
    }
  }
}
