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

package org.apache.hop.pipeline.transforms.metainject;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Read a simple CSV file Just output Strings found in the file... */
public class MetaInject extends BaseTransform<MetaInjectMeta, MetaInjectData> {
  private static final Class<?> PKG = MetaInject.class;

  public MetaInject(
      TransformMeta transformMeta,
      MetaInjectMeta meta,
      MetaInjectData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline trans) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, trans);
  }

  @Override
  public boolean processRow() throws HopException {
    // Read the data from all input transforms and keep it in memory...
    // Skip the transform from which we stream data. Keep that available for runtime action.
    //
    data.rowMap = new HashMap<>();
    data.receivedRows = false;
    data.hasEmptyList = false;

    // If there are no previous transforms we will set receivedRows to true. This allows execution
    // using constants only
    if (getPipelineMeta().getPrevTransformNames(getTransformMeta()).length == 0) {
      data.receivedRows = true;
    }

    readDataFromSourceTransforms();

    injectMetadataIntoTemplateTransforms();

    // Check if all previous transforms are returning data, unless isAllowEmptyStreamOnExecution is
    // true then execute if at least one branch has data
    if (!data.receivedRows || (data.hasEmptyList && !meta.isAllowEmptyStreamOnExecution())) {
      setOutputDone();
      return false;
    }

    postInjectionHandling();

    if (!meta.isNoExecution()) {
      executeTemplatePipeline();
    }

    // We let the pipeline complete its execution to allow for any customizations to MDI to
    // happen in the init methods of transforms.
    //
    String targetFile = resolve(meta.getTargetFile());
    if (!Utils.isEmpty(targetFile)) {
      writeInjectedHpl(targetFile);
    }

    // All done!
    setOutputDone();

    return false;
  }

  private void executeTemplatePipeline() throws HopException {
    // Now we can execute this modified transformation metadata.
    //
    final Pipeline injectPipeline = createInjectPipeline();
    injectPipeline.setParentPipeline(getPipeline());
    injectPipeline.setMetadataProvider(getMetadataProvider());
    if (getPipeline().getParentWorkflow() != null) {
      injectPipeline.setParentWorkflow(getPipeline().getParentWorkflow());
    }

    // Copy all variables over...
    //
    injectPipeline.copyFrom(this);

    // Copy parameter definitions with empty values.
    // Then set those parameters to the values if have any.

    injectPipeline.copyParametersFromDefinitions(data.pipelineMeta);
    for (String variableName : injectPipeline.getVariableNames()) {
      String variableValue = getVariable(variableName);
      if (StringUtils.isNotEmpty(variableValue)) {
        injectPipeline.setParameterValue(variableName, variableValue);
      }
    }

    getPipeline().addExecutionStoppedListener(e -> injectPipeline.stopAll());

    injectPipeline.setLogLevel(getLogLevel());

    // Parameters get activated below so we need to make sure they have values
    //
    injectPipeline.prepareExecution();

    // See if we need to stream some data over...
    //
    RowProducer rowProducer =
        data.streaming ? injectPipeline.addRowProducer(data.streamingTargetTransformName, 0) : null;

    // Finally, add the mapping transformation to the active sub-transformations
    // map in the parent transformation
    //
    getPipeline().addActiveSubPipeline(getTransformName(), injectPipeline);

    if (!Utils.isEmpty(meta.getSourceTransformName())) {
      handleReadingFromSourceTransform(injectPipeline);
    }

    injectPipeline.startThreads();

    Future<HopTransformException> steamingFuture = handleStreamingToTargetTransform(rowProducer);

    // Wait until the child transformation finished processing...
    //
    while (!injectPipeline.isFinished() && !injectPipeline.isStopped() && !isStopped()) {
      copyResult(injectPipeline);

      // Wait a little bit.
      try {
        Thread.sleep(50);
      } catch (Exception e) {
        // Ignore errors
      }
    }
    copyResult(injectPipeline);
    waitUntilFinished(injectPipeline);
    try {
      if (steamingFuture != null && steamingFuture.get() != null) {
        throw steamingFuture.get();
      }
    } catch (ExecutionException ignore) {
      // Ignore
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private Future<HopTransformException> handleStreamingToTargetTransform(RowProducer rowProducer)
      throws HopException {
    Future<HopTransformException> steamingFuture = null;
    if (data.streaming) {
      // Deplete all the rows from the parent transformation into the modified transformation
      //
      IRowSet rowSet = findInputRowSet(data.streamingSourceTransformName);
      if (rowSet == null) {
        throw new HopException(
            "Unable to find transform '"
                + data.streamingSourceTransformName
                + "' to stream data from");
      }
      steamingFuture = ExecutorUtil.getExecutor().submit(() -> putRows(rowSet, rowProducer));
    }
    return steamingFuture;
  }

  private void handleReadingFromSourceTransform(Pipeline injectPipeline) throws HopException {
    ITransform transformInterface = injectPipeline.getTransform(meta.getSourceTransformName(), 0);
    if (transformInterface == null) {
      throw new HopException(
          "Unable to find transform '" + meta.getSourceTransformName() + "' to read from.");
    }
    transformInterface.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            // Just pass along the data as output of this transform...
            //
            MetaInject.this.putRow(rowMeta, row);
          }
        });
  }

  private void postInjectionHandling() {
    List<TransformMeta> targetTransforms = data.pipelineMeta.getTransforms();
    for (Entry<String, ITransformMeta> en : data.transformInjectionMetasMap.entrySet()) {
      en.getValue().resetTransformIoMeta();
      en.getValue().searchInfoAndTargetTransforms(targetTransforms);
    }

    for (String targetTransformName : data.transformInjectionMetasMap.keySet()) {
      if (!data.transformInjectionMetasMap.containsKey(targetTransformName)) {
        TransformMeta targetTransform =
            TransformMeta.findTransform(targetTransforms, targetTransformName);
        if (targetTransform != null) {
          targetTransform.getTransform().searchInfoAndTargetTransforms(targetTransforms);
        }
      }
    }
  }

  private void readDataFromSourceTransforms() throws HopTransformException {
    for (String prevTransformName : getPipelineMeta().getPrevTransformNames(getTransformMeta())) {
      // Don't read from the streaming source transform
      //
      if (!data.streaming
          || !prevTransformName.equalsIgnoreCase(data.streamingSourceTransformName)) {
        List<RowMetaAndData> list = new ArrayList<>();
        IRowSet rowSet = findInputRowSet(prevTransformName);
        Object[] row = getRowFrom(rowSet);
        while (row != null) {
          RowMetaAndData rd = new RowMetaAndData();
          rd.setRowMeta(rowSet.getRowMeta());
          rd.setData(row);
          list.add(rd);

          row = getRowFrom(rowSet);
        }
        if (!list.isEmpty()) {
          data.receivedRows = true;
          data.rowMap.put(prevTransformName, list);
        } else {
          data.hasEmptyList = true;
        }
      }
    }
  }

  private void injectMetadataIntoTemplateTransforms() throws HopException {
    for (Entry<String, ITransformMeta> entry : data.transformInjectionMetasMap.entrySet()) {
      String targetTransformName = entry.getKey();
      ITransformMeta targetTransformMeta = entry.getValue();

      checkInjectionSupported(targetTransformMeta.getClass());

      InjectionSupported annotation =
          targetTransformMeta.getClass().getAnnotation(InjectionSupported.class);
      if (annotation == null) {
        // Injects using @HopMetadataProperty annotations.
        injectMetadataIntoTemplateTransform(targetTransformMeta, targetTransformName);
      } else {
        // Old style metadata injection (Legacy)
        newInjection(targetTransformName, targetTransformMeta);
        newInjectionConstants(this, targetTransformName, targetTransformMeta);
      }
    }
  }

  private void checkInjectionSupported(Class<?> clazz) throws HopException {
    InjectionSupported annotation = clazz.getAnnotation(InjectionSupported.class);
    if (annotation != null) {
      return;
    }

    // See if there are any Hop metadata properties we can use...
    //
    for (Field field : ReflectionUtil.findAllFields(clazz)) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null) {
        return;
      }
    }
    throw new HopException("Injection is not supported for class " + clazz.getSimpleName());
  }

  private void injectMetadataIntoTemplateTransform(
      ITransformMeta targetTransformMeta, String targetTransformName) throws HopException {
    // Find the injection key-to-group mapping
    //
    Map<String, Set<String>> injectionKeyGroupMap =
        HopMetadataInjector.findInjectionGroupKeys(targetTransformMeta.getClass());

    // Which keys need injection into the class (not group rows of data)?
    //
    Map<String, Object> injectionKeyData = new HashMap<>();

    // These are the values to inject into keys belonging to the groups.
    //
    Map<String, RowBuffer> injectionGroupData = new HashMap<>();

    // What is the row layout for a particular group data?
    //
    Map<String, IRowMeta> groupRowMetaMap = new HashMap<>();

    // We keep track of where an injection group data comes from.
    //
    Map<String, String> groupSourceMap = new HashMap<>();

    // We already have the rows.  We just need to rename the fields to the injection key
    //
    for (MetaInjectMapping mapping : meta.getMappings()) {
      collectDataForOneMapping(
          targetTransformName,
          mapping,
          injectionKeyGroupMap,
          groupRowMetaMap,
          groupSourceMap,
          injectionGroupData,
          injectionKeyData);
    }

    // Now all source rows are mapped and we can construct the group data map
    //
    for (Entry<String, String> e : groupSourceMap.entrySet()) {
      String groupKey = e.getKey();
      String sourceTransformName = e.getValue();
      IRowMeta rowMeta = groupRowMetaMap.get(groupKey);
      List<RowMetaAndData> rowMetaAndData = data.rowMap.get(sourceTransformName);
      // Make a copy of the row metadata and data to avoid any risk of
      // another transform modifying the rows during injection.
      //
      RowBuffer rowBuffer = new RowBuffer(rowMeta.clone());
      for (RowMetaAndData rd : rowMetaAndData) {
        rowBuffer.getBuffer().add(rowMeta.cloneRow(rd.getData()));
      }
      injectionGroupData.put(groupKey, rowBuffer);
    }

    // Now everything is ready to be injected into the target transform
    //
    if (!injectionKeyData.isEmpty() || !injectionGroupData.isEmpty()) {
      HopMetadataInjector.inject(
          metadataProvider, targetTransformMeta, injectionKeyData, injectionGroupData);
    }
  }

  private void collectDataForOneMapping(
      String targetTransformName,
      MetaInjectMapping mapping,
      Map<String, Set<String>> injectionKeyGroupMap,
      Map<String, IRowMeta> groupRowMetaMap,
      Map<String, String> groupSourceMap,
      Map<String, RowBuffer> injectionGroupData,
      Map<String, Object> injectionKeyData)
      throws HopTransformException, HopValueException {
    if (!targetTransformName.equalsIgnoreCase(mapping.getTargetTransformName())) {
      // Covered elsewhere.
      return;
    }
    List<RowMetaAndData> sourceRows = data.rowMap.get(mapping.getSourceTransformName());

    // To which group does this belong?
    //
    String groupKey = lookupGroupKey(injectionKeyGroupMap, mapping.getTargetAttributeKey());
    if (mapping.isTargetDetail() || groupKey != null) {
      // If it's a detail and not part of a group, we have a configuration error.
      //
      if (groupKey == null) {
        throw new HopTransformException(
            "The injection group key for target key '"
                + mapping.getTargetAttributeKey()
                + "' was not found.");
      }

      if (sourceRows != null && !sourceRows.isEmpty()) {
        // We need to collect the data for the mapping
        //
        collectDataForOneMappingGroup(
            mapping, groupRowMetaMap, groupSourceMap, groupKey, sourceRows);
      } else {
        // See if this is a constant without a source
        //
        if (StringUtils.isEmpty(mapping.getSourceTransformName())) {
          addConstantToGroupData(mapping, injectionGroupData, groupKey);
        }
      }
    } else {
      // A simple property from the first row.
      // This also captures the "Constant" mappings where we don't have source rows to feed the
      // injection.
      //
      collectInjectionKeyValue(mapping, sourceRows, injectionKeyData);
    }
  }

  private static void collectDataForOneMappingGroup(
      MetaInjectMapping mapping,
      Map<String, IRowMeta> groupRowMetaMap,
      Map<String, String> groupSourceMap,
      String groupKey,
      List<RowMetaAndData> sourceRows)
      throws HopTransformException {
    IRowMeta rowMeta;
    rowMeta =
        groupRowMetaMap.computeIfAbsent(groupKey, f -> sourceRows.getFirst().getRowMeta().clone());
    int index = rowMeta.indexOfValue(mapping.getSourceField());
    if (index < 0) {
      // Is the field already renamed to the target?
      //
      index = rowMeta.indexOfValue(mapping.getTargetAttributeKey());
    }
    if (index < 0) {
      throw new HopTransformException(
          "The field '"
              + mapping.getSourceField()
              + "' from transform '"
              + mapping.getSourceTransformName()
              + "' to inject could not be found");
    }
    // We give the value the name of the target injection key
    //
    IValueMeta valueMeta = rowMeta.getValueMeta(index);
    valueMeta.setName(mapping.getTargetAttributeKey());

    // Let's keep track of where the rows are coming from for the current group
    //
    groupSourceMap.put(groupKey, mapping.getSourceTransformName());
  }

  private static void addConstantToGroupData(
      MetaInjectMapping mapping, Map<String, RowBuffer> injectionGroupData, String groupKey) {
    // We need to add or extend a single row in a row buffer for the given group.
    //
    RowBuffer rowBuffer = injectionGroupData.computeIfAbsent(groupKey, f -> new RowBuffer());

    // We need to extend this row buffer with one value in the metadata and keep it to one
    // row.
    //
    IRowMeta rowMeta = rowBuffer.getRowMeta();
    rowMeta.addValueMeta(new ValueMetaString(mapping.getTargetAttributeKey()));
    List<Object[]> rows = rowBuffer.getBuffer();
    Object[] row;
    if (rows.isEmpty()) {
      row = RowDataUtil.allocateRowData(1);
      rows.add(row);
    } else {
      row = rows.getFirst();
    }
    row = RowDataUtil.createResizedCopy(row, rowMeta.size());
    row[rowMeta.size() - 1] = mapping.getSourceField();
    // Let's not forget to update the row after re-sizing it.
    rows.set(0, row);
  }

  private static String lookupGroupKey(
      Map<String, Set<String>> groupKeysMap, String targetAttributeKey) {
    // Find the first group that matches.
    // If for some reason there is more than one group that matches, we're out of luck.
    // This transform failed to capture the group to which the target attribute key belongs to.
    //
    for (Entry<String, Set<String>> entry : groupKeysMap.entrySet()) {
      if (entry.getValue().contains(targetAttributeKey)) {
        return entry.getKey();
      }
    }
    return null;
  }

  private static void collectInjectionKeyValue(
      MetaInjectMapping mapping,
      List<RowMetaAndData> sourceRows,
      Map<String, Object> injectionKeyData)
      throws HopTransformException, HopValueException {
    if (sourceRows == null || sourceRows.isEmpty()) {
      if (StringUtils.isEmpty(mapping.getSourceTransformName())) {
        // This is a constant String value to set.
        // The value is set in the source field.
        //
        injectionKeyData.put(mapping.getTargetAttributeKey(), mapping.getSourceField());
      }
      return;
    }
    RowMetaAndData sourceRow = sourceRows.getFirst();
    int index = sourceRow.getRowMeta().indexOfValue(mapping.getSourceField());
    if (index < 0) {
      throw new HopTransformException(
          "The field '"
              + mapping.getSourceField()
              + "' from transform '"
              + mapping.getSourceTransformName()
              + "' to inject could not be found");
    }
    // We have the field, we can rename it to the target key
    //
    IValueMeta valueMeta = sourceRow.getRowMeta().getValueMeta(index);
    Object valueData = valueMeta.getNativeDataType(sourceRow.getData()[index]);
    injectionKeyData.put(mapping.getTargetAttributeKey(), valueData);
  }

  HopTransformException putRows(IRowSet rowSet, RowProducer rowProducer) {
    try {
      Object[] row;
      while (!isStopped() && (row = getRowFrom(rowSet)) != null) {
        rowProducer.putRow(rowSet.getRowMeta(), row);
      }
    } catch (HopTransformException e) {
      return e;
    } finally {
      rowProducer.finished();
    }
    return null;
  }

  void waitUntilFinished(Pipeline injectTrans) {
    injectTrans.waitUntilFinished();
  }

  Pipeline createInjectPipeline() throws HopException {
    LocalPipelineEngine lpe = new LocalPipelineEngine(data.pipelineMeta, this, this);
    if (!Utils.isEmpty(meta.getRunConfigurationName())) {
      PipelineRunConfiguration prc =
          metadataProvider
              .getSerializer(PipelineRunConfiguration.class)
              .load(meta.getRunConfigurationName());
      lpe.setPipelineRunConfiguration(prc);
    }
    return lpe;
  }

  private void writeInjectedHpl(String targetFilPath) throws HopException {

    writeInjectedHplToFs(targetFilPath);
  }

  /**
   * Writes the generated meta injection transformation to the file system.
   *
   * @param targetFilePath the filesystem path to which to save the generated injection hpl
   * @throws HopException In case we couldn't write the pipeline XML to the given target path.
   */
  private void writeInjectedHplToFs(String targetFilePath) throws HopException {
    if (meta.isCreateParentFolder()) {
      createParentFolder(targetFilePath);
    }
    try (OutputStream os = HopVfs.getOutputStream(targetFilePath, false, variables)) {
      os.write(XmlHandler.getXmlHeader().getBytes(StandardCharsets.UTF_8));
      os.write(data.pipelineMeta.getXml(this).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new HopException(
          "Unable to write target file (hpl after injection) to file '" + targetFilePath + "'", e);
    }
  }

  private void createParentFolder(String filename) throws HopException {
    // Check for parent folder
    //
    try (FileObject parentFolder = HopVfs.getFileObject(filename, variables).getParent()) {
      // Check the parent folder
      if (parentFolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "MetaInject.Log.ParentFolderExist", HopVfs.getFriendlyURI(parentFolder)));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "MetaInject.Log.ParentFolderNotExist", HopVfs.getFriendlyURI(parentFolder)));
        }
        if (meta.isCreateParentFolder()) {
          parentFolder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "MetaInject.Log.ParentFolderCreated",
                    HopVfs.getFriendlyURI(parentFolder)));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "MetaInject.Log.ParentFolderNotExistCreateIt",
                  HopVfs.getFriendlyURI(parentFolder),
                  HopVfs.getFriendlyURI(filename, variables)));
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to create parent folder for injected pipeline file: " + filename, e);
    }
  }

  /** Inject values from transforms. */
  private void newInjection(String targetTransform, ITransformMeta targetTransformMeta)
      throws HopException {
    if (isDetailed()) {
      logDetailed("Handing transform '" + targetTransform + "' injection!");
    }
    BeanInjectionInfo injectionInfo = new BeanInjectionInfo(targetTransformMeta.getClass());
    BeanInjector injector = new BeanInjector(injectionInfo, metadataProvider);

    // Collect all the metadata for this target transform...
    //
    boolean wasInjection = false;
    for (MetaInjectMapping mapping : meta.getMappings()) {
      if (mapping.getTargetTransformName().equalsIgnoreCase(targetTransform)
          && StringUtils.isNotEmpty(mapping.getSourceTransformName())) {
        // This is the transform to collect data for...
        // We also know which transform to read the data from. (source)
        //
        // from specified transform
        List<RowMetaAndData> rows = data.rowMap.get(mapping.getSourceTransformName());
        if (!Utils.isEmpty(rows)) {
          // Which metadata key is this referencing? Find the attribute key in the metadata
          // entries...
          //
          if (injector.hasProperty(targetTransformMeta, mapping.getTargetAttributeKey())) {
            // target transform has specified key
            boolean skip = false;
            for (RowMetaAndData r : rows) {
              if (r.getRowMeta().indexOfValue(mapping.getSourceField()) < 0) {
                logError(
                    BaseMessages.getString(
                        PKG,
                        "MetaInject.SourceFieldIsNotDefined.Message",
                        mapping.getSourceField(),
                        getPipelineMeta().getName()));
                // source transform doesn't contain specified field
                skip = true;
              }
            }
            if (!skip) {
              // specified field exist - need to inject
              injector.setProperty(
                  targetTransformMeta,
                  mapping.getTargetAttributeKey(),
                  rows,
                  mapping.getSourceField());
              wasInjection = true;
            }
          } else {
            // target transform doesn't have specified key - just report but don't fail like in
            logError(
                BaseMessages.getString(
                    PKG,
                    "MetaInject.TargetKeyIsNotDefined.Message",
                    mapping.getTargetAttributeKey(),
                    getPipelineMeta().getName()));
          }
        }
      }
    }
    if (wasInjection) {
      injector.runPostInjectionProcessing(targetTransformMeta);
    }
  }

  /**
   * Inject constant values.
   *
   * @param variables
   * @param targetTransform
   * @param targetTransformMeta
   */
  private void newInjectionConstants(
      IVariables variables, String targetTransform, ITransformMeta targetTransformMeta)
      throws HopException {
    if (isDetailed()) {
      logDetailed("Handing transform '" + targetTransform + "' constants injection!");
    }
    BeanInjectionInfo injectionInfo = new BeanInjectionInfo(targetTransformMeta.getClass());
    BeanInjector injector = new BeanInjector(injectionInfo, metadataProvider);

    // Collect all the metadata for this target transform...
    //
    for (MetaInjectMapping mapping : meta.getMappings()) {

      if (mapping.getTargetTransformName().equalsIgnoreCase(targetTransform)
          && StringUtils.isNotEmpty(mapping.getSourceTransformName())) {
        // This is the transform to collect data for...
        // We also know which transform to read the data from. (source)
        //
        // inject constant
        if (injector.hasProperty(targetTransformMeta, mapping.getTargetAttributeKey())) {
          // target transform has specified key
          String value = variables.resolve(mapping.getSourceField());
          injector.setProperty(targetTransformMeta, mapping.getTargetAttributeKey(), null, value);
        } else {
          // target transform doesn't have specified key - just report but don't fail like in 6.0
          logError(
              BaseMessages.getString(
                  PKG,
                  "MetaInject.TargetKeyIsNotDefined.Message",
                  mapping.getTargetAttributeKey(),
                  getPipelineMeta().getName()));
        }
      }
    }
  }

  private void copyResult(Pipeline trans) {
    Result result = trans.getResult();
    setLinesInput(result.getNrLinesInput());
    setLinesOutput(result.getNrLinesOutput());
    setLinesRead(result.getNrLinesRead());
    setLinesWritten(result.getNrLinesWritten());
    setLinesUpdated(result.getNrLinesUpdated());
    setLinesRejected(result.getNrLinesRejected());
    setErrors(result.getNrErrors());
  }

  @Override
  public boolean init() {

    if (super.init()) {
      try {
        data.pipelineMeta = loadPipelineMeta();
        checkSourceTransformsAvailability();
        checkTargetTransformsAvailability();
        // Get a mapping between the transform name and the injection...
        //
        // Get new injection info
        data.transformInjectionMetasMap = new HashMap<>();
        for (TransformMeta transformMeta : data.pipelineMeta.getUsedTransforms()) {
          ITransformMeta meta = transformMeta.getTransform();
          if (BeanInjectionInfo.isInjectionSupported(meta.getClass())) {
            data.transformInjectionMetasMap.put(transformMeta.getName(), meta);
          }
        }

        // See if we need to stream data from a specific transform into the template
        //
        if (StringUtils.isNotEmpty(meta.getStreamSourceTransformName())) {
          data.streaming = true;
          data.streamingSourceTransformName = meta.getStreamSourceTransformName();
          data.streamingTargetTransformName = meta.getStreamTargetTransformName();
        }
        return true;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MetaInject.BadEncoding.Message"), e);
        return false;
      }
    }

    return false;
  }

  private void checkTargetTransformsAvailability() {
    Set<String> unavailableTargetTransforms =
        getUnavailableTargetTransforms(meta.getMappings(), data.pipelineMeta);
    for (String unavailableTargetTransform : unavailableTargetTransforms) {
      logError(
          BaseMessages.getString(
              PKG,
              "MetaInject.TargetTransformIsNotDefined.Message",
              unavailableTargetTransform,
              data.pipelineMeta.getName()));
    }
  }

  public static void removeUnavailableTransformsFromMapping(
      List<MetaInjectMapping> mappings,
      Set<String> unavailableSourceTransforms,
      Set<String> unavailableTargetTransforms) {
    mappings.removeIf(
        mapping ->
            unavailableSourceTransforms.contains(mapping.getSourceTransformName())
                || unavailableTargetTransforms.contains(mapping.getTargetTransformName()));
  }

  public static Set<String> getUnavailableTargetTransforms(
      List<MetaInjectMapping> mappings, PipelineMeta injectedPipelineMeta) {
    // If we remove the available transforms from the used set,
    // the ones that are left are the unavailable target transforms.
    //
    Set<String> availableTargetTransforms = getTransformsInPipeline(injectedPipelineMeta);
    Set<String> usedTargetTransforms = getUsedTargetTransforms(mappings);
    usedTargetTransforms.removeAll(availableTargetTransforms);
    return Collections.unmodifiableSet(usedTargetTransforms);
  }

  public static Set<MetaInjectMapping> getUnavailableTargetKeys(
      List<MetaInjectMapping> mappings,
      PipelineMeta injectedPipelineMeta,
      Set<String> unavailableTargetTransforms) {
    Set<MetaInjectMapping> missingKeys = new HashSet<>();
    Map<String, BeanInjectionInfo> beanInfos = getUsedTransformBeanInfos(injectedPipelineMeta);
    for (MetaInjectMapping mapping : mappings) {
      if (!unavailableTargetTransforms.contains(mapping.getTargetTransformName())) {
        BeanInjectionInfo info = beanInfos.get(mapping.getTargetTransformName().toUpperCase());
        if (info != null && !info.getProperties().containsKey(mapping.getTargetAttributeKey())) {
          missingKeys.add(mapping);
        }
      }
    }
    return missingKeys;
  }

  private static Map<String, BeanInjectionInfo> getUsedTransformBeanInfos(
      PipelineMeta pipelineMeta) {
    Map<String, BeanInjectionInfo> res = new HashMap<>();
    for (TransformMeta transformMeta : pipelineMeta.getUsedTransforms()) {
      Class<? extends ITransformMeta> transformMetaClass = transformMeta.getTransform().getClass();
      if (BeanInjectionInfo.isInjectionSupported(transformMetaClass)) {
        res.put(transformMeta.getName().toUpperCase(), new BeanInjectionInfo(transformMetaClass));
      }
    }
    return res;
  }

  private static Set<String> getTransformsInPipeline(PipelineMeta pipelineMeta) {
    Set<String> usedTransformNames = new HashSet<>();
    for (TransformMeta currentTransform : pipelineMeta.getUsedTransforms()) {
      usedTransformNames.add(currentTransform.getName().toUpperCase());
    }
    return usedTransformNames;
  }

  private static Set<String> getUsedSourceTransforms(List<MetaInjectMapping> mappings) {
    Set<String> transforms = new HashSet<>();
    for (MetaInjectMapping mapping : mappings) {
      // Watch out for constant values where the source transform is not specified.
      if (StringUtils.isNotEmpty(mapping.getSourceTransformName())) {
        transforms.add(mapping.getSourceTransformName().toUpperCase());
      }
    }
    return transforms;
  }

  private static Set<String> getUsedTargetTransforms(List<MetaInjectMapping> mappings) {
    Set<String> transforms = new HashSet<>();
    for (MetaInjectMapping mapping : mappings) {
      transforms.add(mapping.getTargetTransformName().toUpperCase());
    }
    return transforms;
  }

  public static Set<String> getUnavailableSourceTransforms(
      List<MetaInjectMapping> mappings, PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    // The unavailable source transforms are the used ones minus the available
    //
    Set<String> availableSourceTransforms =
        new HashSet<>(convertToUpperCaseSet(pipelineMeta.getPrevTransformNames(transformMeta)));
    Set<String> usedSourceTransforms = getUsedSourceTransforms(mappings);
    usedSourceTransforms.removeAll(availableSourceTransforms);
    return Collections.unmodifiableSet(usedSourceTransforms);
  }

  private void checkSourceTransformsAvailability() {
    Set<String> unavailableSourceTransforms =
        getUnavailableSourceTransforms(meta.getMappings(), getPipelineMeta(), getTransformMeta());

    for (String unavailableTransform : unavailableSourceTransforms) {
      logError(
          BaseMessages.getString(
              PKG,
              "MetaInject.SourceTransformIsNotAvailable.Message",
              unavailableTransform,
              getPipelineMeta().getName()));
    }
  }

  /** package-local visibility for testing purposes */
  static Set<String> convertToUpperCaseSet(String[] array) {
    if (array == null) {
      return Collections.emptySet();
    }
    Set<String> strings = new HashSet<>();
    for (String currentString : array) {
      strings.add(currentString.toUpperCase());
    }
    return strings;
  }

  /** package-local visibility for testing purposes */
  PipelineMeta loadPipelineMeta() throws HopException {
    return MetaInjectMeta.loadPipelineMeta(meta, getPipeline().getMetadataProvider(), this);
  }
}
