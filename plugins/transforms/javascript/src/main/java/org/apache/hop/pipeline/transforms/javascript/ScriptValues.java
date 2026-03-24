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

package org.apache.hop.pipeline.transforms.javascript;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.JavaScriptUtils;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

/**
 * Executes a JavaScript on the values in the input stream. Selected calculated values can then be
 * put on the output stream.
 */
@Getter
@Setter
public class ScriptValues extends BaseTransform<ScriptValuesMeta, ScriptValuesData> {
  private static final Class<?> PKG = ScriptValuesMeta.class;

  /** Excludes the current row from the output row set and continues processing on the next row. */
  public static final int SKIP_PIPELINE = 1;

  /**
   * Excludes the current row from the output row set, and any remaining rows are not processed, but
   * does not generate an error.
   */
  public static final int ABORT_PIPELINE = -1;

  /**
   * Excludes the current row from the output row set, generates an error, and any remaining rows
   * are not processed.
   */
  public static final int ERROR_PIPELINE = -2;

  /** Includes the current row in the output row set. */
  public static final int CONTINUE_PIPELINE = 0;

  private boolean bWithPipelineStat = false;

  private boolean bRC = false;

  private boolean bFirstRun = false;

  private final List<ScriptValuesScript> jsScripts;

  private String strTransformScript = "";

  private String strStartScript = "";

  private String strEndScript = "";

  private Script script;

  public ScriptValues(
      TransformMeta transformMeta,
      ScriptValuesMeta meta,
      ScriptValuesData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    jsScripts = new ArrayList<>();
  }

  private void determineUsedFields(IRowMeta row) {
    int nr = 0;
    // Count the occurrences of the values.
    // Perhaps we find values in comments, but we take no risk!
    //
    for (int i = 0; i < row.size(); i++) {
      String valueName = row.getValueMeta(i).getName().toUpperCase();
      if (strTransformScript.toUpperCase().contains(valueName)) {
        nr++;
      }
    }

    // Allocate fields_used
    data.fieldsUsed = new int[nr];

    nr = 0;
    // Count the occurrences of the values.
    // Perhaps we find values in comments, but we take no risk!
    //
    for (int i = 0; i < row.size(); i++) {
      // Values are case-insensitive in JavaScript.
      //
      String valname = row.getValueMeta(i).getName();
      if (strTransformScript.contains(valname)) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ScriptValuesMod.Log.UsedValueName", String.valueOf(i), valname));
        }
        data.fieldsUsed[nr] = i;
        nr++;
      }
    }

    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ScriptValuesMod.Log.UsingValuesFromInputStream",
              String.valueOf(data.fieldsUsed.length)));
    }
  }

  @SuppressWarnings("deprecation")
  private void addValuesToContext(IRowMeta rowMeta, Object[] row) throws HopException {
    int iPipelineStat = CONTINUE_PIPELINE;
    if (first) {
      first = false;

      // What is the output row looking like?
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Determine the indexes of the fields used!
      //
      determineUsedFields(rowMeta);

      // Get the indexes of the replaced fields...
      //
      data.replaceIndex = new int[meta.getScriptFields().size()];
      for (int i = 0; i < meta.getScriptFields().size(); i++) {
        ScriptValuesMeta.ScriptField field = meta.getScriptFields().get(i);
        if (field.isReplace()) {
          data.replaceIndex[i] = rowMeta.indexOfValue(field.getName());
          if (data.replaceIndex[i] < 0) {
            if (Utils.isEmpty(field.getName())) {
              throw new HopTransformException(
                  BaseMessages.getString(
                      PKG,
                      "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound",
                      field.getName()));
            }
            data.replaceIndex[i] = rowMeta.indexOfValue(field.getRename());
            if (data.replaceIndex[i] < 0) {
              throw new HopTransformException(
                  BaseMessages.getString(
                      PKG,
                      "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound",
                      field.getRename()));
            }
          }
        } else {
          data.replaceIndex[i] = -1;
        }
      }

      // set the optimization level
      data.context = ContextFactory.getGlobal().enterContext();

      try {
        String optimizationLevelAsString = resolve(meta.getOptimizationLevel());
        if (!Utils.isEmpty(Const.trim(optimizationLevelAsString))) {
          data.context.setOptimizationLevel(Integer.parseInt(optimizationLevelAsString.trim()));
          if (isBasic()) {
            logBasic(
                BaseMessages.getString(
                    PKG,
                    "ScriptValuesMod.Optimization.Level",
                    resolve(meta.getOptimizationLevel())));
          }
        } else {
          data.context.setOptimizationLevel(
              Integer.parseInt(ScriptValuesMeta.OPTIMIZATION_LEVEL_DEFAULT));
          if (isBasic()) {
            logBasic(
                BaseMessages.getString(
                    PKG,
                    "ScriptValuesMod.Optimization.UsingDefault",
                    ScriptValuesMeta.OPTIMIZATION_LEVEL_DEFAULT));
          }
        }
      } catch (NumberFormatException nfe) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "ScriptValuesMetaMod.Exception.NumberFormatException",
                resolve(meta.getOptimizationLevel())));
      } catch (IllegalArgumentException iae) {
        throw new HopException(iae.getMessage());
      }

      data.scope = data.context.initStandardObjects(null, false);

      bFirstRun = true;

      Scriptable jsTransform = Context.toObject(this, data.scope);
      data.scope.put("_transform_", data.scope, jsTransform);

      // Adding the existing Scripts to the Context
      for (int i = 0; i < meta.getJsScripts().size(); i++) {
        Scriptable jsR = Context.toObject(jsScripts.get(i).getScript(), data.scope);
        data.scope.put(jsScripts.get(i).getName(), data.scope, jsR);
      }

      // Adding the Name of the Pipeline to the Context
      data.scope.put("_PipelineName_", data.scope, getPipelineMeta().getName());

      try {
        // Add the used fields...
        //
        addFieldsToScope(rowMeta, row);

        // Add the old style row object for compatibility reasons.
        // also add the meta information for the whole row.
        //
        addRowAndRowMetaToScope(rowMeta, row);

        // Adding some default JavaScriptFunctions to the System
        addFunctionsToScope();

        // Adding some Constants to the JavaScript
        addConstantsToScope();

        // Checking for StartScript
        checkForStartScript();

        // Now Compile our Script
        data.script = data.context.compileString(strTransformScript, "script", 1, null);
      } catch (Exception e) {
        throw new HopValueException(
            BaseMessages.getString(PKG, "ScriptValuesMod.Log.CouldNotCompileJavascript"), e);
      }
    }

    // Filling the defined TranVars with the Values from the Row
    //
    Object[] outputRow = RowDataUtil.resizeArray(row, data.outputRowMeta.size());

    // Keep an index...
    int outputIndex = rowMeta.size();

    try {
      addFieldsToScope(rowMeta, row);
      addRowAndRowMetaToScope(rowMeta, row);

      // Executing our Script
      data.script.exec(data.context, data.scope, data.scope);

      if (bFirstRun) {
        bFirstRun = false;
        // Check if we had a Pipeline Status
        Object pipelineStatus = data.scope.get("pipeline_Status", data.scope);
        if (pipelineStatus != Scriptable.NOT_FOUND) {
          bWithPipelineStat = true;
          if (isDetailed()) {
            logDetailed(
                ("pipeline_Status found. Checking pipeline status while script execution."));
          }
        } else {
          if (isDetailed()) {
            logDetailed(("No pipeline_Status found. Pipeline status checking not available."));
          }
          bWithPipelineStat = false;
        }
      }

      if (bWithPipelineStat) {
        iPipelineStat = (int) Context.toNumber(data.scope.get("pipeline_Status", data.scope));
      }

      if (iPipelineStat == CONTINUE_PIPELINE) {
        bRC = true;
        for (int i = 0; i < meta.getScriptFields().size(); i++) {
          ScriptValuesMeta.ScriptField field = meta.getScriptFields().get(i);
          Object result = data.scope.get(field.getName(), data.scope);
          Object valueData = getValueFromJScript(result, i);
          if (data.replaceIndex[i] < 0) {
            outputRow[outputIndex++] = valueData;
          } else {
            outputRow[data.replaceIndex[i]] = valueData;
          }
        }

        // Also modify the "in-place" value changes:
        // --> the field.trim() type of changes...
        // As such we overwrite all the used fields again.
        //
        putRow(data.outputRowMeta, outputRow);
      } else {
        switch (iPipelineStat) {
          case SKIP_PIPELINE:
            // eat this row.
            bRC = true;
            break;
          case ABORT_PIPELINE:
            if (data.context != null) {
              Context.exit();
            }
            stopAll();
            setOutputDone();
            bRC = false;
            break;
          case ERROR_PIPELINE:
            if (data.context != null) {
              Context.exit();
            }
            setErrors(1);
            stopAll();
            bRC = false;
            break;
          default:
            break;
        }
      }
    } catch (Exception e) {
      throw new HopValueException(
          BaseMessages.getString(PKG, "ScriptValuesMod.Log.JavascriptError"), e);
    }
  }

  private void addRowAndRowMetaToScope(IRowMeta rowMeta, Object[] row) {
    Scriptable jsRow = Context.toObject(row, data.scope);
    data.scope.put("row", data.scope, jsRow);
    Scriptable jsRowMeta = Context.toObject(rowMeta, data.scope);
    data.scope.put("rowMeta", data.scope, jsRowMeta);
  }

  private void checkForStartScript() throws HopValueException {
    try {
      if (!Utils.isEmpty(strStartScript)) {
        Script startScript = data.context.compileString(strStartScript, "pipeline_Start", 1, null);
        startScript.exec(data.context, data.scope, data.scope);
        if (isDetailed()) {
          logDetailed(("Start Script found!"));
        }
      } else {
        if (isDetailed()) {
          logDetailed(("No starting Script found!"));
        }
      }
    } catch (Exception es) {
      throw new HopValueException(
          BaseMessages.getString(PKG, "ScriptValuesMod.Log.ErrorProcessingStartScript"), es);
    }
  }

  private void addFieldsToScope(IRowMeta rowMeta, Object[] row) throws HopValueException {
    for (int i = 0; i < data.fieldsUsed.length; i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(data.fieldsUsed[i]);
      Object valueData = row[data.fieldsUsed[i]];

      Object normalStorageValueData = valueMeta.convertToNormalStorageType(valueData);
      Scriptable jsarg;
      if (normalStorageValueData != null) {
        jsarg = Context.toObject(normalStorageValueData, data.scope);
      } else {
        jsarg = null;
      }
      data.scope.put(valueMeta.getName(), data.scope, jsarg);
    }
  }

  private void addFunctionsToScope() throws HopValueException {
    try {
      Context.javaToJS(ScriptValuesAddedFunctions.class, data.scope);
      ((ScriptableObject) data.scope)
          .defineFunctionProperties(
              ScriptValuesAddedFunctions.jsFunctionList,
              ScriptValuesAddedFunctions.class,
              ScriptableObject.DONTENUM);
    } catch (Exception ex) {
      throw new HopValueException(
          BaseMessages.getString(PKG, "ScriptValuesMod.Log.CouldNotAddDefaultFunctions"), ex);
    }
  }

  private void addConstantsToScope() throws HopValueException {
    try {
      data.scope.put("SKIP_PIPELINE", data.scope, SKIP_PIPELINE);
      data.scope.put("ABORT_PIPELINE", data.scope, ABORT_PIPELINE);
      data.scope.put("ERROR_PIPELINE", data.scope, ERROR_PIPELINE);
      data.scope.put("CONTINUE_PIPELINE", data.scope, CONTINUE_PIPELINE);
    } catch (Exception ex) {
      throw new HopValueException(
          BaseMessages.getString(PKG, "ScriptValuesMod.Log.CouldNotAddDefaultConstants"), ex);
    }
  }

  public Object getValueFromJScript(Object result, int i) throws HopValueException {
    ScriptValuesMeta.ScriptField field = meta.getScriptFields().get(i);
    if (!Utils.isEmpty(field.getName())) {
      try {
        return (result == null)
            ? null
            : JavaScriptUtils.convertFromJs(result, field.getType(), field.getName());
      } catch (Exception e) {
        throw new HopValueException(
            BaseMessages.getString(PKG, "ScriptValuesMod.Log.JavascriptError"), e);
      }
    } else {
      throw new HopValueException("No name was specified for result value #" + (i + 1));
    }
  }

  public IRowMeta getOutputRowMeta() {
    return data.outputRowMeta;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) {
      // Modification for Additional End Function
      processEndOfStream();

      setOutputDone();
      return false;
    }

    // Getting the Row, with the Pipeline Status
    try {
      addValuesToContext(getInputRowMeta(), r);
    } catch (HopValueException e) {
      String location = null;
      if (e.getCause() instanceof EvaluatorException evaluatorException) {
        location =
            "--> " + evaluatorException.lineNumber() + ":" + evaluatorException.columnNumber();
      }

      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), r, 1, e.getMessage() + Const.CR + location, null, "SCR-001");
        bRC = true; // continue by all means, even on the first row and out of this ugly design
      } else {
        throw (e);
      }
    }

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ScriptValuesMod.Log.LineNumber") + getLinesRead());
    }
    return bRC;
  }

  private void processEndOfStream() {
    try {
      if (data.context != null) {
        // Checking for EndScript
        if (!Utils.isEmpty(strEndScript)) {
          Script endScript = data.context.compileString(strEndScript, "pipeline_End", 1, null);
          endScript.exec(data.context, data.scope, data.scope);
          if (isDetailed()) {
            logDetailed(("End Script found!"));
          }
        } else {
          if (isDetailed()) {
            logDetailed(("No end Script found!"));
          }
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ScriptValuesMod.Log.UnexpectedeError")
              + " : "
              + e.toString());
      logError(
          BaseMessages.getString(PKG, "ScriptValuesMod.Log.ErrorStackTrace")
              + Const.CR
              + Const.getSimpleStackTrace(e)
              + Const.CR
              + Const.getStackTracker(e));
      setErrors(1);
      stopAll();
    }

    try {
      if (data.context != null) {
        Context.exit();
      }
    } catch (Exception er) {
      // Eat this error, it's typically : "Calling Context.exit without previous Context.enter"
    }
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    // Add init code here.
    // Get the actual Scripts from our MetaData
    //
    jsScripts.clear();
    meta.getJsScripts().forEach(jsScript -> jsScripts.add(new ScriptValuesScript(jsScript)));
    for (ScriptValuesScript jsScript : jsScripts) {
      switch (jsScript.getType()) {
        case ScriptValuesScript.TRANSFORM_SCRIPT:
          strTransformScript = jsScript.getScript();
          break;
        case ScriptValuesScript.START_SCRIPT:
          strStartScript = jsScript.getScript();
          break;
        case ScriptValuesScript.END_SCRIPT:
          strEndScript = jsScript.getScript();
          break;
        default:
          break;
      }
    }
    return true;
  }

  @Override
  public void dispose() {
    try {
      if (data.context != null) {
        Context.exit();
      }
    } catch (Exception er) {
      // Eat this error, it's typically : "Calling Context.exit without previous Context.enter"
    }

    super.dispose();
  }
}
