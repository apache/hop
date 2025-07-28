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
 *
 */

package org.apache.hop.pipeline.transforms.script;

import java.math.BigDecimal;
import java.util.Date;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Executes a JSR-223 compiledScript on the values in the input stream. Selected calculated values
 * can then be put on the output stream.
 *
 * @author Matt Burgess
 */
public class Script extends BaseTransform<ScriptMeta, ScriptData> implements ITransform {
  private static final Class<?> PKG = Script.class;

  public static final int ABORT_PIPELINE = -1;
  public static final int ERROR_PIPELINE = -2;
  public static final int CONTINUE_PIPELINE = 0;
  public static final int SKIP_PIPELINE = 1;
  public static final String CONST_ORG_MOZILLA_JAVASCRIPT_UNDEFINED =
      "org.mozilla.javascript.Undefined";
  public static final String CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_NUMBER =
      "org.mozilla.javascript.NativeNumber";
  public static final String CONST_JAVA_LANG_DOUBLE = "java.lang.Double";
  public static final String CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_JAVA_OBJECT =
      "org.mozilla.javascript.NativeJavaObject";
  public static final String CONST_PREVIOUS_ROW = "previousRow";

  private boolean bWithPipelineStat = false;

  private boolean bRC = false;

  private boolean bFirstRun = false;

  private int rowNumber = 0;

  private String strTransformScript = "";

  private String strStartScript = "";

  private String strEndScript = "";

  private Bindings bindings;

  private Object[] previousRow = null;

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public Script(
      TransformMeta transformMeta,
      ScriptMeta meta,
      ScriptData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void determineUsedFields(IRowMeta row) {
    if (row == null) {
      return;
    }

    int nr = 0;
    // Count the occurrences of the values.
    // Perhaps we find values in comments, but we take no risk!
    //
    for (int i = 0; i < row.size(); i++) {
      String valName = row.getValueMeta(i).getName().toUpperCase();
      if (strTransformScript.toUpperCase().contains(valName)) {
        nr++;
      }
    }

    // Allocate fieldsUsed
    data.fieldsUsed = new int[nr];
    data.valuesUsed = new Object[nr];

    nr = 0;
    // Count the occurrences of the values.
    // Perhaps we find values in comments, but we take no risk!
    //
    for (int i = 0; i < row.size(); i++) {
      // Values are case-insensitive in JavaScript.
      //
      String valName = row.getValueMeta(i).getName();
      if (strTransformScript.contains(valName)) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "Script.Log.UsedValueName", String.valueOf(i), valName)); // $NON-NLS-3$
        }
        data.fieldsUsed[nr] = i;
        nr++;
      }
    }

    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "Script.Log.UsingValuesFromInputStream",
              String.valueOf(data.fieldsUsed.length)));
    }
  }

  private boolean addValues(IRowMeta rowMeta, Object[] row) throws HopException {
    if (first) {
      first = false;

      if (rowMeta == null) {
        rowMeta = new RowMeta();
      }
      data.outputRowMeta = rowMeta.clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Determine the indexes of the fields used!
      //
      determineUsedFields(rowMeta);

      // Get the indexes of the replaced fields...
      //
      data.replaceIndex = new int[meta.getFields().size()];
      for (int i = 0; i < meta.getFields().size(); i++) {
        ScriptMeta.SField field = meta.getFields().get(i);
        if (field.isReplace()) {
          data.replaceIndex[i] = rowMeta.indexOfValue(field.getName());
          if (data.replaceIndex[i] < 0) {
            if (StringUtils.isEmpty(field.getName())) {
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

      data.context = data.engine.getContext();
      if (data.context == null) {
        data.context = new SimpleScriptContext();
      }
      bindings = data.engine.getBindings(ScriptContext.ENGINE_SCOPE);

      bFirstRun = true;

      bindings.put("transform", this);
      // Adding the Name of the Transformation to the Context
      //
      bindings.put("transformName", getTransformName());
      bindings.put("pipelineName", getPipelineMeta().getName());

      // Adding the existing Scripts to the Context
      //
      for (int i = 0; i < meta.getScripts().size(); i++) {
        ScriptMeta.SScript script = meta.getScripts().get(i);
        bindings.put(script.getScriptName(), script.getScript());
      }

      try {
        addRowBindings(rowMeta, row);

        // Adding some Constants to the compiledScript
        addConstantBindings();

        try {
          // Checking for StartScript
          if (StringUtils.isNotEmpty(strStartScript)) {
            if (isDetailed()) {
              logDetailed(("Start compiledScript found!"));
            }
            if (data.engine instanceof Compilable compilable) {
              CompiledScript startScript = compilable.compile(strStartScript);
              startScript.eval(bindings);
            } else {
              // Can't compile beforehand, so just eval it
              data.engine.eval(strStartScript);
            }

          } else {
            if (isDetailed()) {
              logDetailed(("No starting compiledScript found!"));
            }
          }
        } catch (Exception es) {
          throw new HopValueException(
              BaseMessages.getString(PKG, "Script.Log.ErrorProcessingStartScript"), es);
        }

        data.rawScript = strTransformScript;
        // Now Compile our Script if supported by the engine
        if (data.engine instanceof Compilable compilable) {
          data.compiledScript = compilable.compile(strTransformScript);

        } else {
          data.compiledScript = null;
        }
      } catch (Exception e) {
        throw new HopValueException(
            BaseMessages.getString(PKG, "Script.Log.CouldNotCompileScript"), e);
      }
    }

    bindings.put("rowNumber", ++rowNumber);

    // Filling the defined TranVars with the Values from the Row
    //
    Object[] outputRow = RowDataUtil.resizeArray(row, data.outputRowMeta.size());

    // Keep an index...
    int outputIndex = rowMeta == null ? 0 : rowMeta.size();

    try {
      try {
        bindings.put("row", row);

        // Try to add the last row's data (null or not)
        try {
          bindings.put(CONST_PREVIOUS_ROW, previousRow);
        } catch (Exception t) {
          logError(
              BaseMessages.getString(
                  PKG, "Script.Exception.ErrorSettingVariable", CONST_PREVIOUS_ROW),
              t);
        }

        for (int i = 0; i < data.fieldsUsed.length; i++) {
          IValueMeta valueMeta = rowMeta.getValueMeta(data.fieldsUsed[i]);
          Object valueData = row[data.fieldsUsed[i]];

          Object normalStorageValueData = valueMeta.convertToNormalStorageType(valueData);

          bindings.put(valueMeta.getName(), normalStorageValueData);
        }

        // also add the meta information for the whole row
        //
        bindings.put("rowMeta", rowMeta);
      } catch (Exception e) {
        throw new HopValueException(BaseMessages.getString(PKG, "Script.Log.UnexpectedError"), e);
      }

      Object scriptResult = evalScript();

      if (bFirstRun) {
        bFirstRun = false;
        // Check if we had a Transformation Status
        Object statusVariable = bindings.get("pipeline_status");
        if (statusVariable != null) {
          bWithPipelineStat = true;
          if (isDetailed()) {
            logDetailed(
                ("Value pipeline_status found. Checking pipeline status while compiledScript execution."));
          }
        } else {
          if (isDetailed()) {
            logDetailed(
                ("No pipeline_status value found. Pipeline status checking not available."));
          }
          bWithPipelineStat = false;
        }
      }

      int pipelineStatus = CONTINUE_PIPELINE;
      if (bWithPipelineStat) {
        Object statusVariable = bindings.get("pipeline_status");
        if (Integer.class.isAssignableFrom(statusVariable.getClass())) {
          pipelineStatus = (Integer) statusVariable;
        }
      }

      if (pipelineStatus == CONTINUE_PIPELINE) {
        bRC = true;
        for (int i = 0; i < meta.getFields().size(); i++) {
          ScriptMeta.SField field = meta.getFields().get(i);
          Object result = bindings.get(field.getName());
          Object valueData = getValueFromScript(field.isScriptResult() ? scriptResult : result, i);
          if (data.replaceIndex[i] < 0) {
            outputRow[outputIndex++] = valueData;
          } else {
            outputRow[data.replaceIndex[i]] = valueData;
          }
        }

        putRow(data.outputRowMeta, outputRow);
      } else {
        switch (pipelineStatus) {
          case SKIP_PIPELINE:
            // eat this row.
            bRC = true;
            break;
          case ABORT_PIPELINE:
            if (data.engine != null) {
              stopAll();
            }
            setOutputDone();
            bRC = false;
            break;
          case ERROR_PIPELINE:
            if (data.engine != null) {
              setErrors(1);
            }
            stopAll();
            bRC = false;
            break;
          default:
            break;
        }
      }
    } catch (ScriptException e) {
      throw new HopValueException(BaseMessages.getString(PKG, "Script.Log.ScriptError"), e);
    }
    return bRC;
  }

  private void addRowBindings(IRowMeta rowMeta, Object[] row) {
    try {
      bindings.put("row", row);
      bindings.put(CONST_PREVIOUS_ROW, previousRow);

      // also add the meta information for the whole row
      //
      bindings.put("rowMeta", rowMeta);
      bindings.put("outputRowMeta", data.outputRowMeta);

      // Add the used fields...
      //
      if (data.fieldsUsed != null) {
        for (int i = 0; i < data.fieldsUsed.length; i++) {
          IValueMeta valueMeta = rowMeta.getValueMeta(data.fieldsUsed[i]);
          Object valueData = row[data.fieldsUsed[i]];

          Object normalStorageValueData = valueMeta.convertToNormalStorageType(valueData);
          bindings.put(valueMeta.getName(), normalStorageValueData);
        }
      }

    } catch (Exception t) {
      logError(BaseMessages.getString(PKG, "Script.Exception.ErrorSettingVariable"), t);
    }
  }

  private void addConstantBindings() throws HopValueException {
    try {
      bindings.put("SKIP_PIPELINE", SKIP_PIPELINE);
      bindings.put("ABORT_PIPELINE", ABORT_PIPELINE);
      bindings.put("ERROR_PIPELINE", ERROR_PIPELINE);
      bindings.put("CONTINUE_PIPELINE", CONTINUE_PIPELINE);

      bindings.put("RowDataUtil", RowDataUtil.class);

    } catch (Exception ex) {
      throw new HopValueException(
          BaseMessages.getString(PKG, "Script.Log.CouldNotAddDefaultConstants"), ex);
    }
  }

  protected Object evalScript() throws ScriptException {
    if (data.compiledScript != null) {
      try {
        return data.compiledScript.eval(data.context);
      } catch (UnsupportedOperationException uoe) {
        // The script engine might not support eval with script context, so try just the Bindings
        // instead
        return data.compiledScript.eval(bindings);
      }

    } else if (data.engine != null && data.rawScript != null) {
      try {
        return data.engine.eval(data.rawScript, data.context);
      } catch (UnsupportedOperationException uoe) {
        // The script engine might not support eval with script context, so try just the Bindings
        // instead
        return data.engine.eval(data.rawScript, bindings);
      }
    }
    return null;
  }

  public Object getValueFromScript(Object result, int i) throws HopValueException {
    ScriptMeta.SField field = meta.getFields().get(i);

    if (StringUtils.isNotEmpty(field.getName())) {
      try {
        if (result != null) {
          String classType = result.getClass().getName();
          switch (field.getHopType()) {
            case IValueMeta.TYPE_NUMBER:
              if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_UNDEFINED)) {
                return null;
              } else if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_NUMBER)
                  || Number.class.isAssignableFrom(result.getClass())) {
                Number nb = (Number) result;
                return nb.doubleValue();
              } else {
                // Last resort, try to parse from toString()
                return Double.parseDouble(result.toString());
              }

            case IValueMeta.TYPE_INTEGER:
              if (classType.equalsIgnoreCase("java.lang.Byte")) {
                return ((Byte) result).longValue();
              } else if (classType.equalsIgnoreCase("java.lang.Short")) {
                return ((Short) result).longValue();
              } else if (classType.equalsIgnoreCase("java.lang.Integer")) {
                return ((Integer) result).longValue();
              } else if (classType.equalsIgnoreCase("java.lang.Long")) {
                return result;
              } else if (classType.equalsIgnoreCase(CONST_JAVA_LANG_DOUBLE)) {
                return ((Double) result).longValue();
              } else if (classType.equalsIgnoreCase("java.lang.String")) {
                return Long.parseLong((String) result);
              } else if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_UNDEFINED)) {
                return null;
              } else if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_NUMBER)) {
                Number nb = (Number) result;
                return nb.longValue();
              } else if (classType.equalsIgnoreCase(
                  CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_JAVA_OBJECT)) {
                try {
                  return Long.parseLong(result.toString());
                } catch (Exception e2) {
                  String string = (String) result;
                  return Long.parseLong(Const.trim(string));
                }
              } else {
                return Long.parseLong(result.toString());
              }

            case IValueMeta.TYPE_STRING:
              if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_JAVA_OBJECT)
                  || classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_UNDEFINED)) {
                // Is it a java Value class ?
                try {
                  return result.toString();
                } catch (Exception ev) {
                  // convert to a string should work in most
                  // cases...
                  //
                  return result;
                }
              } else {
                // A String perhaps?
                return result;
              }

            case IValueMeta.TYPE_DATE:
              double dbl = 0;
              if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_UNDEFINED)) {
                return null;
              } else {
                if (classType.equalsIgnoreCase("org.mozilla.javascript.NativeDate")) {
                  dbl = (Double) result;
                } else if (classType.equalsIgnoreCase(
                        CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_JAVA_OBJECT)
                    || classType.equalsIgnoreCase("java.util.Date")) {
                  // Is it a java Date() class ?
                  try {
                    Date dat = (Date) result;
                    dbl = dat.getTime();
                  } catch (Exception e) {
                    // Is it a Value?
                    //
                    try {
                      String string = (String) result;
                      return XmlHandler.stringToDate(string);
                    } catch (Exception e3) {
                      throw new HopValueException("Can't convert a string to a date");
                    }
                  }
                } else if (classType.equalsIgnoreCase(CONST_JAVA_LANG_DOUBLE)) {
                  dbl = (Double) result;
                } else {
                  String string = (String) result;
                  dbl = Double.parseDouble(string);
                }
                long lng = Math.round(dbl);
                return new Date(lng);
              }

            case IValueMeta.TYPE_BOOLEAN:
              return result;

            case IValueMeta.TYPE_BIGNUMBER:
              if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_UNDEFINED)) {
                return null;
              } else if (classType.equalsIgnoreCase(CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_NUMBER)) {
                Number nb = (Number) result;
                // sure
                return new BigDecimal(nb.longValue());
              } else if (classType.equalsIgnoreCase(
                  CONST_ORG_MOZILLA_JAVASCRIPT_NATIVE_JAVA_OBJECT)) {
                // Is it a BigDecimal class ?
                try {
                  return (BigDecimal) result;
                } catch (Exception e) {
                  String string = (String) result;
                  return new BigDecimal(string);
                }
              } else if (classType.equalsIgnoreCase("java.lang.Byte")) {
                return new BigDecimal(((java.lang.Byte) result).longValue());
              } else if (classType.equalsIgnoreCase("java.lang.Short")) {
                return new BigDecimal(((Short) result).longValue());
              } else if (classType.equalsIgnoreCase("java.lang.Integer")) {
                return new BigDecimal(((Integer) result).longValue());
              } else if (classType.equalsIgnoreCase("java.lang.Long")) {
                return new BigDecimal((Long) result);
              } else if (classType.equalsIgnoreCase(CONST_JAVA_LANG_DOUBLE)) {
                return new BigDecimal(((Double) result).longValue());
              } else if (classType.equalsIgnoreCase("java.lang.String")) {
                return new BigDecimal(Long.parseLong((String) result));
              } else {
                throw new RuntimeException(
                    "JavaScript conversion to BigNumber not implemented for " + classType);
              }

            case IValueMeta.TYPE_BINARY:
              {
                return result;
              }
            case IValueMeta.TYPE_NONE:
              {
                throw new RuntimeException(
                    "No data output data type was specified for new field ["
                        + field.getName()
                        + "]");
              }
            default:
              {
                throw new RuntimeException(
                    "JavaScript conversion not implemented for type "
                        + field.getHopType()
                        + " ("
                        + field.getType()
                        + ")");
              }
          }
        } else {
          return null;
        }
      } catch (Exception e) {
        throw new HopValueException(BaseMessages.getString(PKG, "Script.Log.ScriptError"), e);
      }
    } else {
      throw new HopValueException("No name was specified for result value #" + (i + 1));
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();
    if (r == null && !first) {
      // Modification for Additional End Function
      try {
        if (data.engine != null) {

          // Run the start and transformation scripts once if there are no incoming rows

          // Checking for EndScript
          if (!Utils.isEmpty(strEndScript)) {
            data.engine.eval(strEndScript, bindings);
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
        logError(BaseMessages.getString(PKG, "Script.Log.UnexpectedError") + " : " + e);
        logError(
            BaseMessages.getString(PKG, "Script.Log.ErrorStackTrace")
                + Const.CR
                + Const.getStackTracker(e));
        setErrors(1);
        stopAll();
      }

      if (data.engine != null) {
        setOutputDone();
      }
      return false;
    }

    // Getting the Row, with the Pipeline Status
    try {
      addValues(getInputRowMeta(), r);
    } catch (HopValueException e) {
      String location = "<unknown>";
      if (e.getCause() instanceof ScriptException ee) {
        location = "--> " + ee.getLineNumber() + ":" + ee.getColumnNumber(); // $NON-NLS-1$
        //
      }

      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), r, 1, e.getMessage() + Const.CR + location, null, "SCR-001");
        bRC = true; // continue by all means, even on the first row and
        // out of this ugly design
      } else {
        logError(
            BaseMessages.getString(PKG, "Script.Exception.CouldNotExecuteScript", location), e);
        setErrors(1);
        bRC = false;
      }
    }

    if (checkFeedback(getLinesRead())) {
      logBasic(BaseMessages.getString(PKG, "Script.Log.LineNumber") + getLinesRead());
    }
    previousRow = r;
    return bRC;
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }

    // Add init code here.
    // Get the actual Scripts from our MetaData
    //
    for (ScriptMeta.SScript script : meta.getScripts()) {
      switch (script.getScriptType()) {
        case TRANSFORM_SCRIPT:
          strTransformScript = script.getScript();
          break;
        case START_SCRIPT:
          strStartScript = script.getScript();
          break;
        case END_SCRIPT:
          strEndScript = script.getScript();
          break;
        default:
          break;
      }
    }
    try {
      data.engine = ScriptUtils.getInstance().getScriptEngineByName(meta.getLanguageName());
    } catch (Exception e) {
      logError("Error obtaining scripting engine for language " + meta.getLanguageName(), e);
    }
    rowNumber = 0;
    previousRow = null;
    return true;
  }
}
