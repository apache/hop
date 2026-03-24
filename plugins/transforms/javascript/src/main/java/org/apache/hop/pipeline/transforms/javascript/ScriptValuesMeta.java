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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.JavaScriptException;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

@Transform(
    id = "ScriptValueMod",
    image = "javascript.svg",
    name = "i18n::ScriptValuesMod.Name",
    description = "i18n::ScriptValuesMod.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::ScriptValuesMeta.keyword",
    documentationUrl = "/pipeline/transforms/javascript.html")
@Getter
@Setter
public class ScriptValuesMeta extends BaseTransformMeta<ScriptValues, ScriptValuesData> {
  private static final Class<?> PKG = ScriptValuesMeta.class;

  public static final String OPTIMIZATION_LEVEL_DEFAULT = "9";

  @Getter
  @Setter
  public static class ScriptField {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FIELD_NAME",
        injectionKeyDescription = "ScriptValuesMod.Injection.FIELD_NAME")
    private String name;

    @HopMetadataProperty(
        key = "rename",
        injectionKey = "FIELD_RENAME_TO",
        injectionKeyDescription = "ScriptValuesMod.Injection.FIELD_RENAME_TO")
    private String rename;

    @HopMetadataProperty(
        key = "type",
        intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
        injectionKey = "FIELD_TYPE",
        injectionKeyDescription = "ScriptValuesMod.Injection.FIELD_TYPE")
    private int type;

    @HopMetadataProperty(
        key = "length",
        injectionKey = "FIELD_LENGTH",
        injectionKeyDescription = "ScriptValuesMod.Injection.FIELD_LENGTH")
    private int length;

    @HopMetadataProperty(
        key = "precision",
        injectionKey = "FIELD_PRECISION",
        injectionKeyDescription = "ScriptValuesMod.Injection.FIELD_PRECISION")
    private int precision;

    @HopMetadataProperty(
        key = "replace",
        injectionKey = "FIELD_REPLACE",
        injectionKeyDescription = "ScriptValuesMod.Injection.FIELD_REPLACE")
    private boolean replace;

    public ScriptField() {}

    public ScriptField(ScriptField f) {
      this.name = f.name;
      this.rename = f.rename;
      this.type = f.type;
      this.length = f.length;
      this.precision = f.precision;
      this.replace = f.replace;
    }
  }

  @HopMetadataProperty(
      key = "jsScript",
      groupKey = "jsScripts",
      injectionGroupKey = "SCRIPTS",
      injectionGroupDescription = "ScriptValuesMod.Injection.SCRIPTS")
  private List<ScriptValuesScript> jsScripts;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "ScriptValuesMod.Injection.FIELDS")
  private List<ScriptField> scriptFields;

  @HopMetadataProperty(
      key = "optimizationLevel",
      injectionKey = "OPTIMIZATION_LEVEL",
      injectionKeyDescription = "ScriptValuesMod.Injection.OPTIMIZATION_LEVEL")
  private String optimizationLevel;

  public ScriptValuesMeta() {
    super();
    jsScripts = new ArrayList<>();
    scriptFields = new ArrayList<>();
    optimizationLevel = OPTIMIZATION_LEVEL_DEFAULT;

    ScriptValuesScript script = new ScriptValuesScript();
    script.setType(ScriptValuesScript.TRANSFORM_SCRIPT);
    script.setName(BaseMessages.getString(PKG, "ScriptValuesMod.Script1"));
    script.setScript(
        "//" + BaseMessages.getString(PKG, "ScriptValuesMod.ScriptHere") + Const.CR + Const.CR);
    jsScripts.add(script);
  }

  public ScriptValuesMeta(ScriptValuesMeta m) {
    this();
    this.optimizationLevel = m.optimizationLevel;
    m.jsScripts.forEach(s -> this.jsScripts.add(new ScriptValuesScript(s)));
    m.scriptFields.forEach(f -> scriptFields.add(new ScriptField(f)));
  }

  @Override
  public Object clone() {
    return new ScriptValuesMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String originTransformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      for (ScriptField field : scriptFields) {
        if (!Utils.isEmpty(field.getName())) {
          int valueIndex;
          IValueMeta v;
          if (field.isReplace()) {
            valueIndex = row.indexOfValue(field.getName());
            if (valueIndex < 0) {
              // The field was not found using the "name" field
              if (Utils.isEmpty(field.getRename())) {
                // There is no "rename" field to try; Therefore we cannot find the
                // field to replace
                throw new HopTransformException(
                    BaseMessages.getString(
                        PKG,
                        "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound",
                        field.getName()));
              } else {
                // Lookup the field to replace using the "rename" field
                valueIndex = row.indexOfValue(field.getRename());
                if (valueIndex < 0) {
                  // The field was not found using the "rename" field"; Therefore
                  // we cannot find the field to replace
                  //
                  throw new HopTransformException(
                      BaseMessages.getString(
                          PKG,
                          "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound",
                          field.getRename()));
                }
              }
            }

            // Change the data type to match what's specified...
            //
            IValueMeta source = row.getValueMeta(valueIndex);
            v = ValueMetaFactory.cloneValueMeta(source, field.getType());
            row.setValueMeta(valueIndex, v);
          } else {
            if (!Utils.isEmpty(field.getRename())) {
              v = ValueMetaFactory.createValueMeta(field.getRename(), field.getType());
            } else {
              v = ValueMetaFactory.createValueMeta(field.getName(), field.getType());
            }
          }
          v.setLength(field.getLength());
          v.setPrecision(field.getPrecision());
          v.setOrigin(originTransformName);
          if (!field.isReplace()) {
            row.addValueMeta(v);
          }
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(e);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    String errorMessage = "";
    CheckResult cr;

    Context jsContext;
    ScriptableObject jsScope;

    jsContext = ContextFactory.getGlobal().enterContext();
    jsScope = jsContext.initStandardObjects(null, false);
    try {
      jsContext.setOptimizationLevel(Integer.parseInt(variables.resolve(optimizationLevel)));
    } catch (NumberFormatException nfe) {
      errorMessage =
          "Error with optimization level.  Could not convert the value of "
              + variables.resolve(optimizationLevel)
              + " to an integer.";
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } catch (IllegalArgumentException iae) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, iae.getMessage(), transformMeta);
      remarks.add(cr);
    }

    String strActiveStartScriptName = "";
    String strActiveEndScriptName = "";

    String strActiveScript = "";
    String strActiveStartScript = "";
    String strActiveEndScript = "";

    // Building the Scripts
    if (!jsScripts.isEmpty()) {
      for (ScriptValuesScript jsScript : jsScripts) {
        if (jsScript.isTransformScript()) {
          strActiveScript = jsScript.getScript();
        } else if (jsScript.isStartScript()) {
          strActiveStartScriptName = jsScript.getName();
          strActiveStartScript = jsScript.getScript();
        } else if (jsScript.isEndScript()) {
          strActiveEndScriptName = jsScript.getName();
          strActiveEndScript = jsScript.getScript();
        }
      }
    }

    if (prev != null && !strActiveScript.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "ScriptValuesMetaMod.CheckResult.ConnectedTransformOK",
                  String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);

      // Adding the existing Scripts to the Context
      for (ScriptValuesScript jsScript : jsScripts) {
        Scriptable jsR = Context.toObject(jsScript.getScript(), jsScope);
        jsScope.put(jsScript.getName(), jsScope, jsR);
      }

      // Adding some default JavaScriptFunctions to the System
      try {
        Context.javaToJS(ScriptValuesAddedFunctions.class, jsScope);
        jsScope.defineFunctionProperties(
            ScriptValuesAddedFunctions.jsFunctionList,
            ScriptValuesAddedFunctions.class,
            ScriptableObject.DONTENUM);
      } catch (Exception ex) {
        errorMessage = "Couldn't add Default Functions! Error:" + Const.CR + ex;
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      // Adding some Constants to the JavaScript
      try {
        jsScope.put("SKIP_PIPELINE", jsScope, ScriptValues.SKIP_PIPELINE);
        jsScope.put("ABORT_PIPELINE", jsScope, ScriptValues.ABORT_PIPELINE);
        jsScope.put("ERROR_PIPELINE", jsScope, ScriptValues.ERROR_PIPELINE);
        jsScope.put("CONTINUE_PIPELINE", jsScope, ScriptValues.CONTINUE_PIPELINE);
      } catch (Exception ex) {
        errorMessage = "Couldn't add Pipeline Constants! Error:" + Const.CR + ex;
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      try {
        ScriptValuesDummy dummyTransform =
            new ScriptValuesDummy(prev, pipelineMeta.getTransformFields(variables, transformMeta));
        Scriptable jsvalue = Context.toObject(dummyTransform, jsScope);
        jsScope.put("_transform_", jsScope, jsvalue);

        Object[] row = new Object[prev.size()];
        Scriptable jsRowMeta = Context.toObject(prev, jsScope);
        jsScope.put("rowMeta", jsScope, jsRowMeta);
        for (int i = 0; i < prev.size(); i++) {
          IValueMeta valueMeta = prev.getValueMeta(i);
          Object valueData = null;

          // Set date and string values to something to simulate real thing
          //
          if (valueMeta.isDate()) {
            valueData = new Date();
          }
          if (valueMeta.isString()) {
            valueData =
                "test value test value test value test value test value "
                    + "test value test value test value test value test value";
          }
          if (valueMeta.isInteger()) {
            valueData = 0L;
          }
          if (valueMeta.isNumber()) {
            valueData = 0.0;
          }
          if (valueMeta.isBigNumber()) {
            valueData = BigDecimal.ZERO;
          }
          if (valueMeta.isBoolean()) {
            valueData = Boolean.TRUE;
          }
          if (valueMeta.isBinary()) {
            valueData =
                new byte[] {
                  0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                };
          }

          row[i] = valueData;
          if (valueData != null) {
            Scriptable jsArgument = Context.toObject(valueData, jsScope);
            jsScope.put(valueMeta.getName(), jsScope, jsArgument);
          }
        }
        Scriptable jsRow = Context.toObject(row, jsScope);
        jsScope.put("row", jsScope, jsRow);
      } catch (Exception ev) {
        errorMessage = "Couldn't add Input fields to Script! Error:" + Const.CR + ev;
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      checkStartScript(
          transformMeta,
          jsContext,
          jsScope,
          strActiveStartScript,
          strActiveStartScriptName,
          remarks);
      checkTransformScript(transformMeta, jsContext, jsScope, strActiveStartScript, remarks);
      checkEndScript(
          transformMeta, jsContext, jsScope, strActiveEndScript, strActiveEndScriptName, remarks);
    } else {
      Context.exit();
      errorMessage =
          BaseMessages.getString(
              PKG, "ScriptValuesMetaMod.CheckResult.CouldNotGetFieldsFromPreviousTransform");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.ConnectedTransformOK2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  private void checkTransformScript(
      TransformMeta transformMeta,
      Context jsContext,
      ScriptableObject jsScope,
      String strActiveScript,
      List<ICheckResult> remarks) {
    try {
      Script jsscript = jsContext.compileString(strActiveScript, "script", 1, null);
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.ScriptCompiledOK"),
              transformMeta);
      remarks.add(cr);

      checkTransformScriptExecution(transformMeta, jsContext, jsScope, remarks, jsscript);
    } catch (Exception e) {
      Context.exit();
      String errorMessage =
          BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.CouldNotCompileScript")
              + Const.CR
              + e;
      CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  private void checkTransformScriptExecution(
      TransformMeta transformMeta,
      Context jsContext,
      ScriptableObject jsScope,
      List<ICheckResult> remarks,
      Script jsscript) {
    CheckResult cr;
    try {
      jsscript.exec(jsContext, jsScope, jsScope);
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.ScriptCompiledOK2"),
              transformMeta);
      remarks.add(cr);

      if (!scriptFields.isEmpty()) {
        String message =
            BaseMessages.getString(
                    PKG,
                    "ScriptValuesMetaMod.CheckResult.FailedToGetValues",
                    String.valueOf(scriptFields.size()))
                + Const.CR
                + Const.CR;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, message, transformMeta);
        remarks.add(cr);
      }
    } catch (JavaScriptException jse) {
      Context.exit();
      String errorMessage =
          BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.CouldNotExecuteScript")
              + Const.CR
              + jse;
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } catch (Exception e) {
      Context.exit();
      String errorMessage =
          BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.CouldNotExecuteScript2")
              + Const.CR
              + e;
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  private void checkStartScript(
      TransformMeta transformMeta,
      Context jsContext,
      ScriptableObject jsScope,
      String strActiveStartScript,
      String strActiveStartScriptName,
      List<ICheckResult> remarks) {
    try {
      // Checking for StartScript
      if (!Utils.isEmpty(strActiveStartScript)) {
        jsContext.evaluateString(jsScope, strActiveStartScript, "pipeline_Start", 1, null);
        String errorMessage = "Found Start Script. " + strActiveStartScriptName + " Processing OK";
        CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
        remarks.add(cr);
      }
    } catch (Exception e) {
      String errorMessage = "Couldn't process Start Script! Error:" + Const.CR + e;
      CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  /** Checking End Script */
  private void checkEndScript(
      TransformMeta transformMeta,
      Context jsContext,
      ScriptableObject jsScope,
      String strActiveEndScript,
      String strActiveEndScriptName,
      List<ICheckResult> remarks) {
    try {
      if (!Utils.isEmpty(strActiveEndScript)) {
        jsContext.evaluateString(jsScope, strActiveEndScript, "pipeline_End", 1, null);
        String errorMessage = "Found End Script. " + strActiveEndScriptName + " Processing OK";
        CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
        remarks.add(cr);
      }
    } catch (Exception e) {
      String errorMessage = "Couldn't process End Script! Error:" + Const.CR + e;
      CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  public String getFunctionFromScript(String strFunction, String strScript) {
    StringBuilder sRC = new StringBuilder();
    int iStartPos = strScript.indexOf(strFunction);
    if (iStartPos > 0) {
      iStartPos = strScript.indexOf('{', iStartPos);
      int iCounter = 1;
      while (iCounter != 0) {
        if (strScript.charAt(iStartPos++) == '{') {
          iCounter++;
        } else if (strScript.charAt(iStartPos++) == '}') {
          iCounter--;
        }
        sRC.append(strScript.charAt(iStartPos));
      }
    }
    return sRC.toString();
  }

  public String[] getJSScriptNames() {
    String[] strJSNames = new String[jsScripts.size()];
    for (int i = 0; i < jsScripts.size(); i++) {
      strJSNames[i] = jsScripts.get(i).getName();
    }
    return strJSNames;
  }
}
