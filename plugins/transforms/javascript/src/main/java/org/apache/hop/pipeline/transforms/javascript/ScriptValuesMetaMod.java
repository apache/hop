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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.DataTypeConverter;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.injection.NullNumberConverter;
import org.apache.hop.core.plugins.HopURLClassLoader;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.JavaScriptException;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.math.BigDecimal;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/*
 * Created on 2-jun-2003
 *
 */
@Transform(
    id = "ScriptValueMod",
    image = "javascript.svg",
    name = "i18n::ScriptValuesMod.Name",
    description = "i18n::ScriptValuesMod.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/javascript.html")
@InjectionSupported(
    localizationPrefix = "ScriptValuesMod.Injection.",
    groups = {"FIELDS", "SCRIPTS"})
public class ScriptValuesMetaMod extends BaseTransformMeta
    implements ITransformMeta<ScriptValuesMod, ScriptValuesModData> {
  private static final Class<?> PKG = ScriptValuesMetaMod.class; // For Translator

  private static final String JSSCRIPT_TAG_TYPE = "jsScript_type";
  private static final String JSSCRIPT_TAG_NAME = "jsScript_name";
  private static final String JSSCRIPT_TAG_SCRIPT = "jsScript_script";

  public static final String OPTIMIZATION_LEVEL_DEFAULT = "9";

  private ScriptValuesAddClasses[] additionalClasses;

  @InjectionDeep private ScriptValuesScript[] jsScripts;

  @Injection(name = "FIELD_NAME", group = "FIELDS")
  private String[] fieldname;

  @Injection(name = "FIELD_RENAME_TO", group = "FIELDS")
  private String[] rename;

  @Injection(
      name = "FIELD_TYPE",
      group = "FIELDS",
      convertEmpty = true,
      converter = DataTypeConverter.class)
  private int[] type;

  @Injection(
      name = "FIELD_LENGTH",
      group = "FIELDS",
      convertEmpty = true,
      converter = NullNumberConverter.class)
  private int[] length;

  @Injection(
      name = "FIELD_PRECISION",
      group = "FIELDS",
      convertEmpty = true,
      converter = NullNumberConverter.class)
  private int[] precision;

  @Injection(name = "FIELD_REPLACE", group = "FIELDS")
  private boolean[] replace; // Replace the specified field.

  @Injection(name = "OPTIMIZATION_LEVEL")
  private String optimizationLevel;

  public ScriptValuesMetaMod() {
    super(); // allocate BaseTransformMeta
    try {
      parseXmlForAdditionalClasses();
    } catch (Exception e) {
      /* Ignore */
    }
  }

  /** @return Returns the length. */
  public int[] getLength() {
    return length;
  }

  /** @param length The length to set. */
  public void setLength(int[] length) {
    this.length = length;
  }

  /** @return Returns the name. */
  public String[] getFieldname() {
    return fieldname;
  }

  /** @param fieldname The name to set. */
  public void setFieldname(String[] fieldname) {
    this.fieldname = fieldname;
  }

  /** @return Returns the precision. */
  public int[] getPrecision() {
    return precision;
  }

  /** @param precision The precision to set. */
  public void setPrecision(int[] precision) {
    this.precision = precision;
  }

  /** @return Returns the rename. */
  public String[] getRename() {
    return rename;
  }

  /** @param rename The rename to set. */
  public void setRename(String[] rename) {
    this.rename = rename;
  }

  /** @return Returns the type. */
  public int[] getType() {
    return this.type;
  }

  @AfterInjection
  public void afterInjection() {
    // extend all fields related arrays to match the length of the fieldname array, as they may all
    // be different
    // sizes, after meta injection
    extend(fieldname.length);
  }

  /** @param type The type to set. */
  public void setType(int[] type) {
    this.type = type;
  }

  public int getNumberOfJSScripts() {
    return jsScripts.length;
  }

  public String[] getJSScriptNames() {
    String[] strJSNames = new String[jsScripts.length];
    for (int i = 0; i < jsScripts.length; i++) {
      strJSNames[i] = jsScripts[i].getScriptName();
    }
    return strJSNames;
  }

  public ScriptValuesScript[] getJSScripts() {
    return jsScripts;
  }

  public void setJSScripts(ScriptValuesScript[] jsScripts) {
    this.jsScripts = jsScripts;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    fieldname = new String[nrFields];
    rename = new String[nrFields];
    type = new int[nrFields];
    for (int i = 0; i < nrFields; i++) {
      type[i] = -1;
    }
    length = new int[nrFields];
    for (int i = 0; i < nrFields; i++) {
      length[i] = -1;
    }
    precision = new int[nrFields];
    for (int i = 0; i < nrFields; i++) {
      precision[i] = -1;
    }
    replace = new boolean[nrFields];
  }

  /**
   * Extends all field related arrays so that they are the same size.
   *
   * @param nrFields
   */
  @VisibleForTesting
  void extend(int nrFields) {
    fieldname = extend(fieldname, nrFields);
    rename = extend(rename, nrFields);
    type = extend(type, nrFields);
    length = extend(length, nrFields);
    precision = extend(precision, nrFields);
    replace = extend(replace, nrFields);
  }

  private String[] extend(final String[] array, final int nrFields) {
    if (array == null) {
      return new String[nrFields];
    } else if (array.length < nrFields) {
      return Arrays.copyOf(array, nrFields);
    } else {
      return array;
    }
  }

  private int[] extend(final int[] array, final int nrFields) {
    if (array == null || array.length < nrFields) {
      int originalLength = array == null ? 0 : array.length;
      final int[] newArray = array == null ? new int[nrFields] : Arrays.copyOf(array, nrFields);
      for (int i = originalLength; i < nrFields; i++) {
        newArray[i] = -1;
      }
      return newArray;
    } else {
      return array;
    }
  }

  private boolean[] extend(final boolean[] array, final int nrFields) {
    if (array == null) {
      return new boolean[nrFields];
    } else if (array.length < nrFields) {
      return Arrays.copyOf(array, nrFields);
    } else {
      return array;
    }
  }

  public Object clone() {
    ScriptValuesMetaMod retval = (ScriptValuesMetaMod) super.clone();

    int nrFields = fieldname.length;

    retval.allocate(nrFields);
    System.arraycopy(fieldname, 0, retval.fieldname, 0, nrFields);
    System.arraycopy(rename, 0, retval.rename, 0, nrFields);
    System.arraycopy(type, 0, retval.type, 0, nrFields);
    System.arraycopy(length, 0, retval.length, 0, nrFields);
    System.arraycopy(precision, 0, retval.precision, 0, nrFields);
    System.arraycopy(replace, 0, retval.replace, 0, nrFields);

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ScriptValuesModData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ScriptValuesMod(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      String script = XmlHandler.getTagValue(transformNode, "script");
      optimizationLevel = XmlHandler.getTagValue(transformNode, "optimizationLevel");

      // When in compatibility mode, we load the script, not the other tabs...
      //
      if (!Utils.isEmpty(script)) {
        jsScripts = new ScriptValuesScript[1];
        jsScripts[0] =
            new ScriptValuesScript(ScriptValuesScript.TRANSFORM_SCRIPT, "ScriptValue", script);
      } else {
        Node scripts = XmlHandler.getSubNode(transformNode, "jsScripts");
        int nrscripts = XmlHandler.countNodes(scripts, "jsScript");
        jsScripts = new ScriptValuesScript[nrscripts];
        for (int i = 0; i < nrscripts; i++) {
          Node fnode = XmlHandler.getSubNodeByNr(scripts, "jsScript", i);

          jsScripts[i] =
              new ScriptValuesScript(
                  Integer.parseInt(XmlHandler.getTagValue(fnode, JSSCRIPT_TAG_TYPE)),
                  XmlHandler.getTagValue(fnode, JSSCRIPT_TAG_NAME),
                  XmlHandler.getTagValue(fnode, JSSCRIPT_TAG_SCRIPT));
        }
      }

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldname[i] = XmlHandler.getTagValue(fnode, "name");
        rename[i] = XmlHandler.getTagValue(fnode, "rename");
        type[i] = ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fnode, "type"));

        String slen = XmlHandler.getTagValue(fnode, "length");
        String sprc = XmlHandler.getTagValue(fnode, "precision");
        length[i] = Const.toInt(slen, -1);
        precision[i] = Const.toInt(sprc, -1);
        replace[i] = "Y".equalsIgnoreCase(XmlHandler.getTagValue(fnode, "replace"));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "ScriptValuesMetaMod.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    jsScripts = new ScriptValuesScript[1];
    jsScripts[0] =
        new ScriptValuesScript(
            ScriptValuesScript.TRANSFORM_SCRIPT,
            BaseMessages.getString(PKG, "ScriptValuesMod.Script1"),
            "//" + BaseMessages.getString(PKG, "ScriptValuesMod.ScriptHere") + Const.CR + Const.CR);

    int nrFields = 0;
    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      fieldname[i] = "newvalue";
      rename[i] = "newvalue";
      type[i] = IValueMeta.TYPE_NUMBER;
      length[i] = -1;
      precision[i] = -1;
      replace[i] = false;
    }

    //    compatible = false;
    optimizationLevel = OPTIMIZATION_LEVEL_DEFAULT;
  }

  public void getFields(
      IRowMeta row,
      String originTransformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      for (int i = 0; i < fieldname.length; i++) {
        if (!Utils.isEmpty(fieldname[i])) {
          int valueIndex = -1;
          IValueMeta v;
          if (replace[i]) {
            valueIndex = row.indexOfValue(fieldname[i]);
            if (valueIndex < 0) {
              // The field was not found using the "name" field
              if (Utils.isEmpty(rename[i])) {
                // There is no "rename" field to try; Therefore we cannot find the
                // field to replace
                throw new HopTransformException(
                    BaseMessages.getString(
                        PKG, "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound", fieldname[i]));
              } else {
                // Lookup the field to replace using the "rename" field
                valueIndex = row.indexOfValue(rename[i]);
                if (valueIndex < 0) {
                  // The field was not found using the "rename" field"; Therefore
                  // we cannot find the field to replace
                  //
                  throw new HopTransformException(
                      BaseMessages.getString(
                          PKG, "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound", rename[i]));
                }
              }
            }

            // Change the data type to match what's specified...
            //
            IValueMeta source = row.getValueMeta(valueIndex);
            v = ValueMetaFactory.cloneValueMeta(source, type[i]);
            row.setValueMeta(valueIndex, v);
          } else {
            if (!Utils.isEmpty(rename[i])) {
              v = ValueMetaFactory.createValueMeta(rename[i], type[i]);
            } else {
              v = ValueMetaFactory.createValueMeta(fieldname[i], type[i]);
            }
          }
          v.setLength(length[i]);
          v.setPrecision(precision[i]);
          v.setOrigin(originTransformName);
          if (!replace[i]) {
            row.addValueMeta(v);
          }
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(e);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XmlHandler.addTagValue("optimizationLevel", optimizationLevel));

    retval.append("    <jsScripts>");
    for (int i = 0; i < jsScripts.length; i++) {
      retval.append("      <jsScript>");
      retval
          .append("        ")
          .append(XmlHandler.addTagValue(JSSCRIPT_TAG_TYPE, jsScripts[i].getScriptType()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue(JSSCRIPT_TAG_NAME, jsScripts[i].getScriptName()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue(JSSCRIPT_TAG_SCRIPT, jsScripts[i].getScript()));
      retval.append("      </jsScript>");
    }
    retval.append("    </jsScripts>");

    retval.append("    <fields>");
    for (int i = 0; i < fieldname.length; i++) {
      retval.append("      <field>");
      retval.append("        ").append(XmlHandler.addTagValue("name", fieldname[i]));
      retval.append("        ").append(XmlHandler.addTagValue("rename", rename[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(type[i])));
      retval.append("        ").append(XmlHandler.addTagValue("length", length[i]));
      retval.append("        ").append(XmlHandler.addTagValue("precision", precision[i]));
      retval.append("        ").append(XmlHandler.addTagValue("replace", replace[i]));
      retval.append("      </field>");
    }
    retval.append("    </fields>");

    return retval.toString();
  }

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
    boolean errorFound = false;
    String errorMessage = "";
    CheckResult cr;

    Context jscx;
    Scriptable jsscope;
    Script jsscript;

    jscx = ContextFactory.getGlobal().enterContext();
    jsscope = jscx.initStandardObjects(null, false);
    try {
      jscx.setOptimizationLevel(Integer.valueOf(variables.resolve(optimizationLevel)));
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
    if (jsScripts.length > 0) {
      for (int i = 0; i < jsScripts.length; i++) {
        if (jsScripts[i].isTransformScript()) {
          // strActiveScriptName =jsScripts[i].getScriptName();
          strActiveScript = jsScripts[i].getScript();
        } else if (jsScripts[i].isStartScript()) {
          strActiveStartScriptName = jsScripts[i].getScriptName();
          strActiveStartScript = jsScripts[i].getScript();
        } else if (jsScripts[i].isEndScript()) {
          strActiveEndScriptName = jsScripts[i].getScriptName();
          strActiveEndScript = jsScripts[i].getScript();
        }
      }
    }

    if (prev != null && strActiveScript.length() > 0) {
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
      for (int i = 0; i < getNumberOfJSScripts(); i++) {
        Scriptable jsR = Context.toObject(jsScripts[i].getScript(), jsscope);
        jsscope.put(jsScripts[i].getScriptName(), jsscope, jsR);
      }

      // Modification for Additional Script parsing
      try {
        if (getAddClasses() != null) {
          for (int i = 0; i < getAddClasses().length; i++) {
            Object jsOut = Context.javaToJS(getAddClasses()[i].getAddObject(), jsscope);
            ScriptableObject.putProperty(jsscope, getAddClasses()[i].getJSName(), jsOut);
          }
        }
      } catch (Exception e) {
        errorMessage = ("Couldn't add JavaClasses to Context! Error:");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      // Adding some default JavaScriptFunctions to the System
      try {
        Context.javaToJS(ScriptValuesAddedFunctions.class, jsscope);
        ((ScriptableObject) jsscope)
            .defineFunctionProperties(
                ScriptValuesAddedFunctions.jsFunctionList,
                ScriptValuesAddedFunctions.class,
                ScriptableObject.DONTENUM);
      } catch (Exception ex) {
        errorMessage = "Couldn't add Default Functions! Error:" + Const.CR + ex.toString();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      // Adding some Constants to the JavaScript
      try {
        jsscope.put("SKIP_PIPELINE", jsscope, Integer.valueOf(ScriptValuesMod.SKIP_PIPELINE));
        jsscope.put("ABORT_PIPELINE", jsscope, Integer.valueOf(ScriptValuesMod.ABORT_PIPELINE));
        jsscope.put("ERROR_PIPELINE", jsscope, Integer.valueOf(ScriptValuesMod.ERROR_PIPELINE));
        jsscope.put(
            "CONTINUE_PIPELINE", jsscope, Integer.valueOf(ScriptValuesMod.CONTINUE_PIPELINE));
      } catch (Exception ex) {
        errorMessage = "Couldn't add Pipeline Constants! Error:" + Const.CR + ex.toString();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      try {
        ScriptValuesModDummy dummyTransform =
            new ScriptValuesModDummy(
                prev, pipelineMeta.getTransformFields(variables, transformMeta));
        Scriptable jsvalue = Context.toObject(dummyTransform, jsscope);
        jsscope.put("_transform_", jsscope, jsvalue);

        Object[] row = new Object[prev.size()];
        Scriptable jsRowMeta = Context.toObject(prev, jsscope);
        jsscope.put("rowMeta", jsscope, jsRowMeta);
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
            valueData = Long.valueOf(0L);
          }
          if (valueMeta.isNumber()) {
            valueData = new Double(0.0);
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

          Scriptable jsarg = Context.toObject(valueData, jsscope);
          jsscope.put(valueMeta.getName(), jsscope, jsarg);
        }
        Scriptable jsRow = Context.toObject(row, jsscope);
        jsscope.put("row", jsscope, jsRow);
      } catch (Exception ev) {
        errorMessage = "Couldn't add Input fields to Script! Error:" + Const.CR + ev.toString();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      try {
        // Checking for StartScript
        if (strActiveStartScript != null && strActiveStartScript.length() > 0) {
          /* Object startScript = */ jscx.evaluateString(
              jsscope, strActiveStartScript, "pipeline_Start", 1, null);
          errorMessage = "Found Start Script. " + strActiveStartScriptName + " Processing OK";
          cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (Exception e) {
        errorMessage = "Couldn't process Start Script! Error:" + Const.CR + e.toString();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }

      try {
        jsscript = jscx.compileString(strActiveScript, "script", 1, null);

        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.ScriptCompiledOK"),
                transformMeta);
        remarks.add(cr);

        try {

          jsscript.exec(jscx, jsscope);

          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.ScriptCompiledOK2"),
                  transformMeta);
          remarks.add(cr);

          if (fieldname.length > 0) {
            StringBuilder message =
                new StringBuilder(
                    BaseMessages.getString(
                            PKG,
                            "ScriptValuesMetaMod.CheckResult.FailedToGetValues",
                            String.valueOf(fieldname.length))
                        + Const.CR
                        + Const.CR);

            if (errorFound) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR, message.toString(), transformMeta);
            } else {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, message.toString(), transformMeta);
            }
            remarks.add(cr);
          }
        } catch (JavaScriptException jse) {
          Context.exit();
          errorMessage =
              BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.CouldNotExecuteScript")
                  + Const.CR
                  + jse.toString();
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        } catch (Exception e) {
          Context.exit();
          errorMessage =
              BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.CouldNotExecuteScript2")
                  + Const.CR
                  + e.toString();
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }

        // Checking End Script
        try {
          if (strActiveEndScript != null && strActiveEndScript.length() > 0) {
            /* Object endScript = */ jscx.evaluateString(
                jsscope, strActiveEndScript, "pipeline_End", 1, null);
            errorMessage = "Found End Script. " + strActiveEndScriptName + " Processing OK";
            cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
            remarks.add(cr);
          }
        } catch (Exception e) {
          errorMessage = "Couldn't process End Script! Error:" + Const.CR + e.toString();
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (Exception e) {
        Context.exit();
        errorMessage =
            BaseMessages.getString(PKG, "ScriptValuesMetaMod.CheckResult.CouldNotCompileScript")
                + Const.CR
                + e.toString();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }
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

  public String getFunctionFromScript(String strFunction, String strScript) {
    String sRC = "";
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
        sRC = sRC + strScript.charAt(iStartPos);
      }
    }
    return sRC;
  }

  public ScriptValuesModData getTransformData() {
    return new ScriptValuesModData();
  }

  // This is for Additional Classloading
  public void parseXmlForAdditionalClasses() throws HopException {
    try {
      Properties sysprops = System.getProperties();
      String strActPath = sysprops.getProperty("user.dir");
      Document dom =
          XmlHandler.loadXmlFile(strActPath + "/plugins/transforms/ScriptValues_mod/plugin.xml");
      Node transformNode = dom.getDocumentElement();
      Node libraries = XmlHandler.getSubNode(transformNode, "js_libraries");
      int nbOfLibs = XmlHandler.countNodes(libraries, "js_lib");
      additionalClasses = new ScriptValuesAddClasses[nbOfLibs];
      for (int i = 0; i < nbOfLibs; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(libraries, "js_lib", i);
        String strJarName = XmlHandler.getTagAttribute(fnode, "name");
        String strClassName = XmlHandler.getTagAttribute(fnode, "classname");
        String strJSName = XmlHandler.getTagAttribute(fnode, "js_name");

        Class<?> addClass =
            LoadAdditionalClass(
                strActPath + "/plugins/transforms/ScriptValues_mod/" + strJarName, strClassName);
        Object addObject = addClass.newInstance();
        additionalClasses[i] = new ScriptValuesAddClasses(addClass, addObject, strJSName);
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "ScriptValuesMetaMod.Exception.UnableToParseXMLforAdditionalClasses"),
          e);
    }
  }

  private static Class<?> LoadAdditionalClass(String strJar, String strClassName)
      throws HopException {
    try {
      Thread t = Thread.currentThread();
      ClassLoader cl = t.getContextClassLoader();
      URL u = new URL("jar:file:" + strJar + "!/");
      // We never know what else the script wants to load with the class loader, so lets not close
      // it just like that.
      @SuppressWarnings("resource")
      HopURLClassLoader kl = new HopURLClassLoader(new URL[] {u}, cl);
      Class<?> toRun = kl.loadClass(strClassName);
      return toRun;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ScriptValuesMetaMod.Exception.UnableToLoadAdditionalClass"),
          e);
    }
  }

  public ScriptValuesAddClasses[] getAddClasses() {
    return additionalClasses;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the replace */
  public boolean[] getReplace() {
    return replace;
  }

  /** @param replace the replace to set */
  public void setReplace(boolean[] replace) {
    this.replace = replace;
  }

  public void setOptimizationLevel(String optimizationLevel) {
    this.optimizationLevel = optimizationLevel;
  }

  public String getOptimizationLevel() {
    return this.optimizationLevel;
  }
}
