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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SuperScript",
    image = "script.svg",
    name = "Script",
    description = "Executes scripts for JSR-223 Script Engines",
    categoryDescription = "Scripting",
    keywords = "script,scripting,groovy,python,javascript,ecmascript,ruby",
    documentationUrl = "/pipeline/transforms/script.html")
public class ScriptMeta extends BaseTransformMeta<Script, ScriptData> implements ITransformMeta {
  private static final Class<?> PKG = ScriptMeta.class;

  @HopMetadataProperty(key = "scriptLanguage")
  private String languageName;

  @HopMetadataProperty(groupKey = "scripts", key = "script")
  private List<SScript> scripts;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<SField> fields;

  public ScriptMeta() {
    scripts = new ArrayList<>();
    fields = new ArrayList<>();
  }

  public ScriptMeta(ScriptMeta m) {
    this();
    this.languageName = m.languageName;
    m.scripts.forEach(s -> this.scripts.add(new SScript(s)));
    m.fields.forEach(f -> this.fields.add(new SField(f)));
  }

  @Override
  public ScriptMeta clone() {
    return new ScriptMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (SField field : fields) {
      // Skip field without a name
      //
      if (StringUtils.isEmpty(field.getName())) {
        continue;
      }

      int fieldType = field.getHopType();
      String fieldName;
      int replaceIndex;

      if (field.isReplace()) {
        // Look up the field to replace...
        //
        if (rowMeta.searchValueMeta(field.getName()) == null
            && StringUtils.isEmpty(field.getRename())) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "ScriptMeta.Exception.FieldToReplaceNotFound", field.getName()));
        }
        replaceIndex = rowMeta.indexOfValue(field.getRename());

        // Change the data type to match what's specified...
        //
        fieldName = field.getRename();
      } else {
        replaceIndex = -1;
        if (StringUtils.isNotEmpty(field.getRename())) {
          fieldName = field.getName();
        } else {
          fieldName = field.getRename();
        }
      }

      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(fieldName, fieldType);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setOrigin(name);
        if (field.isReplace() && replaceIndex >= 0) {
          rowMeta.setValueMeta(replaceIndex, v);
        } else {
          rowMeta.addValueMeta(v);
        }
      } catch (HopPluginException e) {
        throw new HopTransformException(
            "Error handling field " + field.getName() + " with Hop data type: " + field.getType(),
            e);
      }
    }
  }

  /**
   * Gets languageName
   *
   * @return value of languageName
   */
  public String getLanguageName() {
    return languageName;
  }

  /**
   * Sets languageName
   *
   * @param languageName value of languageName
   */
  public void setLanguageName(String languageName) {
    this.languageName = languageName;
  }

  /**
   * Gets scripts
   *
   * @return value of scripts
   */
  public List<SScript> getScripts() {
    return scripts;
  }

  /**
   * Sets scripts
   *
   * @param scripts value of scripts
   */
  public void setScripts(List<SScript> scripts) {
    this.scripts = scripts;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<SField> getFields() {
    return fields;
  }

  /**
   * Sets fields
   *
   * @param fields value of fields
   */
  public void setFields(List<SField> fields) {
    this.fields = fields;
  }

  public static final class SField {
    @HopMetadataProperty private String name;
    @HopMetadataProperty private String rename;
    @HopMetadataProperty private String type;
    @HopMetadataProperty private int length;
    @HopMetadataProperty private int precision;

    /** Replace the specified field. */
    @HopMetadataProperty private boolean replace;

    /** Does this field contain the result of the compiledScript evaluation? */
    @HopMetadataProperty private boolean scriptResult;

    public SField() {}

    public SField(SField f) {
      this();
      this.name = f.name;
      this.rename = f.rename;
      this.type = f.type;
      this.length = f.length;
      this.precision = f.precision;
      this.replace = f.replace;
      this.scriptResult = f.scriptResult;
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }

    public IValueMeta createHopValue() throws HopPluginException {
      IValueMeta valueMeta =
          ValueMetaFactory.createValueMeta(Const.NVL(name, rename), getHopType());
      valueMeta.setLength(length);
      valueMeta.setPrecision(precision);
      return valueMeta;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets rename
     *
     * @return value of rename
     */
    public String getRename() {
      return rename;
    }

    /**
     * Sets rename
     *
     * @param rename value of rename
     */
    public void setRename(String rename) {
      this.rename = rename;
    }

    /**
     * Gets type
     *
     * @return value of type
     */
    public String getType() {
      return type;
    }

    /**
     * Sets type
     *
     * @param type value of type
     */
    public void setType(String type) {
      this.type = type;
    }

    /**
     * Gets length
     *
     * @return value of length
     */
    public int getLength() {
      return length;
    }

    /**
     * Sets length
     *
     * @param length value of length
     */
    public void setLength(int length) {
      this.length = length;
    }

    /**
     * Gets precision
     *
     * @return value of precision
     */
    public int getPrecision() {
      return precision;
    }

    /**
     * Sets precision
     *
     * @param precision value of precision
     */
    public void setPrecision(int precision) {
      this.precision = precision;
    }

    /**
     * Gets replace
     *
     * @return value of replace
     */
    public boolean isReplace() {
      return replace;
    }

    /**
     * Sets replace
     *
     * @param replace value of replace
     */
    public void setReplace(boolean replace) {
      this.replace = replace;
    }

    /**
     * Gets scriptResult
     *
     * @return value of scriptResult
     */
    public boolean isScriptResult() {
      return scriptResult;
    }

    /**
     * Sets scriptResult
     *
     * @param scriptResult value of scriptResult
     */
    public void setScriptResult(boolean scriptResult) {
      this.scriptResult = scriptResult;
    }
  }

  public enum ScriptType implements IEnumHasCodeAndDescription {
    NORMAL_SCRIPT("-1", "Normal script"),
    TRANSFORM_SCRIPT("0", "Transform script"),
    START_SCRIPT("1", "Start script"),
    END_SCRIPT("2", "End script");

    private String code;
    private String description;

    ScriptType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Sets code
     *
     * @param code value of code
     */
    public void setCode(String code) {
      this.code = code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }

    /**
     * Sets description
     *
     * @param description value of description
     */
    public void setDescription(String description) {
      this.description = description;
    }
  }

  public static final class SScript {
    @HopMetadataProperty(key = "scriptType", storeWithCode = true)
    private ScriptType scriptType;

    @HopMetadataProperty(key = "scriptName")
    private String scriptName;

    @HopMetadataProperty(key = "scriptBody")
    private String script;

    public SScript() {
      scriptType = ScriptType.TRANSFORM_SCRIPT;
      scriptName = "script";
    }

    public SScript(ScriptType scriptType, String scriptName, String script) {
      this.scriptType = scriptType;
      this.scriptName = scriptName;
      this.script = script;
    }

    public SScript(SScript s) {
      this();
      this.scriptType = s.scriptType;
      this.scriptName = s.scriptName;
      this.script = s.script;
    }

    public boolean isTransformScript() {
      return this.scriptType == ScriptType.TRANSFORM_SCRIPT;
    }

    public boolean isStartScript() {
      return this.scriptType == ScriptType.START_SCRIPT;
    }

    public boolean isEndScript() {
      return this.scriptType == ScriptType.END_SCRIPT;
    }

    /**
     * Gets scriptType
     *
     * @return value of scriptType
     */
    public ScriptType getScriptType() {
      return scriptType;
    }

    /**
     * Sets scriptType
     *
     * @param scriptType value of scriptType
     */
    public void setScriptType(ScriptType scriptType) {
      this.scriptType = scriptType;
    }

    /**
     * Gets scriptName
     *
     * @return value of scriptName
     */
    public String getScriptName() {
      return scriptName;
    }

    /**
     * Sets scriptName
     *
     * @param scriptName value of scriptName
     */
    public void setScriptName(String scriptName) {
      this.scriptName = scriptName;
    }

    /**
     * Gets script
     *
     * @return value of script
     */
    public String getScript() {
      return script;
    }

    /**
     * Sets script
     *
     * @param script value of script
     */
    public void setScript(String script) {
      this.script = script;
    }
  }
}
