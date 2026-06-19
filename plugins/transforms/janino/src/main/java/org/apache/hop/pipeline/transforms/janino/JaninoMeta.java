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
package org.apache.hop.pipeline.transforms.janino;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "Janino",
    image = "janino.svg",
    name = "i18n::Janino.Name",
    description = "i18n::Janino.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::JaninoMeta.keyword",
    documentationUrl = "/pipeline/transforms/userdefinedjavaexpression.html")
@Getter
@Setter
public class JaninoMeta extends BaseTransformMeta<Janino, JaninoData> {
  private static final Class<?> PKG = JaninoMeta.class;

  /** Default matches Janino bytecode default ({@code UnitCompiler#getDefaultTargetVersion()}). */
  public static final int JAVA_TARGET_VERSION_DEFAULT = 6;

  public static final int JAVA_TARGET_VERSION_MIN = 6;
  public static final int JAVA_TARGET_VERSION_MAX = 21;

  /** The formula calculations to be performed */
  @HopMetadataProperty(
      key = "formula",
      injectionGroupKey = "FORMULA",
      injectionGroupDescription = "Janino.Injection.FORMULA")
  private List<JaninoMetaFunction> functions;

  /**
   * Java language / class file level passed to Janino ({@link
   * org.codehaus.janino.ExpressionEvaluator #setSourceVersion} and {@link
   * org.codehaus.janino.ExpressionEvaluator#setTargetVersion}). When unset or invalid, {@link
   * #JAVA_TARGET_VERSION_DEFAULT} is used.
   */
  @HopMetadataProperty(key = "java_target_version")
  private int javaTargetVersion = JAVA_TARGET_VERSION_DEFAULT;

  public JaninoMeta() {
    super();
    this.functions = new ArrayList<>();
  }

  public JaninoMeta(JaninoMeta m) {
    this();
    m.functions.forEach(f -> this.functions.add(new JaninoMetaFunction(f)));
    this.javaTargetVersion = m.javaTargetVersion;
  }

  /**
   * Resolved Janino compiler source/target version (major Java version number), for backwards
   * compatibility when pipelines omit {@link #javaTargetVersion} or contain invalid values.
   */
  public int getEffectiveJavaTargetVersion() {
    if (javaTargetVersion < JAVA_TARGET_VERSION_MIN
        || javaTargetVersion > JAVA_TARGET_VERSION_MAX) {
      return JAVA_TARGET_VERSION_DEFAULT;
    }
    return javaTargetVersion;
  }

  @Override
  public Object clone() {
    return new JaninoMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (JaninoMetaFunction fn : functions) {
      if (Utils.isEmpty(fn.getReplaceField())) {
        // Not replacing a field.
        if (!Utils.isEmpty(fn.getFieldName())) {
          // It's a new field!

          try {
            IValueMeta v = ValueMetaFactory.createValueMeta(fn.getFieldName(), fn.getValueType());
            v.setLength(fn.getValueLength(), fn.getValuePrecision());
            v.setOrigin(name);
            row.addValueMeta(v);
          } catch (Exception e) {
            throw new HopTransformException(e);
          }
        }
      } else {
        // Replacing a field
        int index = row.indexOfValue(fn.getReplaceField());
        if (index < 0) {
          throw new HopTransformException(
              "Unknown field specified to replace with a formula result: ["
                  + fn.getReplaceField()
                  + "]");
        }
        // Change the data type etc.
        //
        IValueMeta v = row.getValueMeta(index).clone();
        v.setLength(fn.getValueLength(), fn.getValuePrecision());
        v.setOrigin(name);
        row.setValueMeta(index, v); // replace it
      }
    }
  }

  /**
   * Checks the settings of this transform and puts the findings in a remarks List.
   *
   * @param remarks The list to put the remarks in @see org.apache.hop.core.CheckResult
   * @param transformMeta The transformMeta to help checking
   * @param prev The fields coming from the previous transform
   * @param input The input transform names
   * @param output The output transform names
   * @param info The fields that are used as information by the transform
   */
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
    CheckResult cr;
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "JaninoMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JaninoMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "JaninoMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JaninoMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
