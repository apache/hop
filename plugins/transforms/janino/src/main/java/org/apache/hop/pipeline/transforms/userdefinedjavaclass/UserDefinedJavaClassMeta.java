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

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.janino.JaninoMeta;
import org.apache.hop.pipeline.transforms.util.JaninoCheckerUtil;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

@Transform(
    id = "UserDefinedJavaClass",
    image = "userdefinedjavaclass.svg",
    name = "i18n::UserDefinedJavaClass.Name",
    description = "i18n::UserDefinedJavaClass.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::UserDefinedJavaClassMeta.keyword",
    documentationUrl = "/pipeline/transforms/userdefinedjavaclass.html")
@Getter
@Setter
public class UserDefinedJavaClassMeta
    extends BaseTransformMeta<UserDefinedJavaClass, UserDefinedJavaClassData> {
  private static final Class<?> PKG = UserDefinedJavaClassMeta.class;

  static {
    IVariables vs = new Variables();
    // sets up the default variables
    vs.initializeFrom(null);
    String maxSizeStr = vs.getVariable(UserDefinedJavaClass.HOP_DEFAULT_CLASS_CACHE_SIZE, "100");
    int maxCacheSize;
    try {
      maxCacheSize = Integer.parseInt(maxSizeStr);
    } catch (Exception ignored) {
      // default to 100 if property not set
      maxCacheSize = 100;
    }
    // Initialize Class ICache
    CLASS_CACHE = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build();
  }

  @Getter
  @Setter
  public static class FieldInfo implements Cloneable {
    @HopMetadataProperty(
        key = "field_name",
        injectionKey = "FIELD_NAME",
        injectionKeyDescription = "UserDefinedJavaClass.Injection.FIELD_NAME")
    private String name;

    @HopMetadataProperty(
        key = "field_type",
        intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
        injectionKey = "FIELD_TYPE",
        injectionKeyDescription = "UserDefinedJavaClass.Injection.FIELD_TYPE")
    private int type;

    @HopMetadataProperty(
        key = "field_length",
        injectionKey = "FIELD_LENGTH",
        injectionKeyDescription = "UserDefinedJavaClass.Injection.FIELD_LENGTH")
    private int length;

    @HopMetadataProperty(
        key = "field_precision",
        injectionKey = "FIELD_PRECISION",
        injectionKeyDescription = "UserDefinedJavaClass.Injection.FIELD_PRECISION")
    private int precision;

    public FieldInfo() {}

    public FieldInfo(FieldInfo f) {
      super();
      this.name = f.name;
      this.type = f.type;
      this.length = f.length;
      this.precision = f.precision;
    }

    public FieldInfo(String name, int type, int length, int precision) {
      this();
      this.name = name;
      this.type = type;
      this.length = length;
      this.precision = precision;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      return super.clone();
    }
  }

  // Runtime fields. Consider moving these out.
  //
  private Class<TransformClassBase> cookedTransformClass;
  private List<Exception> cookErrors;
  private static final Cache<String, Class<?>> CLASS_CACHE;
  private boolean hasChanged;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "FIELD_INFO",
      injectionGroupDescription = "UserDefinedJavaClass.Injection.FIELD_INFO")
  private List<FieldInfo> fields = new ArrayList<>();

  @HopMetadataProperty(
      key = "definition",
      groupKey = "definitions",
      injectionGroupKey = "JAVA_CLASSES",
      injectionGroupDescription = "UserDefinedJavaClass.Injection.JAVA_CLASSES")
  private List<UserDefinedJavaClassDef> definitions = new ArrayList<>();

  @HopMetadataProperty(
      key = "clear_result_fields",
      injectionKey = "CLEAR_RESULT_FIELDS",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.CLEAR_RESULT_FIELDS")
  private boolean clearingResultFields;

  @HopMetadataProperty(
      key = "info_transform",
      groupKey = "info_transforms",
      injectionGroupKey = "INFO_TRANSFORMS",
      injectionGroupDescription = "UserDefinedJavaClass.Injection.INFO_TRANSFORMS")
  private List<InfoTransformDefinition> infoTransformDefinitions;

  @HopMetadataProperty(
      key = "target_transform",
      groupKey = "target_transforms",
      injectionGroupKey = "TARGET_TRANSFORMS",
      injectionGroupDescription = "UserDefinedJavaClass.Injection.TARGET_TRANSFORMS")
  private List<TargetTransformDefinition> targetTransformDefinitions;

  @HopMetadataProperty(
      key = "usage_parameter",
      groupKey = "usage_parameters",
      injectionGroupKey = "PARAMETERS",
      injectionGroupDescription = "UserDefinedJavaClass.Injection.PARAMETERS")
  private List<UsageParameter> usageParameters;

  /**
   * Janino bytecode / language level for compiling embedded user classes (same semantics as {@link
   * JaninoMeta#getEffectiveJavaTargetVersion()}).
   */
  @Getter
  @Setter(AccessLevel.NONE)
  @HopMetadataProperty(key = "java_target_version")
  private int javaTargetVersion = JaninoMeta.JAVA_TARGET_VERSION_DEFAULT;

  public void setJavaTargetVersion(int javaTargetVersion) {
    if (this.javaTargetVersion != javaTargetVersion) {
      this.javaTargetVersion = javaTargetVersion;
      this.hasChanged = true;
    }
  }

  /**
   * Resolved Janino compiler source/target version for {@link ClassBodyEvaluator}, for backwards
   * compatibility when pipelines omit {@link #javaTargetVersion} or contain invalid values.
   */
  public int getEffectiveJavaTargetVersion() {
    if (javaTargetVersion < JaninoMeta.JAVA_TARGET_VERSION_MIN
        || javaTargetVersion > JaninoMeta.JAVA_TARGET_VERSION_MAX) {
      return JaninoMeta.JAVA_TARGET_VERSION_DEFAULT;
    }
    return javaTargetVersion;
  }

  public UserDefinedJavaClassMeta() {
    super();
    hasChanged = true;
    cookErrors = new ArrayList<>();
    infoTransformDefinitions = new ArrayList<>();
    targetTransformDefinitions = new ArrayList<>();
    usageParameters = new ArrayList<>();
  }

  public UserDefinedJavaClassMeta(UserDefinedJavaClassMeta m) {
    this();
    this.cookedTransformClass = null;
    this.clearingResultFields = m.clearingResultFields;
    m.fields.forEach(f -> this.fields.add(new FieldInfo(f)));
    m.definitions.forEach(d -> this.definitions.add(new UserDefinedJavaClassDef(d)));
    m.infoTransformDefinitions.forEach(
        d -> this.infoTransformDefinitions.add(new InfoTransformDefinition(d)));
    m.targetTransformDefinitions.forEach(
        d -> this.targetTransformDefinitions.add(new TargetTransformDefinition(d)));
    m.usageParameters.forEach(u -> this.usageParameters.add(new UsageParameter(u)));
    this.javaTargetVersion = m.javaTargetVersion;
  }

  @VisibleForTesting
  Class<?> cookClass(UserDefinedJavaClassDef def, ClassLoader clsLoader)
      throws CompileException, IOException, HopTransformException {

    String cacheKey = def.getChecksum() + ":" + getEffectiveJavaTargetVersion();
    Class<?> rtn = UserDefinedJavaClassMeta.CLASS_CACHE.getIfPresent(cacheKey);
    if (rtn != null) {
      return rtn;
    }

    if (Thread.currentThread().getContextClassLoader() == null) {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    }

    ClassBodyEvaluator cbe = new ClassBodyEvaluator();
    if (clsLoader == null) {
      cbe.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    } else {
      cbe.setParentClassLoader(clsLoader);
    }

    cbe.setClassName(def.getClassName());

    StringReader sr;
    if (def.isTransformClass()) {
      cbe.setExtendedType(TransformClassBase.class);
      sr = new StringReader(def.getTransformedSource());
    } else {
      sr = new StringReader(def.getSource());
    }

    cbe.setDefaultImports(
        "org.apache.hop.pipeline.transforms.userdefinedjavaclass.*",
        "org.apache.hop.pipeline.transform.*",
        "org.apache.hop.core.row.*",
        "org.apache.hop.core.*",
        "org.apache.hop.core.exception.*",
        "org.apache.hop.pipeline.*",
        "org.apache.hop.workflow.*",
        "org.apache.hop.workflow.action.*",
        "org.apache.hop.core.plugins.*",
        "org.apache.hop.core.variables.*",
        "java.util.*");

    int javaVersion = getEffectiveJavaTargetVersion();
    cbe.setTargetVersion(javaVersion);
    if (javaVersion > JaninoMeta.JAVA_TARGET_VERSION_MIN) {
      cbe.setSourceVersion(javaVersion);
    }

    cbe.cook(new Scanner(null, sr));
    rtn = cbe.getClazz();
    UserDefinedJavaClassMeta.CLASS_CACHE.put(cacheKey, rtn);
    return rtn;
  }

  public void cookClasses() throws HopException {
    cookErrors.clear();
    ClassLoader clsloader = UserDefinedJavaClass.class.getClassLoader();
    for (UserDefinedJavaClassDef def : getDefinitions()) {
      try {
        // Validate Formula
        JaninoCheckerUtil janinoCheckerUtil = new JaninoCheckerUtil();
        List<String> codeCheck = janinoCheckerUtil.checkCode(def.getSource());
        if (!codeCheck.isEmpty()) {
          throw new HopException("Script contains code that is not allowed : " + codeCheck);
        }
        Class<?> cookedClass = cookClass(def, clsloader);
        clsloader = cookedClass.getClassLoader();
        if (def.isTransformClass()) {
          cookedTransformClass = (Class<TransformClassBase>) cookedClass;
        }

      } catch (Throwable e) {
        CompileException exception = new CompileException(e.getMessage(), null);
        exception.setStackTrace(new StackTraceElement[] {});
        cookErrors.add(exception);
      }
    }
    hasChanged = false;
  }

  public TransformClassBase newChildInstance(
      UserDefinedJavaClass parent, UserDefinedJavaClassMeta meta, UserDefinedJavaClassData data) {
    if (checkClassCooked(getLog())) {
      return null;
    }

    try {
      return cookedTransformClass
          .getConstructor(
              UserDefinedJavaClass.class,
              UserDefinedJavaClassMeta.class,
              UserDefinedJavaClassData.class)
          .newInstance(parent, meta, data);
    } catch (Exception e) {
      if (log.isDebug()) {
        log.logError(
            "Full debugging stacktrace of UserDefinedJavaClass instanciation exception:",
            e.getCause());
      }
      HopException hopException = new HopException(e.getMessage());
      hopException.setStackTrace(new StackTraceElement[] {});
      cookErrors.add(hopException);
      return null;
    }
  }

  public void setFieldInfo(List<FieldInfo> fields) {
    replaceFields(fields);
  }

  public void replaceFields(List<FieldInfo> fields) {
    this.fields = fields;
    hasChanged = true;
  }

  /**
   * This method orders the classes by sorting all the normal classes by alphabetic order and then
   * sorting all the transaction classes by alphabetic order. This makes the resolution of classes
   * deterministic by type and then by class name.
   *
   * @param definitions - Unorder list of user defined classes
   * @return - Ordered list of user defined classes
   */
  @VisibleForTesting
  protected List<UserDefinedJavaClassDef> orderDefinitions(
      List<UserDefinedJavaClassDef> definitions) {
    List<UserDefinedJavaClassDef> orderedDefinitions = new ArrayList<>(definitions.size());
    List<UserDefinedJavaClassDef> transactions =
        definitions.stream()
            .filter(UserDefinedJavaClassDef::isTransformClass)
            .sorted(Comparator.comparing(UserDefinedJavaClassDef::getClassName))
            .toList();

    List<UserDefinedJavaClassDef> normalClasses =
        definitions.stream()
            .filter(def -> !def.isTransformClass())
            .sorted(Comparator.comparing(UserDefinedJavaClassDef::getClassName))
            .toList();

    orderedDefinitions.addAll(normalClasses);
    orderedDefinitions.addAll(transactions);
    return orderedDefinitions;
  }

  public void replaceDefinitions(List<UserDefinedJavaClassDef> definitions) {
    this.definitions.clear();
    this.definitions = orderDefinitions(definitions);
    hasChanged = true;
  }

  @Override
  public UserDefinedJavaClassMeta clone() {
    return new UserDefinedJavaClassMeta(this);
  }

  private boolean checkClassCooked(ILogChannel logChannel) {
    boolean ok = cookedTransformClass != null && cookErrors.isEmpty();
    if (hasChanged) {
      try {
        cookClasses();
      } catch (HopException e) {
        throw new HopRuntimeException("Error cooking class", e);
      }

      if (cookedTransformClass == null) {
        if (!cookErrors.isEmpty()) {
          logChannel.logDebug(
              BaseMessages.getString(
                  PKG, "UserDefinedJavaClass.Exception.CookingError", cookErrors.getFirst()));
        }
        ok = false;
      } else {
        ok = true;
      }
    }
    return !ok;
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    if (checkClassCooked(getLog())) {
      return super.getTransformIOMeta();
    }

    try {
      Method getTransformIoMeta =
          cookedTransformClass.getMethod("getTransformIOMeta", UserDefinedJavaClassMeta.class);
      ITransformIOMeta transformIoMeta = (ITransformIOMeta) getTransformIoMeta.invoke(null, this);
      if (transformIoMeta == null) {
        return super.getTransformIOMeta();
      } else {
        return transformIoMeta;
      }
    } catch (Exception e) {
      throw new HopRuntimeException("Error getting transform IO meta for UserDefinedJavaClass", e);
    }
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    for (InfoTransformDefinition transformDefinition : infoTransformDefinitions) {
      transformDefinition.transformMeta =
          TransformMeta.findTransform(transforms, transformDefinition.getTransformName());
    }
    for (TargetTransformDefinition transformDefinition : targetTransformDefinitions) {
      transformDefinition.transformMeta =
          TransformMeta.findTransform(transforms, transformDefinition.transformName);
    }
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
    if (checkClassCooked(getLog())) {
      if (!cookErrors.isEmpty()) {
        throw new HopTransformException(
            "Error initializing UserDefinedJavaClass to get fields: ", cookErrors.getFirst());
      } else {
        return;
      }
    }

    try {
      Method getFieldsMethod =
          cookedTransformClass.getMethod(
              "getFields",
              boolean.class,
              IRowMeta.class,
              String.class,
              IRowMeta[].class,
              TransformMeta.class,
              IVariables.class,
              List.class);
      getFieldsMethod.invoke(
          null,
          isClearingResultFields(),
          row,
          originTransformName,
          info,
          nextTransform,
          variables,
          getFields());
    } catch (Exception e) {
      throw new HopTransformException("Error executing UserDefinedJavaClass.getFields(): ", e);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformInfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "UserDefinedJavaClassMeta.CheckResult.ConnectedTransformOK2"),
              transformInfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "UserDefinedJavaClassMeta.CheckResult.NoInputReceived"),
              transformInfo);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }
}
