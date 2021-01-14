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
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.DataTypeConverter;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.injection.NullNumberConverter;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.w3c.dom.Node;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@InjectionSupported(
    localizationPrefix = "UserDefinedJavaClass.Injection.",
    groups = {"PARAMETERS", "TARGET_TRANSFORMS", "INFO_TRANSFORMS", "JAVA_CLASSES", "FIELD_INFO"})
@Transform(
    id = "UserDefinedJavaClass",
    image = "userdefinedjavaclass.svg",
    name = "i18n::UserDefinedJavaClass.Name",
    description = "i18n::UserDefinedJavaClass.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/userdefinedjavaclass.html")
public class UserDefinedJavaClassMeta extends BaseTransformMeta
    implements ITransformMeta<UserDefinedJavaClass, UserDefinedJavaClassData> {
  private static final Class<?> PKG = UserDefinedJavaClassMeta.class; // For Translator

  public enum ElementNames {
    classType,
    className,
    class_source,
    definitions,
    definition,
    fields,
    field,
    fieldName,
    fieldType,
    fieldLength,
    fieldPrecision,
    clearResultFields,

    infoTransforms,
    infoTransform,
    info_,
    target_transforms,
    target_transform,
    target_,

    transform_tag,
    transformName,
    transformDescription,

    usage_parameters,
    usage_parameter,
    parameter_tag,
    parameterValue,
    parameterDescription,
  }

  @InjectionDeep private List<FieldInfo> fields = new ArrayList<FieldInfo>();

  @InjectionDeep
  private List<UserDefinedJavaClassDef> definitions = new ArrayList<UserDefinedJavaClassDef>();

  public Class<TransformClassBase> cookedTransformClass;
  public List<Exception> cookErrors = new ArrayList<Exception>(0);
  private static final Cache<String, Class<?>> classCache;

  @Injection(name = "CLEAR_RESULT_FIELDS")
  private boolean clearingResultFields;

  private boolean changed;

  @InjectionDeep private List<InfoTransformDefinition> infoTransformDefinitions;
  @InjectionDeep private List<TargetTransformDefinition> targetTransformDefinitions;
  @InjectionDeep private List<UsageParameter> usageParameters;

  static {
    IVariables vs = new Variables();
    vs.initializeFrom(null); // sets up the default variables
    String maxSizeStr = vs.getVariable(UserDefinedJavaClass.HOP_DEFAULT_CLASS_CACHE_SIZE, "100");
    int maxCacheSize = -1;
    try {
      maxCacheSize = Integer.parseInt(maxSizeStr);
    } catch (Exception ignored) {
      maxCacheSize = 100; // default to 100 if property not set
    }
    // Initialize Class ICache
    classCache = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build();
  }

  public static class FieldInfo implements Cloneable {
    @Injection(name = "FIELD_NAME", group = "FIELD_INFO")
    public final String name;

    @Injection(
        name = "FIELD_TYPE",
        group = "FIELD_INFO",
        convertEmpty = true,
        converter = DataTypeConverter.class)
    public final int type;

    @Injection(
        name = "FIELD_LENGTH",
        group = "FIELD_INFO",
        convertEmpty = true,
        converter = NullNumberConverter.class)
    public final int length;

    @Injection(
        name = "FIELD_PRECISION",
        group = "FIELD_INFO",
        convertEmpty = true,
        converter = NullNumberConverter.class)
    public final int precision;

    public FieldInfo() {
      this(null, IValueMeta.TYPE_STRING, -1, -1);
    }

    public FieldInfo(String name, int type, int length, int precision) {
      super();
      this.name = name;
      this.type = type;
      this.length = length;
      this.precision = precision;
    }

    public Object clone() throws CloneNotSupportedException {
      return super.clone();
    }
  }

  public UserDefinedJavaClassMeta() {
    super();
    changed = true;
    infoTransformDefinitions = new ArrayList<>();
    targetTransformDefinitions = new ArrayList<>();
    usageParameters = new ArrayList<>();
  }

  @VisibleForTesting
  Class<?> cookClass(UserDefinedJavaClassDef def, ClassLoader clsloader)
      throws CompileException, IOException, RuntimeException, HopTransformException {

    String checksum = def.getChecksum();
    Class<?> rtn = UserDefinedJavaClassMeta.classCache.getIfPresent(checksum);
    if (rtn != null) {
      return rtn;
    }

    if (Thread.currentThread().getContextClassLoader() == null) {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    }

    ClassBodyEvaluator cbe = new ClassBodyEvaluator();
    if (clsloader == null) {
      cbe.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    } else {
      cbe.setParentClassLoader(clsloader);
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
        new String[] {
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
          "java.util.*",
        });

    cbe.cook(new Scanner(null, sr));
    rtn = cbe.getClazz();
    UserDefinedJavaClassMeta.classCache.put(checksum, rtn);
    return rtn;
  }

  @SuppressWarnings("unchecked")
  public void cookClasses() {
    cookErrors.clear();
    ClassLoader clsloader = UserDefinedJavaClass.class.getClassLoader();
    for (UserDefinedJavaClassDef def : getDefinitions()) {
      if (def.isActive()) {
        try {
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
    }
    changed = false;
  }

  public TransformClassBase newChildInstance(
      UserDefinedJavaClass parent, UserDefinedJavaClassMeta meta, UserDefinedJavaClassData data) {
    if (!checkClassCookings(getLog())) {
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
      HopException kettleException = new HopException(e.getMessage());
      kettleException.setStackTrace(new StackTraceElement[] {});
      cookErrors.add(kettleException);
      return null;
    }
  }

  public List<FieldInfo> getFieldInfo() {
    return Collections.unmodifiableList(fields);
  }

  public void setFieldInfo(List<FieldInfo> fields) {
    replaceFields(fields);
  }

  public void replaceFields(List<FieldInfo> fields) {
    this.fields = fields;
    changed = true;
  }

  public List<UserDefinedJavaClassDef> getDefinitions() {
    return Collections.unmodifiableList(definitions);
  }

  /**
   * This method oders the classes by sorting all the normal classes by alphabetic order and then
   * sorting all the transaction classes by alphabetical order. This makes the resolution of classes
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
            .filter(def -> def.isTransformClass() && def.isActive())
            .sorted((p1, p2) -> p1.getClassName().compareTo(p2.getClassName()))
            .collect(Collectors.toList());

    List<UserDefinedJavaClassDef> normalClasses =
        definitions.stream()
            .filter(def -> !def.isTransformClass())
            .sorted((p1, p2) -> p1.getClassName().compareTo(p2.getClassName()))
            .collect(Collectors.toList());

    orderedDefinitions.addAll(normalClasses);
    orderedDefinitions.addAll(transactions);
    return orderedDefinitions;
  }

  public void replaceDefinitions(List<UserDefinedJavaClassDef> definitions) {
    this.definitions.clear();
    this.definitions = orderDefinitions(definitions);
    changed = true;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    try {

      UserDefinedJavaClassMeta retval = (UserDefinedJavaClassMeta) super.clone();

      if (fields != null) {
        List<FieldInfo> newFields = new ArrayList<FieldInfo>(fields.size());
        for (FieldInfo field : fields) {
          newFields.add((FieldInfo) field.clone());
        }
        retval.fields = newFields;
      }

      if (definitions != null) {
        List<UserDefinedJavaClassDef> newDefinitions = new ArrayList<UserDefinedJavaClassDef>();
        for (UserDefinedJavaClassDef def : definitions) {
          newDefinitions.add((UserDefinedJavaClassDef) def.clone());
        }
        retval.definitions = newDefinitions;
      }

      retval.cookedTransformClass = null;
      retval.cookErrors = new ArrayList<Exception>(0);

      if (infoTransformDefinitions != null) {
        List<InfoTransformDefinition> newInfoTransformDefinitions = new ArrayList<>();
        for (InfoTransformDefinition transform : infoTransformDefinitions) {
          newInfoTransformDefinitions.add((InfoTransformDefinition) transform.clone());
        }
        retval.infoTransformDefinitions = newInfoTransformDefinitions;
      }

      if (targetTransformDefinitions != null) {
        List<TargetTransformDefinition> newTargetTransformDefinitions = new ArrayList<>();
        for (TargetTransformDefinition transform : targetTransformDefinitions) {
          newTargetTransformDefinitions.add((TargetTransformDefinition) transform.clone());
        }
        retval.targetTransformDefinitions = newTargetTransformDefinitions;
      }

      if (usageParameters != null) {
        List<UsageParameter> newUsageParameters = new ArrayList<UsageParameter>();
        for (UsageParameter param : usageParameters) {
          newUsageParameters.add((UsageParameter) param.clone());
        }
        retval.usageParameters = newUsageParameters;
      }

      return retval;

    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node definitionsNode = XmlHandler.getSubNode(transformNode, ElementNames.definitions.name());
      int nrDefinitions = XmlHandler.countNodes(definitionsNode, ElementNames.definition.name());

      for (int i = 0; i < nrDefinitions; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(definitionsNode, ElementNames.definition.name(), i);
        definitions.add(
            new UserDefinedJavaClassDef(
                UserDefinedJavaClassDef.ClassType.valueOf(
                    XmlHandler.getTagValue(fnode, ElementNames.classType.name())),
                XmlHandler.getTagValue(fnode, ElementNames.className.name()),
                XmlHandler.getTagValue(fnode, ElementNames.class_source.name())));
      }
      definitions = orderDefinitions(definitions);

      Node fieldsNode = XmlHandler.getSubNode(transformNode, ElementNames.fields.name());
      int nrFields = XmlHandler.countNodes(fieldsNode, ElementNames.field.name());

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fieldsNode, ElementNames.field.name(), i);
        fields.add(
            new FieldInfo(
                XmlHandler.getTagValue(fnode, ElementNames.fieldName.name()),
                ValueMetaFactory.getIdForValueMeta(
                    XmlHandler.getTagValue(fnode, ElementNames.fieldType.name())),
                Const.toInt(XmlHandler.getTagValue(fnode, ElementNames.fieldLength.name()), -1),
                Const.toInt(
                    XmlHandler.getTagValue(fnode, ElementNames.fieldPrecision.name()), -1)));
      }

      setClearingResultFields(
          !"N"
              .equals(
                  XmlHandler.getTagValue(transformNode, ElementNames.clearResultFields.name())));

      infoTransformDefinitions.clear();
      Node infosNode = XmlHandler.getSubNode(transformNode, ElementNames.infoTransforms.name());
      int nrInfos = XmlHandler.countNodes(infosNode, ElementNames.infoTransform.name());
      for (int i = 0; i < nrInfos; i++) {
        Node infoNode = XmlHandler.getSubNodeByNr(infosNode, ElementNames.infoTransform.name(), i);
        InfoTransformDefinition transformDefinition = new InfoTransformDefinition();
        transformDefinition.tag =
            XmlHandler.getTagValue(infoNode, ElementNames.transform_tag.name());
        transformDefinition.transformName =
            XmlHandler.getTagValue(infoNode, ElementNames.transformName.name());
        transformDefinition.description =
            XmlHandler.getTagValue(infoNode, ElementNames.transformDescription.name());
        infoTransformDefinitions.add(transformDefinition);
      }

      targetTransformDefinitions.clear();
      Node targetsNode =
          XmlHandler.getSubNode(transformNode, ElementNames.target_transforms.name());
      int nrTargets = XmlHandler.countNodes(targetsNode, ElementNames.target_transform.name());
      for (int i = 0; i < nrTargets; i++) {
        Node targetNode =
            XmlHandler.getSubNodeByNr(targetsNode, ElementNames.target_transform.name(), i);
        TargetTransformDefinition transformDefinition = new TargetTransformDefinition();
        transformDefinition.tag =
            XmlHandler.getTagValue(targetNode, ElementNames.transform_tag.name());
        transformDefinition.transformName =
            XmlHandler.getTagValue(targetNode, ElementNames.transformName.name());
        transformDefinition.description =
            XmlHandler.getTagValue(targetNode, ElementNames.transformDescription.name());
        targetTransformDefinitions.add(transformDefinition);
      }

      usageParameters.clear();
      Node parametersNode =
          XmlHandler.getSubNode(transformNode, ElementNames.usage_parameters.name());
      int nrParameters = XmlHandler.countNodes(parametersNode, ElementNames.usage_parameter.name());
      for (int i = 0; i < nrParameters; i++) {
        Node parameterNode =
            XmlHandler.getSubNodeByNr(parametersNode, ElementNames.usage_parameter.name(), i);
        UsageParameter usageParameter = new UsageParameter();
        usageParameter.tag =
            XmlHandler.getTagValue(parameterNode, ElementNames.parameter_tag.name());
        usageParameter.value =
            XmlHandler.getTagValue(parameterNode, ElementNames.parameterValue.name());
        usageParameter.description =
            XmlHandler.getTagValue(parameterNode, ElementNames.parameterDescription.name());
        usageParameters.add(usageParameter);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "UserDefinedJavaClassMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    // Moved the default code generation out of Meta since the Snippits class is in the UI package
    // which isn't in the
    // classpath.
  }

  private boolean checkClassCookings(ILogChannel logChannel) {
    boolean ok = cookedTransformClass != null && cookErrors.size() == 0;
    if (changed) {
      cookClasses();
      if (cookedTransformClass == null) {
        if (cookErrors.size() > 0) {
          logChannel.logDebug(
              BaseMessages.getString(
                  PKG, "UserDefinedJavaClass.Exception.CookingError", cookErrors.get(0)));
        }
        ok = false;
      } else {
        ok = true;
      }
    }
    return ok;
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    if (!checkClassCookings(getLog())) {
      return super.getTransformIOMeta();
    }

    try {
      Method getTransformIOMeta =
          cookedTransformClass.getMethod("getTransformIOMeta", UserDefinedJavaClassMeta.class);
      if (getTransformIOMeta != null) {
        ITransformIOMeta transformIoMeta = (ITransformIOMeta) getTransformIOMeta.invoke(null, this);
        if (transformIoMeta == null) {
          return super.getTransformIOMeta();
        } else {
          return transformIoMeta;
        }
      } else {
        return super.getTransformIOMeta();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return super.getTransformIOMeta();
    }
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    for (InfoTransformDefinition transformDefinition : infoTransformDefinitions) {
      transformDefinition.transformMeta =
          TransformMeta.findTransform(transforms, transformDefinition.transformName);
    }
    for (TargetTransformDefinition transformDefinition : targetTransformDefinitions) {
      transformDefinition.transformMeta =
          TransformMeta.findTransform(transforms, transformDefinition.transformName);
    }
  }

  public void getFields(
      IRowMeta row,
      String originTransformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!checkClassCookings(getLog())) {
      if (cookErrors.size() > 0) {
        throw new HopTransformException(
            "Error initializing UserDefinedJavaClass to get fields: ", cookErrors.get(0));
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
          getFieldInfo());
    } catch (Exception e) {
      throw new HopTransformException("Error executing UserDefinedJavaClass.getFields(): ", e);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append(String.format("\n    <%s>", ElementNames.definitions.name()));
    for (UserDefinedJavaClassDef def : definitions) {
      retval.append(String.format("\n        <%s>", ElementNames.definition.name()));
      retval
          .append("\n        ")
          .append(XmlHandler.addTagValue(ElementNames.classType.name(), def.getClassType().name()));
      retval
          .append("\n        ")
          .append(XmlHandler.addTagValue(ElementNames.className.name(), def.getClassName()));
      retval.append("\n        ");
      retval.append(XmlHandler.addTagValue(ElementNames.class_source.name(), def.getSource()));
      retval.append(String.format("\n        </%s>", ElementNames.definition.name()));
    }
    retval.append(String.format("\n    </%s>", ElementNames.definitions.name()));

    retval.append(String.format("\n    <%s>", ElementNames.fields.name()));
    for (FieldInfo fi : fields) {
      retval.append(String.format("\n        <%s>", ElementNames.field.name()));
      retval
          .append("\n        ")
          .append(XmlHandler.addTagValue(ElementNames.fieldName.name(), fi.name));
      retval
          .append("\n        ")
          .append(
              XmlHandler.addTagValue(
                  ElementNames.fieldType.name(), ValueMetaFactory.getValueMetaName(fi.type)));
      retval
          .append("\n        ")
          .append(XmlHandler.addTagValue(ElementNames.fieldLength.name(), fi.length));
      retval
          .append("\n        ")
          .append(XmlHandler.addTagValue(ElementNames.fieldPrecision.name(), fi.precision));
      retval.append(String.format("\n        </%s>", ElementNames.field.name()));
    }
    retval.append(String.format("\n    </%s>", ElementNames.fields.name()));
    retval.append(
        XmlHandler.addTagValue(ElementNames.clearResultFields.name(), clearingResultFields));

    // Add the XML for the info transform definitions...
    //
    retval.append(XmlHandler.openTag(ElementNames.infoTransforms.name()));
    for (InfoTransformDefinition transformDefinition : infoTransformDefinitions) {
      retval.append(XmlHandler.openTag(ElementNames.infoTransform.name()));
      retval.append(
          XmlHandler.addTagValue(ElementNames.transform_tag.name(), transformDefinition.tag));
      retval.append(
          XmlHandler.addTagValue(
              ElementNames.transformName.name(),
              transformDefinition.transformMeta != null
                  ? transformDefinition.transformMeta.getName()
                  : null));
      retval.append(
          XmlHandler.addTagValue(
              ElementNames.transformDescription.name(), transformDefinition.description));
      retval.append(XmlHandler.closeTag(ElementNames.infoTransform.name()));
    }
    retval.append(XmlHandler.closeTag(ElementNames.infoTransforms.name()));

    // Add the XML for the target transform definitions...
    //
    retval.append(XmlHandler.openTag(ElementNames.target_transforms.name()));
    for (TargetTransformDefinition transformDefinition : targetTransformDefinitions) {
      retval.append(XmlHandler.openTag(ElementNames.target_transform.name()));
      retval.append(
          XmlHandler.addTagValue(ElementNames.transform_tag.name(), transformDefinition.tag));
      retval.append(
          XmlHandler.addTagValue(
              ElementNames.transformName.name(),
              transformDefinition.transformMeta != null
                  ? transformDefinition.transformMeta.getName()
                  : null));
      retval.append(
          XmlHandler.addTagValue(
              ElementNames.transformDescription.name(), transformDefinition.description));
      retval.append(XmlHandler.closeTag(ElementNames.target_transform.name()));
    }
    retval.append(XmlHandler.closeTag(ElementNames.target_transforms.name()));

    retval.append(XmlHandler.openTag(ElementNames.usage_parameters.name()));
    for (UsageParameter usageParameter : usageParameters) {
      retval.append(XmlHandler.openTag(ElementNames.usage_parameter.name()));
      retval.append(XmlHandler.addTagValue(ElementNames.parameter_tag.name(), usageParameter.tag));
      retval.append(
          XmlHandler.addTagValue(ElementNames.parameterValue.name(), usageParameter.value));
      retval.append(
          XmlHandler.addTagValue(
              ElementNames.parameterDescription.name(), usageParameter.description));
      retval.append(XmlHandler.closeTag(ElementNames.usage_parameter.name()));
    }
    retval.append(XmlHandler.closeTag(ElementNames.usage_parameters.name()));

    return retval.toString();
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
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
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "UserDefinedJavaClassMeta.CheckResult.NoInputReceived"),
              transforminfo);
      remarks.add(cr);
    }
  }

  @Override
  public UserDefinedJavaClass createTransform(
      TransformMeta transformMeta,
      UserDefinedJavaClassData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    UserDefinedJavaClass userDefinedJavaClass =
        new UserDefinedJavaClass(transformMeta, this, data, cnr, pipelineMeta, pipeline);
    if (pipeline.hasHaltedComponents()) {
      return null;
    }

    return userDefinedJavaClass;
  }

  public UserDefinedJavaClassData getTransformData() {
    return new UserDefinedJavaClassData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the clearingResultFields */
  public boolean isClearingResultFields() {
    return clearingResultFields;
  }

  /** @param clearingResultFields the clearingResultFields to set */
  public void setClearingResultFields(boolean clearingResultFields) {
    this.clearingResultFields = clearingResultFields;
  }

  /** @return the infoTransformDefinitions */
  public List<InfoTransformDefinition> getInfoTransformDefinitions() {
    return infoTransformDefinitions;
  }

  /** @param infoTransformDefinitions the infoTransformDefinitions to set */
  public void setInfoTransformDefinitions(List<InfoTransformDefinition> infoTransformDefinitions) {
    this.infoTransformDefinitions = infoTransformDefinitions;
  }

  /** @return the targetTransformDefinitions */
  public List<TargetTransformDefinition> getTargetTransformDefinitions() {
    return targetTransformDefinitions;
  }

  /** @param targetTransformDefinitions the targetTransformDefinitions to set */
  public void setTargetTransformDefinitions(
      List<TargetTransformDefinition> targetTransformDefinitions) {
    this.targetTransformDefinitions = targetTransformDefinitions;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  /** @return the usageParameters */
  public List<UsageParameter> getUsageParameters() {
    return usageParameters;
  }

  /** @param usageParameters the usageParameters to set */
  public void setUsageParameters(List<UsageParameter> usageParameters) {
    this.usageParameters = usageParameters;
  }
}
