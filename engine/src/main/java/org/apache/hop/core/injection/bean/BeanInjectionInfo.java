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

package org.apache.hop.core.injection.bean;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.util.ReflectionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Storage for bean annotations info for Metadata Injection and Load/Save. */
public class BeanInjectionInfo<Meta extends Object> {
  private static ILogChannel log;

  protected final Class<Meta> clazz;
  protected final InjectionSupported clazzAnnotation;
  protected Map<String, Property> properties;
  protected List<Group> groupsList;

  /** Used only for fast group search during initialize. */
  protected Map<String, Group> groupsMap;

  protected Set<String> hideProperties;

  public BeanInjectionInfo(Class<Meta> clazz) {
    properties = new HashMap<>();
    groupsList = new ArrayList<>();
    groupsMap = new HashMap<>();
    hideProperties = new HashSet<>();
    log = LogChannel.GENERAL;

    if (log.isDebug()) {
      log.logDebug("Collect bean injection info for " + clazz);
    }
    try {
      this.clazz = clazz;
      clazzAnnotation = clazz.getAnnotation(InjectionSupported.class);
      if (!isInjectionSupported(clazz)) {
        throw new RuntimeException("Injection not supported in " + clazz);
      }

      if (clazzAnnotation == null) {
        extractMetadataProperties(clazz);
      } else {
        extractInjectionInfo(clazz);
      }
    } catch (Throwable ex) {
      log.logError(
          "Error bean injection info collection for " + clazz + ": " + ex.getMessage(), ex);
      throw ex;
    }
  }

  public static <Meta extends Object> boolean isInjectionSupported(Class<Meta> clazz) {
    InjectionSupported annotation = clazz.getAnnotation(InjectionSupported.class);
    if (annotation != null) {
      return true;
    }

    // See if there are any Hop metadata properties we can use...
    //
    for (Field field : ReflectionUtil.findAllFields(clazz)) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null) {
        return true;
      }
    }

    return false;
  }

  private void extractMetadataProperties(Class<Meta> clazz) {
    Map<Field, HopMetadataProperty> propertyFields = new HashMap<>();
    for (Field field : ReflectionUtil.findAllFields(clazz)) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null) {
        propertyFields.put(field, property);
      }
    }

    if (propertyFields.isEmpty()) {
      throw new RuntimeException("Injection not supported in " + clazz);
    }

    // The root Bean Level Info
    //
    BeanLevelInfo<Meta> classLevelInfo = new BeanLevelInfo<>();
    classLevelInfo.leafClass = clazz;

    // Add an empty group...
    //
    Group gr0 = new Group("", "");
    groupsList.add(gr0);
    groupsMap.put(gr0.getKey(), gr0);

    for (Field field : propertyFields.keySet()) {
      Class<?> fieldType = field.getType();

      HopMetadataProperty property = propertyFields.get(field);

      String injectionKey = calculateInjectionKey(field, property);
      String injectionKeyDescription = calculateInjectionKeyDescription(property);
      String injectionGroupKey = calculateInjectionGroupKey(property);
      String injectionGroupDescription = calculateInjectionGroupDescription(property);

      // Class Bean Level Info...
      //
      BeanLevelInfo fieldLevelInfo =
          getLevelInfo(clazz, classLevelInfo, field, fieldType, property, injectionKey);

      Group group = null;
      if (StringUtils.isNotEmpty(injectionGroupKey)) {
        group = new Group(injectionGroupKey, injectionGroupDescription);

        groupsList.add(group);
        groupsMap.put(injectionGroupKey, group);
      }

      if (StringUtils.isNotEmpty(injectionGroupKey)) {
        // Find child properties of the field class...
        // If not a POJO we simply have the one property
        //

        // If it's a List: get that type...
        //
        if (fieldType.equals(java.util.List.class)) {
          ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
          // Take the generic class to pick the child properties from.
          //
          fieldType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
          fieldLevelInfo.dim = BeanLevelInfo.DIMENSION.LIST;
          fieldLevelInfo.leafClass = fieldType; // Not List but the listed type
        }

        // Special case: List<String>
        //
        if (String.class.equals(fieldType)) {
          fieldLevelInfo.leafClass = field.getType();

          // Child bean level info...
          //
          BeanLevelInfo childLevelInfo =
              getLevelInfo(fieldType, fieldLevelInfo, null, fieldType, property, injectionKey);
          childLevelInfo.stringList = true;

          List<BeanLevelInfo> path = Arrays.asList(classLevelInfo, fieldLevelInfo, childLevelInfo);
          Property p = new Property(injectionKey, injectionKeyDescription, injectionGroupKey, path);
          group.properties.add(p);
          properties.put(injectionKey, p);
        } else {
          for (Field childField : ReflectionUtil.findAllFields(fieldType)) {
            Class<?> childFieldType = childField.getType();
            HopMetadataProperty childProperty = childField.getAnnotation(HopMetadataProperty.class);
            if (childProperty != null) {
              String childInjectionKey = calculateInjectionKey(childField, childProperty);
              String childInjectionKeyDescription = calculateInjectionKeyDescription(childProperty);

              // Child bean level info...
              //
              BeanLevelInfo childLevelInfo =
                  getLevelInfo(
                      fieldType,
                      fieldLevelInfo,
                      childField,
                      childFieldType,
                      childProperty,
                      childInjectionKey);

              List<BeanLevelInfo> path =
                  Arrays.asList(classLevelInfo, fieldLevelInfo, childLevelInfo);
              Property p =
                  new Property(
                      childInjectionKey, childInjectionKeyDescription, injectionGroupKey, path);
              group.properties.add(p);
              properties.put(childInjectionKey, p);
            }
          }
        }
      } else {
        // Normal property: add it to the root group
        //
        Property p =
            new Property(
                injectionKey,
                injectionKeyDescription,
                "",
                Arrays.asList(classLevelInfo, fieldLevelInfo));
        gr0.properties.add(p);
        properties.put(injectionKey, p);
      }
    }
    properties = Collections.unmodifiableMap(properties);
    groupsList = Collections.unmodifiableList(groupsList);
  }

  private BeanLevelInfo getLevelInfo(
      Class<?> parentClass,
      BeanLevelInfo parent,
      Field field,
      Class<?> fieldType,
      HopMetadataProperty property,
      String injectionKey) {
    BeanLevelInfo fieldLevelInfo = new BeanLevelInfo();
    fieldLevelInfo.parent = parent;
    fieldLevelInfo.leafClass = fieldType;
    if (property != null) {
      try {
        fieldLevelInfo.converter = property.injectionConverter().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Unable to instantiate injection metadata converter class "
                + property.injectionConverter().getName(),
            e);
      }
    }
    fieldLevelInfo.nameKey = injectionKey;
    fieldLevelInfo.field = field;
    if (field != null) {
      fieldLevelInfo.field.setAccessible(true);
    }
    fieldLevelInfo.dim = BeanLevelInfo.DIMENSION.NONE;
    boolean isBoolean = Boolean.class.equals(fieldType) || boolean.class.equals(fieldType);
    if (field != null) {
      try {
        fieldLevelInfo.getter =
            parentClass.getMethod(ReflectionUtil.getGetterMethodName(field.getName(), isBoolean));
      } catch (Exception e) {
        throw new RuntimeException(
            "Unable to find getter for field "
                + field.getName()
                + " in class "
                + parentClass.getName(),
            e);
      }
      try {
        fieldLevelInfo.setter =
            parentClass.getMethod(ReflectionUtil.getSetterMethodName(field.getName()), fieldType);
      } catch (Exception e) {
        throw new RuntimeException(
            "Unable to find setter for field "
                + field.getName()
                + " in class "
                + parentClass.getName(),
            e);
      }
    }
    return fieldLevelInfo;
  }

  private String calculateInjectionGroupDescription(HopMetadataProperty property) {
    String injectionGroupDescription = property.injectionGroupDescription();
    if (StringUtils.isEmpty(injectionGroupDescription)) {
      injectionGroupDescription = "";
    }
    return injectionGroupDescription;
  }

  private String calculateInjectionGroupKey(HopMetadataProperty property) {
    String injectionGroupKey = property.injectionGroupKey();
    if (StringUtils.isEmpty(injectionGroupKey)) {
      injectionGroupKey = property.groupKey();
    }
    return injectionGroupKey;
  }

  private String calculateInjectionKeyDescription(HopMetadataProperty property) {
    String injectionKeyDescription = property.injectionKeyDescription();
    if (StringUtils.isEmpty(injectionKeyDescription)) {
      injectionKeyDescription = "";
    }
    return injectionKeyDescription;
  }

  private String calculateInjectionKey(Field field, HopMetadataProperty property) {
    String injectionKey = property.injectionKey();
    if (StringUtils.isEmpty(injectionKey)) {
      injectionKey = property.key();
    }
    if (StringUtils.isEmpty(injectionKey)) {
      injectionKey = field.getName();
    }
    return injectionKey;
  }

  private void extractInjectionInfo(Class<Meta> clazz) {
    Map<String, Type> parameterTypesMap = new HashMap<>();
    TypeVariable<? extends Class<?>>[] typeParameters = clazz.getTypeParameters();
    for (TypeVariable<? extends Class<?>> typeParameter : typeParameters) {
      String parameterTypeName = typeParameter.getName();
      Type parameterType = typeParameter.getBounds()[0];
      parameterTypesMap.put(parameterTypeName, parameterType);
    }

    // Add an empty group
    //
    Group gr0 = new Group("", "");
    groupsList.add(gr0);
    groupsMap.put(gr0.getKey(), gr0);

    for (String group : clazzAnnotation.groups()) {
      String groupDescription = clazzAnnotation.localizationPrefix() + group;
      Group gr = new Group(group, groupDescription);
      groupsList.add(gr);
      groupsMap.put(gr.getKey(), gr);
    }
    for (String p : clazzAnnotation.hide()) {
      hideProperties.add(p);
    }

    BeanLevelInfo root = new BeanLevelInfo();
    root.leafClass = clazz;
    if (parameterTypesMap.isEmpty()) {
      root.init(this);
    } else {
      root.init(this, parameterTypesMap);
    }
    properties = Collections.unmodifiableMap(properties);
    groupsList = Collections.unmodifiableList(groupsList);
    groupsMap = null;
  }

  public String getLocalizationPrefix() {
    return clazzAnnotation.localizationPrefix();
  }

  public Map<String, Property> getProperties() {
    return properties;
  }

  public List<Group> getGroups() {
    return groupsList;
  }

  protected void addInjectionProperty(Injection metaInj, BeanLevelInfo leaf) {
    if (StringUtils.isBlank(metaInj.name())) {
      throw new RuntimeException("Property name shouldn't be blank in the " + clazz);
    }

    String propertyName = calcPropertyName(metaInj, leaf);
    if (properties.containsKey(propertyName)) {
      throw new RuntimeException("Property '" + propertyName + "' already defined for " + clazz);
    }

    // probably hidden
    if (hideProperties.contains(propertyName)) {
      return;
    }

    String injectionKeyDescription = clazzAnnotation.localizationPrefix() + propertyName;

    Property prop =
        new Property(
            propertyName, injectionKeyDescription, metaInj.group(), leaf.createCallStack());
    properties.put(prop.key, prop);
    Group gr = groupsMap.get(metaInj.group());
    if (gr == null) {
      throw new RuntimeException(
          "Group '"
              + metaInj.group()
              + "' for property '"
              + metaInj.name()
              + "' is not defined "
              + clazz);
    }
    gr.properties.add(prop);
  }

  public String getDescription(String description) {
    if (StringUtils.isEmpty(description)) {
      return "";
    }
    String translated = BaseMessages.getString(clazz, description);
    if (translated != null && translated.startsWith("!") && translated.endsWith("!")) {
      Class<?> baseClass = clazz.getSuperclass();
      while (baseClass != null) {
        InjectionSupported baseAnnotation = baseClass.getAnnotation(InjectionSupported.class);
        if (baseAnnotation != null) {
          translated = BaseMessages.getString(baseClass, description);
          if (translated != null && !translated.startsWith("!") && !translated.endsWith("!")) {
            return translated;
          }
        }
        baseClass = baseClass.getSuperclass();
      }
    }
    return translated;
  }

  private String calcPropertyName(Injection metaInj, BeanLevelInfo leaf) {
    String name = metaInj.name();
    while (leaf != null) {
      if (StringUtils.isNotEmpty(leaf.nameKey)) {
        name = leaf.nameKey + "." + name;
      }
      leaf = leaf.parent;
    }
    if (!name.equals(metaInj.name()) && !metaInj.group().isEmpty()) {
      // group exist with prefix
      throw new RuntimeException("Group shouldn't be declared with prefix in " + clazz);
    }
    return name;
  }

  public class Property {
    private final String key;
    private final String description;
    private final String groupKey;
    protected final List<BeanLevelInfo> path;
    public final int pathArraysCount;

    public Property(String key, String description, String groupKey, List<BeanLevelInfo> path) {
      this.key = key;
      this.description = description;
      this.groupKey = groupKey;
      this.path = path;
      int ac = 0;
      for (BeanLevelInfo level : path) {
        if (level.dim != BeanLevelInfo.DIMENSION.NONE) {
          ac++;
        }
      }
      pathArraysCount = ac;
    }

    public String getKey() {
      return key;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }

    public String getGroupKey() {
      return groupKey;
    }

    public String getTranslatedDescription() {
      return BeanInjectionInfo.this.getDescription(description);
    }

    public Class<?> getPropertyClass() {
      return path.get(path.size() - 1).leafClass;
    }

    public boolean hasMatch(String filterString) {
      if (StringUtils.isEmpty(filterString)) {
        return true;
      }
      if (getKey().toUpperCase().contains(filterString.toUpperCase())) {
        return true;
      }
      if (getTranslatedDescription().toUpperCase().contains(filterString.toUpperCase())) {
        return true;
      }
      return false;
    }

    /**
     * Gets path
     *
     * @return value of path
     */
    public List<BeanLevelInfo> getPath() {
      return path;
    }
  }

  public class Group {
    private final String key;
    private final String description;
    protected final List<Property> properties = new ArrayList<>();

    public Group(String key, String description) {
      this.key = key;
      this.description = description;
    }

    public String getKey() {
      return key;
    }

    /**
     * Gets groupDescription
     *
     * @return value of groupDescription
     */
    public String getDescription() {
      return description;
    }

    public List<Property> getProperties() {
      return Collections.unmodifiableList(properties);
    }

    public String getTranslatedDescription() {
      return BeanInjectionInfo.this.getDescription(description);
    }

    public boolean hasMatchingProperty(String filterString) {
      // Empty string always matches
      if (StringUtils.isEmpty(filterString)) {
        return true;
      }
      // The group name also matches
      //
      if (key.toUpperCase().contains(filterString.toUpperCase())) {
        return true;
      }
      for (Property property : properties) {
        if (property.hasMatch(filterString)) {
          return true;
        }
      }
      return false;
    }
  }
}
