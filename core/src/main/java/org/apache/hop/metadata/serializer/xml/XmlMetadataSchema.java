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

package org.apache.hop.metadata.serializer.xml;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IIntCodeConverter;
import org.apache.hop.metadata.util.ReflectionUtil;

/**
 * Generates W3C XML Schema Definition (XSD) representations for classes annotated with {@link
 * HopMetadataProperty}.
 */
@Getter
@Setter
public class XmlMetadataSchema {

  public static final String XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema";
  public static final String HOP_BOOLEAN_TYPE = "HopBoolean";

  private Class<?> rootClass;
  private String rootElementName;
  private boolean includeLaxAny = false;
  private boolean includeHopBooleanType = true;
  private boolean flexibleElementOrder = true;

  /** Substitutions for interfaces/classes (e.g. replace ITransformMeta with TableInputMeta) */
  private Map<Class<?>, Class<?>> classSubstitutions = new HashMap<>();

  /** Fixed element values by tag name (e.g. "type" -> "TableInput") */
  private Map<String, String> fixedFieldValues = new HashMap<>();

  /** Extra root elements to declare (e.g. <TableInput type="TableInputMetaType"/>) */
  private Map<String, String> extraRootElements = new LinkedHashMap<>();

  // State during generation
  private Map<Class<?>, String> complexTypeNames = new HashMap<>();
  private Map<String, String> complexTypeDefinitions = new LinkedHashMap<>();
  private Map<String, String> simpleTypeDefinitions = new LinkedHashMap<>();
  private Set<String> usedTypeNames = new HashSet<>();
  private Set<Class<?>> currentlyGeneratingTypes = new HashSet<>();
  private boolean booleanTypeUsed = false;

  public XmlMetadataSchema() {}

  public XmlMetadataSchema(Class<?> rootClass, String rootElementName) {
    this.rootClass = rootClass;
    this.rootElementName = rootElementName;
  }

  public void substituteType(Class<?> targetInterface, Class<?> concreteClass) {
    classSubstitutions.put(targetInterface, concreteClass);
  }

  public void setFixedFieldValue(String fieldTag, String fixedValue) {
    fixedFieldValues.put(fieldTag, fixedValue);
  }

  public void addExtraRootElement(String elementName, String typeName) {
    extraRootElements.put(elementName, typeName);
  }

  /**
   * Quick utility method to generate an XML Schema string for the given class and root element
   * name.
   */
  public static String generateSchema(Class<?> clazz, String rootElementName) {
    XmlMetadataSchema schema = new XmlMetadataSchema(clazz, rootElementName);
    return schema.generate();
  }

  /** Quick utility method to generate an XML Schema string for the given class. */
  public static String generateSchema(Class<?> clazz) {
    return generateSchema(clazz, null);
  }

  /** Generate the full XSD schema document as formatted XML string. */
  public String generate() {
    clearState();

    if (rootClass == null) {
      throw new IllegalArgumentException("Root class must not be null");
    }

    String rootElement = rootElementName;
    if (StringUtils.isEmpty(rootElement)) {
      HopMetadataWrapper wrapper = rootClass.getAnnotation(HopMetadataWrapper.class);
      if (wrapper != null && StringUtils.isNotEmpty(wrapper.tag())) {
        rootElement = wrapper.tag();
      } else {
        rootElement = rootClass.getSimpleName();
      }
    }

    // Register root complex type
    String rootTypeName = getOrCreateComplexTypeName(rootClass);
    generateComplexType(rootClass, rootTypeName);

    StringBuilder xsd = new StringBuilder();
    xsd.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>").append(Const.CR);
    xsd.append("<xs:schema xmlns:xs=\"")
        .append(XSD_NAMESPACE)
        .append("\" elementFormDefault=\"qualified\">")
        .append(Const.CR);

    // Simple types (e.g. HopBoolean, enums)
    if (booleanTypeUsed && includeHopBooleanType) {
      xsd.append("  <xs:simpleType name=\"")
          .append(HOP_BOOLEAN_TYPE)
          .append("\">")
          .append(Const.CR);
      xsd.append("    <xs:restriction base=\"xs:string\">").append(Const.CR);
      xsd.append("      <xs:pattern value=\"[YyNn]|true|false|TRUE|FALSE|1|0|\"/>")
          .append(Const.CR);
      xsd.append("    </xs:restriction>").append(Const.CR);
      xsd.append("  </xs:simpleType>").append(Const.CR);
    }

    for (String simpleTypeDef : simpleTypeDefinitions.values()) {
      xsd.append(simpleTypeDef).append(Const.CR);
    }

    // Root element declaration
    xsd.append("  <xs:element name=\"")
        .append(rootElement)
        .append("\" type=\"")
        .append(rootTypeName)
        .append("\"/>")
        .append(Const.CR);

    // Extra root elements
    for (Map.Entry<String, String> entry : extraRootElements.entrySet()) {
      xsd.append("  <xs:element name=\"")
          .append(entry.getKey())
          .append("\" type=\"")
          .append(entry.getValue())
          .append("\"/>")
          .append(Const.CR);
    }

    // Complex types
    for (String complexTypeDef : complexTypeDefinitions.values()) {
      xsd.append(complexTypeDef).append(Const.CR);
    }

    xsd.append("</xs:schema>").append(Const.CR);

    return xsd.toString();
  }

  private void clearState() {
    complexTypeNames.clear();
    complexTypeDefinitions.clear();
    simpleTypeDefinitions.clear();
    usedTypeNames.clear();
    currentlyGeneratingTypes.clear();
    booleanTypeUsed = false;
  }

  /** Determine or allocate a unique complex type name for a class. */
  public String getOrCreateComplexTypeName(Class<?> clazz) {
    if (complexTypeNames.containsKey(clazz)) {
      return complexTypeNames.get(clazz);
    }

    String baseName = clazz.getSimpleName();
    if (StringUtils.isEmpty(baseName)) {
      baseName = "AnonymousType";
    } else {
      baseName = baseName + "Type";
    }

    String typeName = baseName;
    int index = 2;
    while (usedTypeNames.contains(typeName)) {
      typeName = baseName + "_" + index;
      index++;
    }

    usedTypeNames.add(typeName);
    complexTypeNames.put(clazz, typeName);
    return typeName;
  }

  /** Generate complex type for a class if not already generated. */
  public void generateComplexType(Class<?> clazz, String typeName) {
    if (complexTypeDefinitions.containsKey(typeName) || currentlyGeneratingTypes.contains(clazz)) {
      return;
    }

    currentlyGeneratingTypes.add(clazz);

    StringBuilder typeXml = new StringBuilder();
    typeXml.append("  <xs:complexType name=\"").append(typeName).append("\">").append(Const.CR);

    if (hasUnsubstitutedInlinedInterface(clazz)) {
      typeXml.append("    <xs:sequence>").append(Const.CR);
      typeXml
          .append("      <xs:any minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"lax\"/>")
          .append(Const.CR);
      typeXml.append("    </xs:sequence>").append(Const.CR);
    } else {
      if (flexibleElementOrder) {
        typeXml.append("    <xs:choice minOccurs=\"0\" maxOccurs=\"unbounded\">").append(Const.CR);
      } else {
        typeXml.append("    <xs:sequence>").append(Const.CR);
      }

      generateSequenceElements(
          clazz, typeXml, "      ", Collections.emptySet(), Collections.emptySet());

      if (flexibleElementOrder) {
        typeXml.append("    </xs:choice>").append(Const.CR);
      } else {
        typeXml.append("    </xs:sequence>").append(Const.CR);
      }
    }
    typeXml.append("  </xs:complexType>");

    complexTypeDefinitions.put(typeName, typeXml.toString());
    currentlyGeneratingTypes.remove(clazz);
  }

  private boolean hasUnsubstitutedInlinedInterface(Class<?> clazz) {
    List<Field> fields =
        ReflectionUtil.findAllFields(clazz, new MetadataPropertyKeyFunction(), false);
    for (Field field : fields) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null && property.inline()) {
        Class<?> fieldType = resolveEffectiveClass(field.getType(), field.getGenericType());
        if (fieldType.isInterface() && fieldType.isAnnotationPresent(HopMetadataObject.class)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Inspects annotated fields and getters of clazz, generating <xs:element> items. */
  private void generateSequenceElements(
      Class<?> clazz,
      StringBuilder xml,
      String indent,
      Set<String> serializeOnly,
      Set<String> childKeysToIgnore) {

    List<Field> fields =
        ReflectionUtil.findAllFields(clazz, new MetadataPropertyKeyFunction(), false);

    for (Field field : fields) {
      if (!isFieldAppropriateForSerialization(field, serializeOnly, childKeysToIgnore)) {
        continue;
      }

      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property == null || childKeysToIgnore.contains(property.key())) {
        continue;
      }

      String tag = property.key();
      if (StringUtils.isEmpty(tag)) {
        tag = field.getName();
      }

      Class<?> fieldType = resolveEffectiveClass(field.getType(), field.getGenericType());

      if (property.inline()) {
        // Inlined object: properties spliced directly into parent sequence
        if (fieldType.isInterface() && fieldType.isAnnotationPresent(HopMetadataObject.class)) {
          // If no substitution provided for this interface, append lax any
          xml.append(indent)
              .append("<xs:any minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"lax\"/>")
              .append(Const.CR);
        } else {
          Set<String> nestedSerializeOnly = Set.of(property.serializeOnly());
          Set<String> nestedIgnore = Set.of(property.childKeysToIgnore());
          generateSequenceElements(fieldType, xml, indent, nestedSerializeOnly, nestedIgnore);
        }
      } else {
        generateElementForProperty(
            tag, property.groupKey(), fieldType, field.getGenericType(), property, xml, indent);
      }
    }

    // Also check annotated getters starting with "get"
    for (Method getter : ReflectionUtil.findAllMethods(clazz, "get")) {
      HopMetadataProperty methodProperty = getter.getAnnotation(HopMetadataProperty.class);
      if (methodProperty == null) {
        continue;
      }

      String tag = methodProperty.key();
      if (StringUtils.isEmpty(tag)) {
        tag = getFieldNameFromGetter(getter.getName());
      }

      Class<?> returnType =
          resolveEffectiveClass(getter.getReturnType(), getter.getGenericReturnType());

      if (methodProperty.inline()) {
        generateSequenceElements(
            returnType,
            xml,
            indent,
            Set.of(methodProperty.serializeOnly()),
            Set.of(methodProperty.childKeysToIgnore()));
      } else {
        generateElementForProperty(
            tag,
            methodProperty.groupKey(),
            returnType,
            getter.getGenericReturnType(),
            methodProperty,
            xml,
            indent);
      }
    }
  }

  private Class<?> resolveEffectiveClass(Class<?> rawClass, Type genericType) {
    if (classSubstitutions.containsKey(rawClass)) {
      return classSubstitutions.get(rawClass);
    }
    return rawClass;
  }

  private boolean isFieldAppropriateForSerialization(
      Field field, Set<String> serializeOnly, Set<String> childKeysToIgnore) {
    if (!serializeOnly.isEmpty() && !serializeOnly.contains(field.getName())) {
      return false;
    }
    if (Modifier.isTransient(field.getModifiers()) || Modifier.isVolatile(field.getModifiers())) {
      return false;
    }
    HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
    if (property != null && childKeysToIgnore.contains(property.key())) {
      return false;
    }
    return property != null && !property.isExcludedFromSerialization();
  }

  private void generateElementForProperty(
      String tag,
      String groupKey,
      Class<?> type,
      Type genericType,
      HopMetadataProperty property,
      StringBuilder xml,
      String indent) {

    // Check if this tag has a fixed value configured (e.g. type="TableInput")
    String fixedValue = fixedFieldValues.get(tag);

    // List scenario
    if (List.class.isAssignableFrom(type)) {
      generateListElement(tag, groupKey, genericType, property, xml, indent);
      return;
    }

    // Map scenario
    if (Map.class.isAssignableFrom(type)) {
      generateMapElement(tag, groupKey, genericType, property, xml, indent);
      return;
    }

    // Single value scenario: in Hop XML serialization, scalar fields are never wrapped in groupKey
    String xsdType = resolveXsdType(type, property);
    appendElementDeclaration(xml, indent, tag, xsdType, fixedValue, 0, 1);
  }

  private static String getFieldNameFromGetter(String methodName) {
    if (methodName.startsWith("get") && methodName.length() > 3) {
      return Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
    }
    return methodName;
  }

  private void appendElementDeclaration(
      StringBuilder xml,
      String indent,
      String tag,
      String xsdType,
      String fixedValue,
      int minOccurs,
      int maxOccurs) {

    xml.append(indent).append("<xs:element name=\"").append(tag).append("\"");

    if (fixedValue != null) {
      xml.append(" type=\"xs:string\" fixed=\"").append(Const.escapeXml(fixedValue)).append("\"");
    } else if (xsdType != null) {
      xml.append(" type=\"").append(xsdType).append("\"");
    }

    if (minOccurs != 1) {
      xml.append(" minOccurs=\"").append(minOccurs).append("\"");
    }
    if (maxOccurs == -1) {
      xml.append(" maxOccurs=\"unbounded\"");
    } else if (maxOccurs != 1) {
      xml.append(" maxOccurs=\"").append(maxOccurs).append("\"");
    }

    xml.append("/>").append(Const.CR);
  }

  private void generateListElement(
      String tag,
      String groupKey,
      Type genericType,
      HopMetadataProperty property,
      StringBuilder xml,
      String indent) {

    Class<?> itemClass = property.listItemClass();
    if (itemClass.equals(Object.class) && (genericType instanceof ParameterizedType pt)) {
      Type[] args = pt.getActualTypeArguments();
      if (args.length > 0) {
        Type arg = args[0];
        if (arg instanceof Class<?> clz) {
          itemClass = clz;
        } else if (arg instanceof WildcardType wt && wt.getUpperBounds().length > 0) {
          Type ub = wt.getUpperBounds()[0];
          if (ub instanceof Class<?> clz) {
            itemClass = clz;
          }
        }
      }
    }

    itemClass = resolveEffectiveClass(itemClass, genericType);
    String itemXsdType = resolveXsdType(itemClass, property);

    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(indent)
          .append("<xs:element name=\"")
          .append(groupKey)
          .append("\" minOccurs=\"0\">")
          .append(Const.CR);
      xml.append(indent).append("  <xs:complexType>").append(Const.CR);
      xml.append(indent).append("    <xs:sequence>").append(Const.CR);
      appendElementDeclaration(xml, indent + "      ", tag, itemXsdType, null, 0, -1);
      xml.append(indent).append("    </xs:sequence>").append(Const.CR);
      xml.append(indent).append("  </xs:complexType>").append(Const.CR);
      xml.append(indent).append("</xs:element>").append(Const.CR);
    } else {
      appendElementDeclaration(xml, indent, tag, itemXsdType, null, 0, -1);
    }
  }

  private void generateMapElement(
      String tag,
      String groupKey,
      Type genericType,
      HopMetadataProperty property,
      StringBuilder xml,
      String indent) {

    if (StringUtils.isNotEmpty(property.storeMapAsList())) {
      Class<?> valueClass = property.mapValueClass();
      if (valueClass.equals(Object.class) && (genericType instanceof ParameterizedType pt)) {
        Type[] args = pt.getActualTypeArguments();
        if (args.length > 1 && args[1] instanceof Class<?> clz) {
          valueClass = clz;
        }
      }
      valueClass = resolveEffectiveClass(valueClass, genericType);
      String valueXsdType = resolveXsdType(valueClass, property);

      if (StringUtils.isNotEmpty(groupKey)) {
        xml.append(indent)
            .append("<xs:element name=\"")
            .append(groupKey)
            .append("\" minOccurs=\"0\">")
            .append(Const.CR);
        xml.append(indent).append("  <xs:complexType>").append(Const.CR);
        xml.append(indent).append("    <xs:sequence>").append(Const.CR);
        appendElementDeclaration(xml, indent + "      ", tag, valueXsdType, null, 0, -1);
        xml.append(indent).append("    </xs:sequence>").append(Const.CR);
        xml.append(indent).append("  </xs:complexType>").append(Const.CR);
        xml.append(indent).append("</xs:element>").append(Const.CR);
      } else {
        appendElementDeclaration(xml, indent, tag, valueXsdType, null, 0, -1);
      }
      return;
    }

    // Generic Map representation (e.g. attributesMap)
    String effectiveGroup = StringUtils.isNotEmpty(groupKey) ? groupKey : tag;
    String itemTag = StringUtils.isNotEmpty(groupKey) ? tag : "entry";
    String keyWrapper =
        StringUtils.isNotEmpty(property.mapKeyWrapper()) ? property.mapKeyWrapper() : "key";
    String valueWrapper =
        StringUtils.isNotEmpty(property.mapValueWrapper()) ? property.mapValueWrapper() : "value";

    xml.append(indent)
        .append("<xs:element name=\"")
        .append(effectiveGroup)
        .append("\" minOccurs=\"0\">")
        .append(Const.CR);
    xml.append(indent).append("  <xs:complexType>").append(Const.CR);
    xml.append(indent).append("    <xs:sequence>").append(Const.CR);
    xml.append(indent)
        .append("      <xs:element name=\"")
        .append(itemTag)
        .append("\" minOccurs=\"0\" maxOccurs=\"unbounded\">")
        .append(Const.CR);
    xml.append(indent).append("        <xs:complexType>").append(Const.CR);
    xml.append(indent).append("          <xs:sequence>").append(Const.CR);
    xml.append(indent)
        .append("            <xs:element name=\"")
        .append(keyWrapper)
        .append("\" type=\"xs:string\" minOccurs=\"0\"/>")
        .append(Const.CR);
    xml.append(indent)
        .append("            <xs:element name=\"")
        .append(valueWrapper)
        .append("\" minOccurs=\"0\" maxOccurs=\"unbounded\">")
        .append(Const.CR);
    xml.append(indent).append("              <xs:complexType>").append(Const.CR);
    xml.append(indent).append("                <xs:sequence>").append(Const.CR);
    xml.append(indent)
        .append(
            "                  <xs:any minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"lax\"/>")
        .append(Const.CR);
    xml.append(indent).append("                </xs:sequence>").append(Const.CR);
    xml.append(indent).append("              </xs:complexType>").append(Const.CR);
    xml.append(indent).append("            </xs:element>").append(Const.CR);
    xml.append(indent).append("          </xs:sequence>").append(Const.CR);
    xml.append(indent).append("        </xs:complexType>").append(Const.CR);
    xml.append(indent).append("      </xs:element>").append(Const.CR);
    xml.append(indent).append("    </xs:sequence>").append(Const.CR);
    xml.append(indent).append("  </xs:complexType>").append(Const.CR);
    xml.append(indent).append("</xs:element>").append(Const.CR);
  }

  /** Maps a Java class to an XSD type, registering any necessary complex/simple types. */
  public String resolveXsdType(Class<?> clazz, HopMetadataProperty property) {
    if (clazz == null || Object.class.equals(clazz)) {
      return "xs:anyType";
    }

    if (String.class.equals(clazz)) {
      return "xs:string";
    }

    if (Boolean.class.equals(clazz) || boolean.class.equals(clazz)) {
      booleanTypeUsed = true;
      return HOP_BOOLEAN_TYPE;
    }

    if (Integer.class.equals(clazz)
        || int.class.equals(clazz)
        || Short.class.equals(clazz)
        || short.class.equals(clazz)) {
      if (property != null && !IIntCodeConverter.None.class.equals(property.intCodeConverter())) {
        return "xs:string";
      }
      return "xs:int";
    }

    if (Long.class.equals(clazz) || long.class.equals(clazz)) {
      return "xs:long";
    }

    if (Double.class.equals(clazz)
        || double.class.equals(clazz)
        || Float.class.equals(clazz)
        || float.class.equals(clazz)
        || BigDecimal.class.equals(clazz)) {
      return "xs:double";
    }

    if (Date.class.equals(clazz)) {
      return "xs:string";
    }

    if (property != null && (property.password() || property.storeWithName())) {
      return "xs:string";
    }

    if (clazz.isEnum()) {
      return getOrCreateEnumType(clazz, property != null && property.storeWithCode());
    }

    // POJO / Complex class
    String complexTypeName = getOrCreateComplexTypeName(clazz);
    generateComplexType(clazz, complexTypeName);
    return complexTypeName;
  }

  private String getOrCreateEnumType(Class<?> enumClass, boolean storeWithCode) {
    String typeName = enumClass.getSimpleName() + (storeWithCode ? "Code" : "") + "Enum";
    if (simpleTypeDefinitions.containsKey(typeName)) {
      return typeName;
    }

    StringBuilder xml = new StringBuilder();
    xml.append("  <xs:simpleType name=\"").append(typeName).append("\">").append(Const.CR);
    xml.append("    <xs:restriction base=\"xs:string\">").append(Const.CR);

    Object[] constants = enumClass.getEnumConstants();
    if (constants != null) {
      for (Object constant : constants) {
        String val;
        if (storeWithCode && constant instanceof IEnumHasCode hasCode) {
          val = hasCode.getCode();
        } else {
          val = ((Enum<?>) constant).name();
        }
        xml.append("      <xs:enumeration value=\"")
            .append(Const.escapeXml(val))
            .append("\"/>")
            .append(Const.CR);
      }
    }

    xml.append("    </xs:restriction>").append(Const.CR);
    xml.append("  </xs:simpleType>");

    simpleTypeDefinitions.put(typeName, xml.toString());
    return typeName;
  }
}
