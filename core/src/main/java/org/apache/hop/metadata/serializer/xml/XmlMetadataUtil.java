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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.EmptyStringEncoder;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHasName;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IIntCodeConverter;
import org.apache.hop.metadata.api.IStringEncoder;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.w3c.dom.Node;

public class XmlMetadataUtil {

  private static final String FQN_TRANSFORM_FACTORY =
      "org.apache.hop.pipeline.transform.ITransformMeta$TransformFactory";
  private static final String FQN_ACTION_FACTORY =
      "org.apache.hop.workflow.action.IAction$ActionFactory";

  private static final String BASE_TRANSFORM_META_CLASS =
      "org.apache.hop.pipeline.transform.BaseTransformMeta";

  /**
   * Prevents infinite recursion when {@code getXml()} on a transform delegates back to {@link
   * #serializeObjectToXml(Object)}.
   */
  private static final ThreadLocal<Integer> LEGACY_TRANSFORM_GET_XML_DEPTH =
      ThreadLocal.withInitial(() -> 0);

  private XmlMetadataUtil() {
    // Hides the public constructor
  }

  /**
   * {@link MissingResourceException} from transform/action factories: substitute {@code Missing} /
   * {@code MissingAction} via reflection (no core → engine compile dependency).
   */
  private static Object newMissingPluginPlaceholder(
      HopMetadataObject metadataObject, Node node, String pluginId) {
    String factoryName = metadataObject.objectFactory().getName();
    String name = node != null ? XmlHandler.getTagValue(node, "name") : null;
    try {
      if (FQN_TRANSFORM_FACTORY.equals(factoryName)) {
        Class<?> c = Class.forName("org.apache.hop.pipeline.transforms.missing.Missing");
        return c.getDeclaredConstructor(String.class, String.class)
            .newInstance(Const.NVL(name, ""), pluginId);
      }
      if (FQN_ACTION_FACTORY.equals(factoryName)) {
        Class<?> c = Class.forName("org.apache.hop.workflow.actions.missing.MissingAction");
        String actionName = (name == null || name.isEmpty()) ? null : name;
        return c.getDeclaredConstructor(String.class, String.class)
            .newInstance(actionName, pluginId);
      }
    } catch (Exception ignored) {
      return null;
    }
    return null;
  }

  /**
   * @return true if the class (or a superclass) declares at least one {@link HopMetadataProperty}
   *     on a field or on a {@code get*()} method, using the same rules as XML serialization.
   */
  public static boolean hasHopMetadataSerializableProperties(Class<?> clazz) {
    Set<String> serializeOnly = Set.of();
    Set<String> childKeysToIgnore = Set.of();
    List<Field> fields =
        ReflectionUtil.findAllFields(clazz, new MetadataPropertyKeyFunction(), false);
    for (Field field : fields) {
      if (getValidFieldAnnotation(field, serializeOnly, childKeysToIgnore) != null) {
        return true;
      }
    }
    for (Method getter : ReflectionUtil.findAllMethods(clazz, "get")) {
      if (getter.getAnnotation(HopMetadataProperty.class) != null) {
        return true;
      }
    }
    return false;
  }

  private static boolean isAssignableFromBaseTransformMeta(Class<?> clazz) {
    try {
      ClassLoader cl = clazz.getClassLoader();
      if (cl == null) {
        cl = XmlMetadataUtil.class.getClassLoader();
      }
      Class<?> base = Class.forName(BASE_TRANSFORM_META_CLASS, false, cl);
      return base.isAssignableFrom(clazz);
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /**
   * This method looks at the fields in the class of the provided parentObject. It then sees which
   * fields have annotation HopMetadataProperty and proceeds to serialize the values of those fields
   * as XML.
   *
   * @param parentObject The parentObject to serialize to XML
   * @return The XML representation of the given parentObject.
   * @throws HopException in case there was a problem during XML serialization
   */
  public static String serializeObjectToXml(Object parentObject) throws HopException {
    return serializeObjectToXml(parentObject, null);
  }

  /**
   * This method looks at the fields in the class of the provided parentObject. It then sees which
   * fields have annotation HopMetadataProperty and proceeds to serialize the values of those fields
   * as XML.
   *
   * @param parentObject The parentObject to serialize to XML
   * @param parentProperty The Hop metadata annotation of the parent object.
   * @return The XML representation of the given parentObject.
   * @throws HopException in case there was a problem during XML serialization
   */
  private static String serializeObjectToXml(
      Object parentObject, HopMetadataProperty parentProperty) throws HopException {
    Class<?> objectClass = parentObject.getClass();

    StringBuilder xml = new StringBuilder();

    HopMetadataWrapper wrapper = objectClass.getAnnotation(HopMetadataWrapper.class);
    if (wrapper != null) {
      xml.append(XmlHandler.openTag(wrapper.tag()));
    }

    Set<String> serializeOnly = new HashSet<>();
    if (parentProperty != null) {
      serializeOnly.addAll(Set.of(parentProperty.serializeOnly()));
    }
    Set<String> childKeysToIgnore = new HashSet<>();
    if (parentProperty != null) {
      childKeysToIgnore.addAll(Set.of(parentProperty.childKeysToIgnore()));
    }

    // Pick up all the fields with @HopMetadataProperty annotation, sorted by name.
    // Serialize them to XML.
    //
    List<Field> fields =
        ReflectionUtil.findAllFields(objectClass, new MetadataPropertyKeyFunction(), false);
    for (Field field : fields) {
      // Is this field appropriate to be considered for serialization?
      // We check it with the method below.
      //
      HopMetadataProperty property =
          getValidFieldAnnotation(field, serializeOnly, childKeysToIgnore);
      if (property != null && !childKeysToIgnore.contains(property.key())) {
        String groupKey = property.groupKey();
        String tag = property.key();
        if (StringUtils.isEmpty(tag)) {
          tag = field.getName();
        }

        Class<?> fieldType = field.getType();

        // Is this a boolean?
        //
        boolean isBoolean = Boolean.class.equals(fieldType) || boolean.class.equals(fieldType);

        // Add the field value to the XML
        //
        serializeFieldValueToXml(parentObject, field, isBoolean, property, xml, tag, groupKey);
      }
    }

    // Also find methods annotated, starting with "get"
    //
    for (Method getter : ReflectionUtil.findAllMethods(objectClass, "get")) {
      HopMetadataProperty methodProperty = getter.getAnnotation(HopMetadataProperty.class);
      if (methodProperty == null) {
        continue;
      }
      // What do we get from this method?
      //
      Object object;
      try {
        object = getter.invoke(parentObject);
      } catch (Exception e) {
        throw new HopException(
            "Error getting a value from annotated method " + getter.getName(), e);
      }
      xml.append(
          serializeObjectToXml(
              object, methodProperty, methodProperty.groupKey(), methodProperty.key()));
    }

    if (wrapper != null) {
      xml.append(XmlHandler.closeTag(wrapper.tag()));
    }

    String result = xml.toString();
    if (StringUtils.isNotBlank(result)) {
      return result;
    }
    if (!hasHopMetadataSerializableProperties(objectClass)
        && isAssignableFromBaseTransformMeta(objectClass)
        && LEGACY_TRANSFORM_GET_XML_DEPTH.get() == 0) {
      try {
        LEGACY_TRANSFORM_GET_XML_DEPTH.set(1);
        Method m = parentObject.getClass().getMethod("getXml");
        m.trySetAccessible();
        if (m.getParameterCount() == 0
            && String.class.equals(m.getReturnType())
            && !BASE_TRANSFORM_META_CLASS.equals(m.getDeclaringClass().getName())) {
          Object out = m.invoke(parentObject);
          if (out instanceof String legacy && StringUtils.isNotEmpty(legacy)) {
            return legacy;
          }
        }
      } catch (ReflectiveOperationException e) {
        throw new HopException("Unable to invoke legacy getXml() on " + objectClass.getName(), e);
      } finally {
        LEGACY_TRANSFORM_GET_XML_DEPTH.set(0);
      }
    }
    return result;
  }

  private static void serializeFieldValueToXml(
      Object parentObject,
      Field field,
      boolean isBoolean,
      HopMetadataProperty property,
      StringBuilder xml,
      String tag,
      String groupKey)
      throws HopException {
    // Get the value of the field...
    //
    Object value = ReflectionUtil.getFieldValue(parentObject, field.getName(), isBoolean);
    if (value != null) {
      // We only serialize non-null values to save space and performance.
      //
      if (property.storeWithName()) {
        if (value instanceof IHasName hasName) {
          xml.append(XmlHandler.addTagValue(tag, hasName.getName()));
        } else {
          throw new HopException(
              "If you want to store an object with its name, make sure the class implements IHasName");
        }
      } else {
        xml.append(serializeObjectToXml(value, property, groupKey, tag));
      }
    }
  }

  private static Class<? extends IIntCodeConverter> getIntCodeConverter(
      HopMetadataProperty property) {
    if (property == null) {
      return IIntCodeConverter.None.class;
    }
    return property.intCodeConverter();
  }

  private static Class<? extends IStringEncoder> getStringEncoderClass(
      HopMetadataProperty property) {
    if (property == null) {
      return EmptyStringEncoder.class;
    }
    return property.stringEncoder();
  }

  private static String serializeObjectToXml(
      Object parentObject, HopMetadataProperty parentProperty, String groupKey, String tag)
      throws HopException {

    boolean password = parentProperty != null && parentProperty.password();
    boolean storeWithCode = parentProperty != null && parentProperty.storeWithCode();
    Class<? extends IIntCodeConverter> intCodeConverterClass = getIntCodeConverter(parentProperty);
    Class<? extends IStringEncoder> stringEncoderClass = getStringEncoderClass(parentProperty);

    StringBuilder xml = new StringBuilder();

    if (parentObject == null) {
      return XmlHandler.addTagValue(tag, (String) null);
    }
    if (parentObject instanceof String string) {
      serializeStringToXml(tag, string, password, xml, stringEncoderClass);
    } else if (parentObject instanceof Boolean bool) {
      xml.append(XmlHandler.addTagValue(tag, bool));
    } else if (parentObject instanceof Integer integer) {
      serializeIntToXml((int) parentObject, tag, integer, intCodeConverterClass, xml);
    } else if (parentObject instanceof Long longValue) {
      xml.append(XmlHandler.addTagValue(tag, longValue));
    } else if (parentObject instanceof Double doubleValue) {
      xml.append(XmlHandler.addTagValue(tag, doubleValue));
    } else if (parentObject instanceof Date date) {
      xml.append(XmlHandler.addTagValue(tag, date));
    } else if (parentObject.getClass().isEnum()) {
      serializeEnumToXMl(parentObject, tag, storeWithCode, xml);
    } else if (parentObject instanceof java.util.List<?> listItems) {
      serializeListToXml(parentProperty, groupKey, tag, listItems, xml);
    } else if (parentObject instanceof java.util.Map<?, ?> map) {
      if (parentProperty == null || StringUtils.isEmpty(parentProperty.storeMapAsList())) {
        serializeMapToXml(
            groupKey,
            tag,
            parentProperty == null ? "" : parentProperty.mapKeyWrapper(),
            parentProperty == null ? "" : parentProperty.mapValueWrapper(),
            map,
            xml);
      } else {
        serializeMapAsListToXml(
            groupKey,
            tag,
            parentProperty.storeMapAsList(),
            parentProperty.mapKeyWrapper(),
            parentProperty.mapValueWrapper(),
            map,
            xml);
      }
    } else {
      serializePojoToXml(parentObject, parentProperty, tag, xml);
    }
    return xml.toString();
  }

  private static void serializeMapAsListToXml(
      String groupKey, String tag, String s, String s1, String s2, Map<?, ?> map, StringBuilder xml)
      throws HopException {
    openTag(groupKey, xml);
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object value = entry.getValue();
      openTag(tag, xml);
      xml.append(serializeObjectToXml(value, null, "", ""));
      closeTag(tag, xml);
    }
    closeTag(groupKey, xml);
  }

  private static void openTag(String groupKey, StringBuilder xml) {
    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(XmlHandler.openTag(groupKey));
    }
  }

  private static void closeTag(String groupKey, StringBuilder xml) {
    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(XmlHandler.closeTag(groupKey));
    }
  }

  private static void serializePojoToXml(
      Object parentObject, HopMetadataProperty parentProperty, String tag, StringBuilder xml)
      throws HopException {
    // POJO : serialize to XML...
    // We only take the fields of the POJO class that are annotated
    // We wrap the POJO properties in the provided tag
    //
    if (parentProperty != null && !parentProperty.inline()) {
      xml.append(XmlHandler.openTag(tag));
    }
    xml.append(serializeObjectToXml(parentObject, parentProperty));
    if (parentProperty != null && !parentProperty.inline()) {
      xml.append(XmlHandler.closeTag(tag));
    }
  }

  private static void serializeMapToXml(
      String groupKey, String tag, String keyTag, String valueTag, Map<?, ?> map, StringBuilder xml)
      throws HopException {
    // For Map we loop over all the keys and get the values.
    // We serialize this way:
    // <groupKey>
    //   <key> key-and-value-elements </key>
    //   <key> key-and-value-elements </key>
    //   ...
    // </groupKey>

    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(XmlHandler.openTag(groupKey));
    }
    Set<? extends Map.Entry<?, ?>> entrySet = map.entrySet();
    for (Map.Entry<?, ?> entry : entrySet) {
      if (StringUtils.isNotEmpty(tag)) {
        xml.append(XmlHandler.openTag(tag));
      }

      serializeMapKeyToXml(xml, keyTag, entry);
      serializeMapValueToXml(xml, valueTag, entry);

      if (StringUtils.isNotEmpty(tag)) {
        xml.append(XmlHandler.closeTag(tag));
      }
    }
    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(XmlHandler.closeTag(groupKey));
    }
  }

  private static void serializeMapKeyToXml(StringBuilder xml, String keyTag, Map.Entry<?, ?> entry)
      throws HopException {
    Object keyObject = entry.getKey();
    Class<?> keyClass = keyObject.getClass();
    HopMetadataProperty keyProperty = keyClass.getAnnotation(HopMetadataProperty.class);
    String keyTagKey = Const.NVL(keyTag, "key");
    String keyTagGroup = "";
    if (keyProperty != null) {
      keyTagKey = Const.NVL(keyProperty.key(), keyTagKey);
    }
    xml.append(serializeObjectToXml(keyObject, keyProperty, keyTagGroup, keyTagKey));
  }

  private static void serializeMapValueToXml(
      StringBuilder xml, String valueTag, Map.Entry<?, ?> entry) throws HopException {
    Object valueObject = entry.getValue();
    Class<?> valueClass = valueObject.getClass();
    HopMetadataProperty keyProperty = valueClass.getAnnotation(HopMetadataProperty.class);
    String valueTagKey = Const.NVL(valueTag, "value");
    String valueTagGroup = "";
    if (keyProperty != null) {
      valueTagKey = Const.NVL(keyProperty.key(), valueTagKey);
    }
    if (valueObject instanceof Map<?, ?> map) {
      // This is typically Map<String, Map<String, String>>
      // We don't have any more values to give names to the underlying map so let's pick key/value
      //
      serializeMapToXml("", valueTagKey, "key", "value", map, xml);
    } else {
      xml.append(serializeObjectToXml(valueObject, keyProperty, valueTagGroup, valueTagKey));
    }
  }

  private static void serializeListToXml(
      HopMetadataProperty parentProperty,
      String groupKey,
      String tag,
      List<?> listItems,
      StringBuilder xml)
      throws HopException {
    // Serialize a list of values
    // Use the key on the annotation to open a new block
    // Store the items in that block
    //
    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(XmlHandler.openTag(groupKey));
    }

    // Add the elements...
    //
    for (Object listItem : listItems) {
      xml.append(serializeObjectToXml(listItem, parentProperty, groupKey, tag));
    }

    if (StringUtils.isNotEmpty(groupKey)) {
      xml.append(XmlHandler.closeTag(groupKey));
    }
  }

  private static void serializeEnumToXMl(
      Object parentObject, String tag, boolean storeWithCode, StringBuilder xml) {
    if (storeWithCode) {
      xml.append(XmlHandler.addTagValue(tag, ((IEnumHasCode) parentObject).getCode()));
    } else {
      xml.append(XmlHandler.addTagValue(tag, ((Enum) parentObject).name()));
    }
  }

  private static void serializeIntToXml(
      int parentObject,
      String tag,
      Integer integer,
      Class<? extends IIntCodeConverter> intCodeConverterClass,
      StringBuilder xml)
      throws HopException {
    if (intCodeConverterClass.equals(IIntCodeConverter.None.class)) {
      xml.append(XmlHandler.addTagValue(tag, integer));
    } else {
      try {
        IIntCodeConverter converter = intCodeConverterClass.getConstructor().newInstance();
        xml.append(XmlHandler.addTagValue(tag, converter.getCode(parentObject)));
      } catch (Exception e) {
        throw new HopException(
            "Error converting int to String code using converter class " + intCodeConverterClass,
            e);
      }
    }
  }

  private static void serializeStringToXml(
      String tag,
      String string,
      boolean password,
      StringBuilder xml,
      Class<? extends IStringEncoder> stringEncoderClass)
      throws HopException {
    // Hang on, is this a password?
    //
    if (password) {
      xml.append(XmlHandler.addTagValue(tag, Encr.encryptPasswordIfNotUsingVariables(string)));
    } else if (!stringEncoderClass.equals(EmptyStringEncoder.class)) {
      // We need to encode the string
      try {
        IStringEncoder encoder = stringEncoderClass.getConstructor().newInstance();
        xml.append(XmlHandler.addTagValue(tag, encoder.encode(string)));
      } catch (Exception e) {
        throw new HopException(
            "Error encoding string with class " + stringEncoderClass.getName(), e);
      }
    } else {
      xml.append(XmlHandler.addTagValue(tag, string));
    }
  }

  /**
   * Load the metadata in the provided XML node and return it as a new object. It does this by
   * looking at the HopMetadataProperty annotations of the fields in the object's class.
   *
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param metadataProvider to load name references from
   * @throws HopXmlException In case there was an error inflating the XML
   */
  public static <T> T deSerializeFromXml(
      Node node, Class<? extends T> clazz, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(null, null, node, clazz, null, metadataProvider);
  }

  /**
   * Load the metadata in the provided XML node and return it as a new object. It does this by
   * looking at the HopMetadataProperty annotations of the fields in the object's class.
   *
   * @param parentObject An optional parent object to allow a factory to load extra information
   *     from.
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param metadataProvider to load name references from
   * @throws HopXmlException In case there was an error inflating the XML
   */
  public static <T> T deSerializeFromXml(
      Object parentObject,
      Node node,
      Class<? extends T> clazz,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(parentObject, null, node, clazz, null, metadataProvider);
  }

  /**
   * Load the metadata in the provided XML node and return it as a new object. It does this by
   * looking at the HopMetadataProperty annotations of the fields in the object's class.
   *
   * @param parentObject An optional parent object to allow a factory to load extra information
   *     from.
   * @param parentProperty The hop metadata property of the parent object (or null)
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param metadataProvider to load name references from
   * @throws HopXmlException In case there was an error inflating the XML
   */
  public static <T> T deSerializeFromXml(
      Object parentObject,
      HopMetadataProperty parentProperty,
      Node node,
      Class<? extends T> clazz,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(parentObject, parentProperty, node, clazz, null, metadataProvider);
  }

  /**
   * Load the metadata in the provided XML node into the given parent object. It does this by
   * looking at the HopMetadataProperty annotations of the fields in the parent object's class.
   *
   * @param parentObject The parent object to load into. If null: create a new parent object.
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param metadataProvider to load name references from
   * @throws HopXmlException In case there was an error inflating the XML
   */
  public static <T> T deSerializeFromXml(
      Node node, Class<? extends T> clazz, T parentObject, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(null, null, node, clazz, parentObject, metadataProvider);
  }

  /**
   * Load the metadata in the provided XML node into the given object. It does this by looking at
   * the HopMetadataProperty annotations of the fields in the object's class.
   *
   * @param parentObject An optional parent object to allow factories to retrieve extra information
   *     (or null).
   * @param parentProperty The parent property (or null)
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param object The object to load into. If null: create a new object.
   * @param metadataProvider to load name references from
   * @throws HopXmlException In case there was an error inflating the XML
   */
  public static <T> T deSerializeFromXml(
      Object parentObject,
      HopMetadataProperty parentProperty,
      Node node,
      Class<? extends T> clazz,
      T object,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    // Do not create a new object if the node is null
    //
    if (node == null) {
      return null;
    }

    Set<String> serializeOnly = new HashSet<>();
    if (parentProperty != null) {
      serializeOnly.addAll(Set.of(parentProperty.serializeOnly()));
    }
    Set<String> childKeysToIgnore = new HashSet<>();
    if (parentProperty != null) {
      childKeysToIgnore.addAll(Set.of(parentProperty.childKeysToIgnore()));
    }

    if (object == null) {
      object = createNewObject(parentObject, node, clazz);

      // In case the object is marked as optional we can stop here
      if (object == null) {
        return null;
      }
    }

    // Warning: in case we call a factory, the interface class turns into an object class.
    //
    HopMetadataWrapper wrapper = object.getClass().getAnnotation(HopMetadataWrapper.class);
    if (wrapper != null) {
      node = XmlHandler.getSubNode(node, wrapper.tag());
    }

    // Pick up all the @HopMetadataProperty annotations.
    // The fields are sorted by name to get a stable XML output when serialized.
    //
    List<Field> fields =
        ReflectionUtil.findAllFields(object.getClass(), new MetadataPropertyKeyFunction(), false);
    for (Field field : fields) {
      // Is this field appropriate to be considered for serialization?
      // We check it with the method below.
      //
      HopMetadataProperty property =
          getValidFieldAnnotation(field, serializeOnly, childKeysToIgnore);
      if (property != null) {
        Class<?> fieldType = field.getType();
        Class<?> genericClass = property.listItemClass();
        if (genericClass.equals(Object.class)
            && (field.getGenericType() instanceof ParameterizedType parameterizedType)) {
          if (parameterizedType.getActualTypeArguments().length > 0) {
            genericClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];
          }
        }

        // Get the tag & tagNode
        String tag = property.key();
        if (StringUtils.isEmpty(tag)) {
          tag = field.getName();
        }
        Node tagNode;
        if (property.inline()) {
          tagNode = node;
        } else {
          tagNode = XmlHandler.getSubNode(node, tag);
        }

        // Get the groupKey & groupNode
        String groupKey = property.groupKey();
        Node groupNode;
        if (StringUtils.isEmpty(groupKey)) {
          groupNode = node;
        } else {
          groupNode = XmlHandler.getSubNode(node, groupKey);
        }
        Object value;
        if (property.storeWithName() && StringUtils.isNotEmpty(property.lookupInList())) {
          value = deSerializeXmlUsingNamedList(parentObject, node, tag, property);
        } else {
          value =
              deSerializeFromXml(
                  object,
                  property,
                  fieldType,
                  groupNode,
                  tagNode,
                  tag,
                  genericClass,
                  metadataProvider);
        }
        try {
          // Only set a value if we have something to set.
          // Empty strings and such will still go through but not null values for int/long/...
          //
          if (value != null) {
            ReflectionUtil.setFieldValue(object, field.getName(), fieldType, value);
          }
        } catch (HopException e) {
          throw new HopXmlException(
              "Unable to set value "
                  + value
                  + " on field "
                  + field.getName()
                  + " in class "
                  + fieldType.getName(),
              e);
        }
      }
    }

    // Also find methods annotated, starting with "get"
    //
    try {
      for (Method setter : ReflectionUtil.findAllMethods(clazz, "set")) {
        HopMetadataProperty property = setter.getAnnotation(HopMetadataProperty.class);
        if (property == null) {
          continue;
        }

        // Get the tag & tagNode
        String tag = property.key();
        Node tagNode;
        if (property.inline()) {
          tagNode = node;
        } else {
          tagNode = XmlHandler.getSubNode(node, tag);
        }

        // Get the groupKey & groupNode
        String groupKey = property.groupKey();
        Node groupNode;
        if (StringUtils.isEmpty(groupKey)) {
          groupNode = node;
        } else {
          groupNode = XmlHandler.getSubNode(node, groupKey);
        }

        // Find the single argument of the setter
        //
        Class<?>[] parameterTypes = setter.getParameterTypes();
        if (parameterTypes.length != 1) {
          throw new HopException(
              "Annotated setter method "
                  + setter.getName()
                  + " in class "
                  + clazz.getName()
                  + " needs to have exactly one argument, the de-serialized object.");
        }
        Class<?> parameterClass = parameterTypes[0];
        Object parameterObject = createNewObject(parameterClass, node, parameterClass);
        Class<?> listItemClass = property.listItemClass();

        parameterObject =
            deSerializeFromXml(
                object,
                property,
                parameterClass,
                groupNode,
                tagNode,
                tag,
                listItemClass,
                metadataProvider);

        // Pass along the inflated data
        //
        setter.invoke(object, parameterObject);
      }

    } catch (Exception e) {
      throw new HopXmlException("Error calling setter method on class " + clazz.getName(), e);
    }

    if (object instanceof ILegacyXml legacyXml) {
      try {
        legacyXml.convertLegacyXml(node, metadataProvider);
      } catch (HopException e) {
        throw new HopXmlException("Error de-serializing legacy XML", e);
      }
    }

    return object;
  }

  private static Object deSerializeXmlUsingNamedList(
      Object parentObject, Node node, String tag, HopMetadataProperty property)
      throws HopXmlException {
    Object value;
    // What is the name of the object to reference?
    //
    String name = XmlHandler.getTagValue(node, tag);
    if (StringUtils.isEmpty(name)) {
      return null;
    }

    // We need the list to look up the name with.
    //
    String listName = property.lookupInList();
    Class<?> objectClass = parentObject.getClass();
    Field listField;
    try {
      listField = objectClass.getDeclaredField(listName);
    } catch (NoSuchFieldException e) {
      throw new HopXmlException(
          "List name " + listName + " was not found in class " + objectClass, e);
    }

    Method getter;
    try {
      getter = ReflectionUtil.findGetter(objectClass, listField);
    } catch (Exception e) {
      throw new HopXmlException("Error finding getter method for field " + listField, e);
    }
    List<IHasName> lookupList;
    try {
      lookupList = (List<IHasName>) getter.invoke(parentObject);
    } catch (Exception e) {
      throw new HopXmlException("Error getting lookup list with method " + getter.getName(), e);
    }
    // Now we can look up the value
    //
    value = null;
    for (IHasName lookup : lookupList) {
      if (name.equalsIgnoreCase(lookup.getName())) {
        value = lookup;
        break;
      }
    }
    if (value == null) {
      throw new HopXmlException("Unable to find object with name " + name + " in list " + listName);
    }
    return value;
  }

  private static HopMetadataProperty getValidFieldAnnotation(
      Field field, Set<String> serializeOnly, Set<String> childKeysToIgnore) {
    if (!serializeOnly.isEmpty() && !serializeOnly.contains(field.getName())) {
      // This is not a field we want to consider
      return null;
    }

    // Don't serialize fields flagged as transient or volatile
    //
    if (Modifier.isTransient(field.getModifiers()) || Modifier.isVolatile(field.getModifiers())) {
      return null;
    }
    HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
    if (property != null && !childKeysToIgnore.contains(property.key())) {
      return property;
    }
    return null;
  }

  private static <T> T createNewObject(Object parentObject, Node node, Class<? extends T> clazz)
      throws HopXmlException {
    T object;
    try {
      // See if this is an interface where we need to use a factory.
      //
      HopMetadataObject metadataObject = clazz.getAnnotation(HopMetadataObject.class);
      if (metadataObject != null) {
        String xmlKey = metadataObject.xmlKey();
        if (StringUtils.isEmpty(xmlKey)) {
          throw new HopXmlException(
              "Please specify which XML attribute to consider the key.  Hop will use this key to create the appropriate class instance of type "
                  + clazz);
        }
        String objectId = XmlHandler.getNodeValue(XmlHandler.getSubNode(node, xmlKey));
        if (StringUtils.isEmpty(objectId)) {
          if (metadataObject.optionalValue()) {
            object = null;
          } else {
            throw new HopXmlException(
                "XML attribute "
                    + xmlKey
                    + " is needed to instantiate type "
                    + clazz
                    + " but it wasn't provided");
          }
        } else {
          try {
            IHopMetadataObjectFactory factory =
                metadataObject.objectFactory().getConstructor().newInstance();
            object = (T) factory.createObject(objectId, parentObject);
          } catch (MissingResourceException e) {
            Object placeholder = newMissingPluginPlaceholder(metadataObject, node, objectId);
            if (placeholder == null) {
              throw new HopXmlException(
                  "Unable to create a new instance of class "
                      + clazz.getName()
                      + " while de-serializing XML: "
                      + e.getMessage(),
                  e);
            }
            object = (T) placeholder;
          } catch (HopMissingPluginsException e) {
            throw new HopXmlException(
                "The plugin for class "
                    + clazz.getName()
                    + " with object ID "
                    + objectId
                    + " is missing",
                e);
          }
        }
      } else {
        if (clazz.equals(List.class)) {
          object = (T) new ArrayList<>();
        } else {
          object = clazz.getDeclaredConstructor().newInstance();
        }
      }
    } catch (Exception e) {
      throw new HopXmlException(
          "Unable to create a new instance of class "
              + clazz.getName()
              + " while de-serializing XML: make sure you have a public empty constructor for this class.",
          e);
    }
    return object;
  }

  private static Object deSerializeFromXml(
      Object parentObject,
      HopMetadataProperty parentProperty,
      Class<?> fieldType,
      Node groupNode,
      Node elementNode,
      String tag,
      Class<?> itemClass,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    String elementString = XmlHandler.getNodeValue(elementNode);

    boolean defaultBoolean = parentProperty != null && parentProperty.defaultBoolean();
    boolean storeWithName = parentProperty != null && parentProperty.storeWithName();
    boolean storeWithCode = parentProperty != null && parentProperty.storeWithCode();
    String[] inlineListTags =
        parentProperty != null ? parentProperty.inlineListTags() : new String[0];
    Class<? extends IIntCodeConverter> intCodeConverterClass = getIntCodeConverter(parentProperty);

    if (storeWithName) {
      // No name: return null
      //
      if (StringUtils.isEmpty(elementString)) {
        return null;
      }
      try {
        // Load the specified field type from the metadata provider.
        return metadataProvider.getSerializer((Class<IHopMetadata>) fieldType).load(elementString);
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to load reference by name '"
                + elementString
                + "' for type "
                + fieldType.getName()
                + ". Does this class implement IHopMetadata?",
            e);
      }
    }

    // Convert to other data type?
    //
    if (fieldType.equals(String.class)) {
      return deSerializeString(
          elementString, parentProperty, elementNode, fieldType, metadataProvider);
    } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
      return deSerializeInteger(elementNode, intCodeConverterClass, elementString);
    } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
      if (elementNode != null) {
        return Long.valueOf(elementString);
      }
    } else if (fieldType.equals(double.class) || fieldType.equals(Double.class)) {
      if (elementNode != null) {
        return Double.valueOf(elementString);
      }
    } else if (fieldType.equals(Date.class)) {
      if (elementNode != null) {
        return XmlHandler.stringToDate(elementString);
      }
    } else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
      if (elementNode != null) {
        return Const.toBoolean(elementString);
      } else {
        return defaultBoolean;
      }
    } else if (fieldType.isEnum()) {
      return deSerializeEnum(fieldType, parentProperty, elementString, storeWithCode);
    } else if (fieldType.equals(java.util.List.class)) {
      // What is the generic type, the list item class?
      //
      return deSerializeList(
          parentObject,
          parentProperty,
          groupNode,
          tag,
          itemClass,
          metadataProvider,
          inlineListTags);
    } else if (fieldType.equals(java.util.Map.class)) {
      Class<?> mapKeyClass = parentProperty != null ? parentProperty.mapKeyClass() : String.class;
      Class<?> mapValueClass =
          parentProperty != null ? parentProperty.mapValueClass() : String.class;
      String keyWrapper = parentProperty != null ? parentProperty.mapKeyWrapper() : "key";
      String valueWrapper = parentProperty != null ? parentProperty.mapValueWrapper() : "value";

      if (parentProperty == null || StringUtils.isEmpty(parentProperty.storeMapAsList())) {
        return deSerializeMap(
            groupNode, tag, keyWrapper, valueWrapper, metadataProvider, mapKeyClass, mapValueClass);
      } else {
        return deSerializeMapFromList(
            groupNode,
            tag,
            parentProperty.storeMapAsList(),
            metadataProvider,
            mapKeyClass,
            mapValueClass);
      }
    } else {
      // Load the metadata for this node...
      //
      return deSerializeFromXml(
          parentObject, parentProperty, elementNode, fieldType, metadataProvider);
    }

    // No value found for the given arguments: return the default value
    //
    return null;
  }

  private static Object deSerializeString(
      String elementString,
      HopMetadataProperty parentProperty,
      Node elementNode,
      Class<?> fieldType,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    if (elementNode != null) {
      // The element is present in the XML, so the user intended a value — even if it's empty.
      // <tag/> and <tag></tag> both have no text children, which makes XmlHandler.getNodeValue
      // return null; collapse that to "" so the field assignment below isn't skipped by the
      // null-guard in the caller (which would otherwise leave the constructor default in place).
      String value = elementString != null ? elementString : "";
      if (parentProperty.password()) {
        return Encr.decryptPasswordOptionallyEncrypted(value);
      }
      if (!EmptyStringEncoder.class.equals(parentProperty.stringEncoder())) {
        // Decode the encoded string
        //
        try {
          IStringEncoder encoder = parentProperty.stringEncoder().getConstructor().newInstance();
          return encoder.decode(value);
        } catch (Exception e) {
          throw new HopXmlException(
              "Error decoding string '"
                  + value
                  + "' with string encoder class "
                  + parentProperty.stringEncoder().getName(),
              e);
        }
      } else {
        return value;
      }
    }
    return null;
  }

  private static Integer deSerializeInteger(
      Node elementNode,
      Class<? extends IIntCodeConverter> intCodeConverterClass,
      String elementString)
      throws HopXmlException {
    if (elementNode == null) {
      return null;
    }
    if (intCodeConverterClass.equals(IIntCodeConverter.None.class)) {
      return Integer.valueOf(elementString);
    } else {
      try {
        IIntCodeConverter converter = intCodeConverterClass.getConstructor().newInstance();
        return converter.getType(elementString);
      } catch (Exception e) {
        throw new HopXmlException(
            "Error converting String code "
                + elementString
                + " to integer using converter class "
                + intCodeConverterClass,
            e);
      }
    }
  }

  private static Object deSerializeEnum(
      Class<?> fieldClass,
      HopMetadataProperty fieldAnnotation,
      String elementString,
      boolean storeWithCode)
      throws HopXmlException {
    final Class<? extends Enum> enumerationClass = (Class<? extends Enum>) fieldClass;
    if (StringUtils.isNotEmpty(elementString)) {
      if (storeWithCode) {
        try {
          IEnumHasCode[] values = (IEnumHasCode[]) enumerationClass.getEnumConstants();
          for (IEnumHasCode value : values) {
            if (value.getCode().equals(elementString)) {
              return value;
            }
          }
          // Retry with the description for compatibility with older versions
          //
          if (enumerationClass.isInstance(IEnumHasCodeAndDescription.class)) {
            IEnumHasCodeAndDescription[] vals =
                (IEnumHasCodeAndDescription[]) enumerationClass.getEnumConstants();
            for (IEnumHasCodeAndDescription value : vals) {
              if (value.getDescription().equals(elementString)) {
                return value;
              }
            }
          }
        } catch (Exception e) {
          throw new HopXmlException(
              "Unable to get values() of enumeration to look up code value " + elementString, e);
        }
      } else {
        try {
          return Enum.valueOf(enumerationClass, elementString);
        } catch (IllegalArgumentException e) {
          String nameNotFound = fieldAnnotation.enumNameWhenNotFound();
          if (StringUtils.isEmpty(nameNotFound)) {
            throw e;
          } else {
            return Enum.valueOf(enumerationClass, nameNotFound);
          }
        }
      }
    }
    return null;
  }

  private static List<Object> deSerializeList(
      Object parentObject,
      HopMetadataProperty parentProperty,
      Node groupNode,
      String tag,
      Class<?> itemClass,
      IHopMetadataProvider metadataProvider,
      String[] inlineListTags)
      throws HopXmlException {
    // So if we have a List<Field> as per the unit test example
    // We'll now have a String with a bunch of <field></field> groups
    //
    List<Object> list = new ArrayList<>();
    List<Node> itemNodes = XmlHandler.getNodes(groupNode, tag);
    if (inlineListTags.length > 0 && !itemNodes.isEmpty()) {
      // Old XML serialization format where everything is just dumped into the same tag.
      // See also HopMetadataProperty.inlineListTags
      //
      Node parentNode = itemNodes.get(0);
      int listSize = XmlHandler.countNodes(parentNode, inlineListTags[0]);
      if (listSize > 1) {
        itemNodes.clear();
        for (int i = 0; i < listSize; i++) {
          Node node = parentNode.getOwnerDocument().createElement(tag);
          for (String inlineTag : inlineListTags) {
            Node n = XmlHandler.getSubNodeByNr(parentNode, inlineTag, i);
            if (n != null) {
              node.appendChild(n);
            }
          }
          itemNodes.add(node);
        }
      }
    }
    for (Node itemNode : itemNodes) {
      // We assume that the constructor of the parent class created the List object
      // so that we can simply add items to the list here.
      //
      try {
        Object newItem =
            deSerializeFromXml(
                parentObject,
                parentProperty,
                itemClass,
                null,
                itemNode,
                null,
                itemClass,
                metadataProvider);

        // Add it to the list
        //
        list.add(newItem);
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to instantiate a new instance of class "
                + itemClass.getName()
                + ": make sure there is an empty public constructor available to allow XML de-serialization",
            e);
      }
    }

    // We now have the list...
    //
    return list;
  }

  private static Map<Object, Object> deSerializeMap(
      Node groupNode,
      String tag,
      String keyWrapper,
      String valueWrapper,
      IHopMetadataProvider metadataProvider,
      Class<?> keyClass,
      Class<?> valueClass)
      throws HopXmlException {
    // For the map we're looking at the following pattern:
    //
    // <groupKey>
    //   <key> key-and-value-elements </key>
    //   <key> key-and-value-elements </key>
    //   ...
    // </groupKey>
    //
    Map<Object, Object> map = new HashMap<>();
    List<Node> itemNodes = XmlHandler.getNodes(groupNode, tag);
    for (Node itemNode : itemNodes) {
      // This item node contains all items of both the key and value classes.
      // So we can feed this node into both
      //
      Object key = deSerializeMapKey(metadataProvider, keyClass, itemNode, keyWrapper);
      Object value = deSerializeMapValue(metadataProvider, valueClass, itemNode, valueWrapper);

      // Now we can simply add key and value to the map
      //
      map.put(key, value);
    }
    return map;
  }

  private static Map<Object, Object> deSerializeMapFromList(
      Node groupNode,
      String tag,
      String keyFieldName,
      IHopMetadataProvider metadataProvider,
      Class<?> keyClass,
      Class<?> valueClass)
      throws HopXmlException {
    // For the map we're looking at the following pattern:
    //
    // <groupKey>
    //   <key> key-and-value-elements </key>
    //   <key> key-and-value-elements </key>
    //   ...
    // </groupKey>
    //
    Map<Object, Object> map = new HashMap<>();
    List<Node> itemNodes = XmlHandler.getNodes(groupNode, tag);
    for (Node itemNode : itemNodes) {
      // This item node contains only value items.
      // So we can feed this node into both
      //
      Object value = deSerializeFromXml(itemNode, valueClass, metadataProvider);

      // Find field k
      Field keyField;
      try {
        keyField = valueClass.getDeclaredField(keyFieldName);
      } catch (NoSuchFieldException e) {
        throw new HopXmlException(
            "Specified field '" + keyFieldName + "' with storeMapAsList() was not found");
      }
      Method getter;
      try {
        getter = ReflectionUtil.findGetter(valueClass, keyField);
      } catch (NoSuchMethodException e) {
        throw new HopXmlException(
            "Getter method for field "
                + keyFieldName
                + " was not found in class "
                + valueClass.getName());
      }
      Object key;
      try {
        key = getter.invoke(value);
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to invoke getter for field "
                + keyFieldName
                + " in class "
                + valueClass.getName(),
            e);
      }

      // Now we can add key and value to the map
      //
      map.put(key, value);
    }
    return map;
  }

  private static Object deSerializeMapValue(
      IHopMetadataProvider metadataProvider,
      Class<?> valueClass,
      Node itemNode,
      String valueWrapper)
      throws HopXmlException {
    Object value;

    // This is the Map<?, Map<?,?>> scenario
    //
    if (valueClass.equals(HashMap.class)) {
      // What are the Map values?
      //
      Map<Object, Object> childMap = new HashMap<>();
      List<Node> valueNodes = XmlHandler.getNodes(itemNode, valueWrapper);
      for (Node valueNode : valueNodes) {
        Object childKey = deSerializeMapKey(metadataProvider, String.class, valueNode, "key");
        Object childValue = deSerializeMapKey(metadataProvider, String.class, valueNode, "value");
        childMap.put(childKey, childValue);
      }
      return childMap;
    }

    try {
      value = deSerializeFromXml(itemNode, valueClass, metadataProvider);
    } catch (Exception e) {
      throw new HopXmlException(
          "Unable to instantiate a new instance of map value class "
              + valueClass.getName()
              + ": make sure there is an empty public constructor available to allow XML de-serialization",
          e);
    }
    return value;
  }

  private static Object deSerializeMapKey(
      IHopMetadataProvider metadataProvider, Class<?> keyClass, Node itemNode, String keyWrapper)
      throws HopXmlException {
    Object key;

    Node keyNode = XmlHandler.getSubNode(itemNode, keyWrapper);
    if (keyNode != null && keyClass.equals(String.class)) {
      return XmlHandler.getNodeValue(keyNode);
    }
    if (keyNode == null) {
      keyNode = itemNode;
    }
    try {
      // Instantiate the key object
      key = deSerializeFromXml(keyNode, keyClass, metadataProvider);
    } catch (Exception e) {
      throw new HopXmlException(
          "Unable to instantiate a new instance of map key class "
              + keyClass.getName()
              + ": make sure there is an empty public constructor available to allow XML de-serialization",
          e);
    }
    return key;
  }
}
