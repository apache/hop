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
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.EmptyStringEncoder;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IIntCodeConverter;
import org.apache.hop.metadata.api.IStringEncoder;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.w3c.dom.Node;

public class XmlMetadataUtil {
  private XmlMetadataUtil() {
    // Hides the public constructor
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
        ReflectionUtil.findAllFields(objectClass, new MetadataPropertyKeyFunction());
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

    if (wrapper != null) {
      xml.append(XmlHandler.closeTag(wrapper.tag()));
    }

    return xml.toString();
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
        xml.append(XmlHandler.addTagValue(tag, ((IHopMetadata) value).getName()));
      } else {
        xml.append(serializeObjectToXml(value, property, groupKey, tag));
      }
    }
  }

  private static String serializeObjectToXml(
      Object parentObject, HopMetadataProperty parentProperty, String groupKey, String tag)
      throws HopException {
    boolean password = parentProperty.password();
    boolean storeWithCode = parentProperty.storeWithCode();
    Class<? extends IIntCodeConverter> intCodeConverterClass = parentProperty.intCodeConverter();
    Class<? extends IStringEncoder> stringEncoderClass = parentProperty.stringEncoder();

    StringBuilder xml = new StringBuilder();

    if (parentObject == null) {
      xml.append(XmlHandler.addTagValue(tag, (String) null));
    } else {
      if (parentObject instanceof String string) {
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
      } else if (parentObject instanceof Boolean bool) {
        xml.append(XmlHandler.addTagValue(tag, bool));
      } else if (parentObject instanceof Integer integer) {
        if (intCodeConverterClass.equals(IIntCodeConverter.None.class)) {
          xml.append(XmlHandler.addTagValue(tag, integer));
        } else {
          try {
            IIntCodeConverter converter = intCodeConverterClass.getConstructor().newInstance();
            xml.append(XmlHandler.addTagValue(tag, converter.getCode((int) parentObject)));
          } catch (Exception e) {
            throw new HopException(
                "Error converting int to String code using converter class "
                    + intCodeConverterClass,
                e);
          }
        }
      } else if (parentObject instanceof Long longValue) {
        xml.append(XmlHandler.addTagValue(tag, longValue));
      } else if (parentObject instanceof Double doubleValue) {
        xml.append(XmlHandler.addTagValue(tag, doubleValue));
      } else if (parentObject instanceof Date date) {
        xml.append(XmlHandler.addTagValue(tag, date));
      } else if (parentObject.getClass().isEnum()) {
        if (storeWithCode) {
          xml.append(XmlHandler.addTagValue(tag, ((IEnumHasCode) parentObject).getCode()));
        } else {
          xml.append(XmlHandler.addTagValue(tag, ((Enum) parentObject).name()));
        }
      } else if (parentObject instanceof java.util.List listItems) {
        // Serialize a list of values
        // Use the key on the annotation to open a new block
        // Store the items in that block
        //
        if (StringUtils.isNotEmpty(groupKey)) {
          xml.append(XmlHandler.openTag(groupKey)).append(Const.CR);
        }

        // Add the elements...
        //
        for (Object listItem : listItems) {
          xml.append(serializeObjectToXml(listItem, parentProperty, groupKey, tag));
        }

        if (StringUtils.isNotEmpty(groupKey)) {
          xml.append(XmlHandler.closeTag(groupKey)).append(Const.CR);
        }
      } else //noinspection rawtypes
      if (parentObject instanceof java.util.Map map) {
        // For Map we loop over all the keys and get the values.
        // We serialize this way:
        // <groupKey>
        //   <key> key-and-value-elements </key>
        //   <key> key-and-value-elements </key>
        //   ...
        // </groupKey>

        xml.append(XmlHandler.openTag(parentProperty.groupKey())).append(Const.CR);
        Set<Map.Entry<Object, Object>> entrySet = map.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
          xml.append(XmlHandler.openTag(parentProperty.key())).append(Const.CR);
          Object keyObject = entry.getKey();
          Object valueObject = entry.getValue();
          xml.append(serializeObjectToXml(keyObject, parentProperty, groupKey, tag));
          xml.append(serializeObjectToXml(valueObject, parentProperty, groupKey, tag));
          xml.append(XmlHandler.closeTag(parentProperty.key())).append(Const.CR);
        }
        xml.append(XmlHandler.closeTag(parentProperty.groupKey())).append(Const.CR);
      } else {
        // POJO : serialize to XML...
        // We only take the fields of the POJO class that are annotated
        // We wrap the POJO properties in the provided tag
        //
        if (!parentProperty.inline()) {
          xml.append(XmlHandler.openTag(tag)).append(Const.CR);
        }
        xml.append(serializeObjectToXml(parentObject, parentProperty));
        if (!parentProperty.inline()) {
          xml.append(XmlHandler.closeTag(tag)).append(Const.CR);
        }
      }
    }
    return xml.toString();
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
   * @param parentObject An optional parent object to allow factories to retrieve extra information.
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param object The object to load into. If null: create a new object.
   * @param metadataProvider to load name references from
   * @throws HopXmlException In case there was an error inflating the XML
   */
  private static <T> T deSerializeFromXml(
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
    }

    HopMetadataWrapper wrapper = clazz.getAnnotation(HopMetadataWrapper.class);
    if (wrapper != null) {
      node = XmlHandler.getSubNode(node, wrapper.tag());
    }

    // Pick up all the @HopMetadataProperty annotations.
    // The fields are sorted by name to get a stable XML output when serialized.
    //
    List<Field> fields =
        ReflectionUtil.findAllFields(object.getClass(), new MetadataPropertyKeyFunction());
    for (Field field : fields) {
      // Is this field appropriate to be considered for serialization?
      // We check it with the method below.
      //
      HopMetadataProperty property =
          getValidFieldAnnotation(field, serializeOnly, childKeysToIgnore);
      if (property != null) {
        String tag = property.key();
        String groupKey = property.groupKey();
        if (StringUtils.isEmpty(tag)) {
          tag = field.getName();
        }
        Class<?> fieldType = field.getType();
        boolean storeWithCode = property.storeWithCode();
        String[] inlineListTags = property.inlineListTags();

        Node tagNode;
        if (property.inline()) {
          tagNode = node;
        } else {
          tagNode = XmlHandler.getSubNode(node, tag);
        }
        Node groupNode;
        if (StringUtils.isEmpty(groupKey)) {
          groupNode = node;
        } else {
          groupNode = XmlHandler.getSubNode(node, groupKey);
        }
        Object value =
            deSerializeFromXml(
                object, property, fieldType, groupNode, tagNode, tag, field, metadataProvider);

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

    if (object instanceof ILegacyXml legacyXml) {
      try {
        legacyXml.convertLegacyXml(node);
      } catch (HopException e) {
        throw new HopXmlException("Error de-serializing legacy XML", e);
      }
    }

    return object;
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
          throw new HopXmlException(
              "XML attribute "
                  + xmlKey
                  + " is needed to instantiate type "
                  + clazz
                  + " but it wasn't provided");
        }
        IHopMetadataObjectFactory factory =
            metadataObject.objectFactory().getConstructor().newInstance();
        object = (T) factory.createObject(objectId, parentObject);
      } else {
        object = clazz.getDeclaredConstructor().newInstance();
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
      Field field,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    String elementString = XmlHandler.getNodeValue(elementNode);

    boolean defaultBoolean = parentProperty.defaultBoolean();
    boolean storeWithName = parentProperty.storeWithName();
    boolean storeWithCode = parentProperty.storeWithCode();
    String[] inlineListTags = parentProperty.inlineListTags();
    Class<? extends IIntCodeConverter> intCodeConverterClass = parentProperty.intCodeConverter();

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
      Object value = deSerializeEnum(field, elementString, storeWithCode);
      if (value != null) return value;
    } else if (fieldType.equals(java.util.List.class)) {
      return deSerializeList(
          parentObject, parentProperty, groupNode, tag, field, metadataProvider, inlineListTags);
    } else if (fieldType.equals(java.util.Map.class)) {
      return deSerializeMap(parentObject, parentProperty, groupNode, tag, metadataProvider);
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
      if (parentProperty.password()) {
        return Encr.decryptPasswordOptionallyEncrypted(elementString);
      }
      if (!EmptyStringEncoder.class.equals(parentProperty.stringEncoder())) {
        // Decode the encoded string
        //
        try {
          IStringEncoder encoder = parentProperty.stringEncoder().getConstructor().newInstance();
          return encoder.decode(elementString);
        } catch (Exception e) {
          throw new HopXmlException(
              "Error decoding string '"
                  + elementString
                  + "' with string encoder class "
                  + parentProperty.stringEncoder().getName(),
              e);
        }
      } else {
        return elementString;
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

  private static Object deSerializeEnum(Field field, String elementString, boolean storeWithCode)
      throws HopXmlException {
    final Class<? extends Enum> enumerationClass = (Class<? extends Enum>) field.getType();
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
          String nameNotFound =
              field.getAnnotation(HopMetadataProperty.class).enumNameWhenNotFound();
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
      Field field,
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
      ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
      Class<?> listClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];
      try {
        Object newItem =
            deSerializeFromXml(
                parentObject,
                parentProperty,
                listClass,
                null,
                itemNode,
                null,
                null,
                metadataProvider);

        // Add it to the list
        //
        list.add(newItem);
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to instantiate a new instance of class "
                + listClass.getName()
                + ": make sure there is an empty public constructor available to allow XML de-serialization",
            e);
      }
    }

    // We now have the list...
    //
    return list;
  }

  private static Map<Object, Object> deSerializeMap(
      Object parentObject,
      HopMetadataProperty parentProperty,
      Node groupNode,
      String tag,
      IHopMetadataProvider metadataProvider)
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
      Class<?> keyClass = parentProperty.mapKeyClass();
      Class<?> valueClass = parentProperty.mapValueClass();
      Object key;
      Object value;
      try {
        // Instantiate the key object
        key = deSerializeFromXml(itemNode, keyClass, metadataProvider);
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to instantiate a new instance of map key class "
                + keyClass.getName()
                + ": make sure there is an empty public constructor available to allow XML de-serialization",
            e);
      }
      try {
        value = deSerializeFromXml(itemNode, valueClass, metadataProvider);
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to instantiate a new instance of map value class "
                + keyClass.getName()
                + ": make sure there is an empty public constructor available to allow XML de-serialization",
            e);
      }
      // Now we can simply add key and value to the map
      //
      map.put(key, value);
    }
    return map;
  }
}
