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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.w3c.dom.Node;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class XmlMetadataUtil {
  /**
   * This method looks at the fields in the class of the provided object. It then sees which fields
   * have annotation HopMetadataProperty and proceeds to serialize the values of those fields as
   * XML.
   *
   * @param object The object to serialize to XML
   * @return The XML representation of the given object.
   * @throws HopException
   */
  public static String serializeObjectToXml(Object object) throws HopException {
    String xml = "";

    // Pick up all the @HopMetadataProperty annotations
    // Serialize them to XML
    //
    List<Field> fields = new ArrayList(ReflectionUtil.findAllFields(object.getClass()));

    // Sort the fields by name to get stable XML as output
    //
    Collections.sort(fields, Comparator.comparing(Field::getName));

    for (Field field : fields) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null) {
        String groupKey = property.groupKey();
        String tag = property.key();
        if (StringUtils.isEmpty(tag)) {
          tag = field.getName();
        }

        Class<?> fieldType = field.getType();

        // Is this a boolean?
        //
        boolean isBoolean = Boolean.class.equals(fieldType) || boolean.class.equals(fieldType);

        // A password?
        //
        boolean isPassword = property.password();

        // Store enums with their code?
        //
        boolean storeWithCode = property.storeWithCode();

        // Get the value of the field...
        //
        Object value = ReflectionUtil.getFieldValue(object, field.getName(), isBoolean);
        if (value != null) {
          // We only serialize non-null values to save space and performance.
          //
          if (property.storeWithName()) {
            xml += XmlHandler.addTagValue(tag, ((IHopMetadata) value).getName());
          } else {
            xml += serializeObjectToXml(value, groupKey, tag, isPassword, storeWithCode);
          }
        }
      }
    }

    return xml;
  }

  private static String serializeObjectToXml(
      Object value, String groupKey, String tag, boolean password, boolean storeWithCode)
      throws HopException {

    String xml = "";

    if (value == null) {
      xml += XmlHandler.addTagValue(tag, (String) null);
    } else {
      if (value instanceof String) {
        // Hang on, is this a password?
        //
        if (password) {
          xml += XmlHandler.addTagValue(tag, Encr.encryptPassword((String) value));
        } else {
          xml += XmlHandler.addTagValue(tag, (String) value);
        }
      } else if (value instanceof Boolean) {
        xml += XmlHandler.addTagValue(tag, (Boolean) value);
      } else if (value instanceof Integer) {
        xml += XmlHandler.addTagValue(tag, (Integer) value);
      } else if (value instanceof Long) {
        xml += XmlHandler.addTagValue(tag, (Long) value);
      } else if (value instanceof Date) {
        xml += XmlHandler.addTagValue(tag, (Date) value);
      } else if (value.getClass().isEnum()) {
        if (storeWithCode) {
          xml += XmlHandler.addTagValue(tag, ((IEnumHasCode) value).getCode());
        } else {
          xml += XmlHandler.addTagValue(tag, ((Enum) value).name());
        }
      } else if (value instanceof java.util.List) {

        // Serialize a list of values
        // Use the key on the annotation to open a new block
        // Store the items in that block
        //
        if (StringUtils.isNotEmpty(groupKey)) {
          xml += XmlHandler.openTag(groupKey) + Const.CR;
        }

        // Add the elements...
        //
        List listItems = (List) value;
        for (Object listItem : listItems) {
          xml += serializeObjectToXml(listItem, groupKey, tag, password, storeWithCode);
        }

        if (StringUtils.isNotEmpty(groupKey)) {
          xml += XmlHandler.closeTag(groupKey) + Const.CR;
        }

      } else {

        // POJO : serialize to XML...
        // We only take the fields of the POJO class that are annotated
        // We wrap the POJO properties in the provided tag
        //
        xml += XmlHandler.openTag(tag) + Const.CR;
        xml += serializeObjectToXml(value);
        xml += XmlHandler.closeTag(tag) + Const.CR;
      }
    }
    return xml;
  }

  /**
   * Load the metadata in the provided XML node and return it as a new object. It does this by
   * looking at the HopMetadataProperty annotations of the fields in the object's class.
   *
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param metadataProvider to load name references from
   * @throws HopXmlException
   */
  public static <T> T deSerializeFromXml(
      Node node, Class<? extends T> clazz, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(node, clazz, null, metadataProvider);
  }
  /**
   * Load the metadata in the provided XML node into the given object. It does this by looking at
   * the HopMetadataProperty annotations of the fields in the object's class.
   *
   * @param node The metadata to read
   * @param clazz the class to de-serialize
   * @param object The object to load into. If null: create a new object.
   * @param metadataProvider to load name references from
   * @throws HopXmlException
   */
  public static <T> T deSerializeFromXml(
      Node node, Class<? extends T> clazz, T object, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    if (object == null) {
      try {
        object = clazz.newInstance();
      } catch (Exception e) {
        throw new HopXmlException(
            "Unable to create a new instance of class "
                + clazz.getName()
                + " while de-serializing XML: make sure you have a public empty constructor for this class.",
            e);
      }
    }

    // Pick up all the @HopMetadataProperty annotations
    // Serialize them to XML
    //
    Set<Field> fields = ReflectionUtil.findAllFields(clazz);
    for (Field field : fields) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null) {
        String tag = property.key();
        String groupKey = property.groupKey();
        if (StringUtils.isEmpty(tag)) {
          tag = field.getName();
        }
        Class<?> fieldType = field.getType();
        boolean defaultBoolean = property.defaultBoolean();
        boolean storeWithName = property.storeWithName();
        boolean password = property.password();
        boolean storeWithCode = property.storeWithCode();

        Node tagNode = XmlHandler.getSubNode(node, tag);
        Node groupNode;
        if (StringUtils.isEmpty(groupKey)) {
          groupNode = node;
        } else {
          groupNode = XmlHandler.getSubNode(node, groupKey);
        }
        Object value =
            deSerializeFromXml(
                fieldType,
                groupNode,
                tagNode,
                tag,
                field,
                defaultBoolean,
                storeWithName,
                metadataProvider,
                password,
                storeWithCode);

        try {
          ReflectionUtil.setFieldValue(object, field.getName(), fieldType, value);
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
    return object;
  }

  private static Object deSerializeFromXml(
      Class<?> fieldType,
      Node groupNode,
      Node elementNode,
      String tag,
      Field field,
      boolean defaultBoolean,
      boolean storeWithName,
      IHopMetadataProvider metadataProvider,
      boolean password,
      boolean storeWithCode)
      throws HopXmlException {

    String elementString = XmlHandler.getNodeValue(elementNode);

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
      if (elementNode != null) {
        if (password) {
          return Encr.decryptPasswordOptionallyEncrypted(elementString);
        } else {
          return elementString;
        }
      }
    } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
      if (elementNode != null) {
        return Integer.valueOf(elementString);
      }
    } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
      if (elementNode != null) {
        return Long.valueOf(elementString);
      }
    } else if (fieldType.equals(Date.class)) {
      if (elementNode != null) {
        return XmlHandler.stringToDate(elementString);
      }
    } else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
      if (elementNode != null) {
        return "y".equalsIgnoreCase(elementString) || "true".equalsIgnoreCase(elementString);
      } else {
        return defaultBoolean;
      }
    } else if (fieldType.isEnum()) {
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
          } catch (Exception e) {
            throw new HopXmlException(
                "Unable to get values() of enumeration to look up code value " + elementString, e);
          }
        } else {
          return Enum.valueOf(enumerationClass, elementString);
        }
      }
    } else if (fieldType.equals(java.util.List.class)) {
      // So if we have a List<Field> as per the unit test example
      // We'll now have a String with a bunch of <field></field> groups
      //
      List<Object> list = new ArrayList<>();
      List<Node> itemNodes = XmlHandler.getNodes(groupNode, tag);
      for (Node itemNode : itemNodes) {
        // We assume that the constructor of the parent class created the List object
        // so that we can simply add items to the list here.
        //
        ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
        Class<?> listClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];
        try {
          Object newItem =
              deSerializeFromXml(
                  listClass,
                  null,
                  itemNode,
                  null,
                  null,
                  false,
                  false,
                  metadataProvider,
                  password,
                  storeWithCode);

          // Add it to the list
          //
          list.add(newItem);
        } catch (Exception e) {
          throw new HopXmlException(
              "Unable to instantiate a new instance of class "
                  + listClass.getName()
                  + ": make sure there is an empty public constructor available to allow XML de-serialization");
        }
      }

      // We now have the list...
      //
      return list;
    } else {
      // Load the metadata for this node...
      //
      return deSerializeFromXml(elementNode, fieldType, metadataProvider);
    }

    // No value found for the given arguments: return the default value
    //
    return null;
  }
}
