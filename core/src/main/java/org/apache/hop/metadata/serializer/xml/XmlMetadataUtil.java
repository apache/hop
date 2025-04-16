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
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IIntCodeConverter;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.w3c.dom.Node;

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
    Class<?> objectClass = object.getClass();

    StringBuilder xml = new StringBuilder();

    HopMetadataWrapper wrapper = objectClass.getAnnotation(HopMetadataWrapper.class);
    if (wrapper != null) {
      xml.append(XmlHandler.openTag(wrapper.tag()));
    }

    // Pick up all the fields with @HopMetadataProperty annotation, sorted by name.
    // Serialize them to XML.
    //
    List<Field> fields =
        ReflectionUtil.findAllFields(objectClass, new MetadataPropertyKeyFunction());
    for (Field field : fields) {
      // Don't serialize fields flagged as transient or volatile
      //
      if (Modifier.isTransient(field.getModifiers()) || Modifier.isVolatile(field.getModifiers())) {
        continue;
      }
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
            xml.append(XmlHandler.addTagValue(tag, ((IHopMetadata) value).getName()));
          } else {
            xml.append(
                serializeObjectToXml(
                    property,
                    value,
                    groupKey,
                    tag,
                    isPassword,
                    storeWithCode,
                    property.intCodeConverter()));
          }
        }
      }
    }

    if (wrapper != null) {
      xml.append(XmlHandler.closeTag(wrapper.tag()));
    }

    return xml.toString();
  }

  private static String serializeObjectToXml(
      HopMetadataProperty property,
      Object value,
      String groupKey,
      String tag,
      boolean password,
      boolean storeWithCode,
      Class<? extends IIntCodeConverter> intCodeConverterClass)
      throws HopException {

    StringBuilder xml = new StringBuilder();

    if (value == null) {
      xml.append(XmlHandler.addTagValue(tag, (String) null));
    } else {
      if (value instanceof String string) {
        // Hang on, is this a password?
        //
        if (password) {
          xml.append(XmlHandler.addTagValue(tag, Encr.encryptPasswordIfNotUsingVariables(string)));
        } else {
          xml.append(XmlHandler.addTagValue(tag, string));
        }
      } else if (value instanceof Boolean bool) {
        xml.append(XmlHandler.addTagValue(tag, bool));
      } else if (value instanceof Integer integer) {
        if (intCodeConverterClass.equals(IIntCodeConverter.None.class)) {
          xml.append(XmlHandler.addTagValue(tag, integer));
        } else {
          try {
            IIntCodeConverter converter = intCodeConverterClass.getConstructor().newInstance();
            xml.append(XmlHandler.addTagValue(tag, converter.getCode((int) value)));
          } catch (Exception e) {
            throw new HopException(
                "Error converting int to String code using converter class "
                    + intCodeConverterClass,
                e);
          }
        }
      } else if (value instanceof Long longValue) {
        xml.append(XmlHandler.addTagValue(tag, longValue));
      } else if (value instanceof Double doubleValue) {
        xml.append(XmlHandler.addTagValue(tag, doubleValue));
      } else if (value instanceof Date date) {
        xml.append(XmlHandler.addTagValue(tag, date));
      } else if (value.getClass().isEnum()) {
        if (storeWithCode) {
          xml.append(XmlHandler.addTagValue(tag, ((IEnumHasCode) value).getCode()));
        } else {
          xml.append(XmlHandler.addTagValue(tag, ((Enum) value).name()));
        }
      } else if (value instanceof java.util.List list) {

        // Serialize a list of values
        // Use the key on the annotation to open a new block
        // Store the items in that block
        //
        if (StringUtils.isNotEmpty(groupKey)) {
          xml.append(XmlHandler.openTag(groupKey)).append(Const.CR);
        }

        // Add the elements...
        //
        List listItems = list;
        for (Object listItem : listItems) {
          xml.append(
              serializeObjectToXml(
                  property,
                  listItem,
                  groupKey,
                  tag,
                  password,
                  storeWithCode,
                  property.intCodeConverter()));
        }

        if (StringUtils.isNotEmpty(groupKey)) {
          xml.append(XmlHandler.closeTag(groupKey)).append(Const.CR);
        }

      } else {

        // POJO : serialize to XML...
        // We only take the fields of the POJO class that are annotated
        // We wrap the POJO properties in the provided tag
        //
        if (!property.inline()) {
          xml.append(XmlHandler.openTag(tag)).append(Const.CR);
        }
        xml.append(serializeObjectToXml(value));
        if (!property.inline()) {
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
   * @throws HopXmlException
   */
  public static <T> T deSerializeFromXml(
      Node node, Class<? extends T> clazz, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(null, node, clazz, null, metadataProvider);
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
   * @throws HopXmlException
   */
  public static <T> T deSerializeFromXml(
      Object parentObject,
      Node node,
      Class<? extends T> clazz,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    return deSerializeFromXml(parentObject, node, clazz, null, metadataProvider);
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
    return deSerializeFromXml(null, node, clazz, object, metadataProvider);
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
   * @throws HopXmlException
   */
  public static <T> T deSerializeFromXml(
      Object parentObject,
      Node node,
      Class<? extends T> clazz,
      T object,
      IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    if (object == null) {
      try {
        // Do not create a new object if the node is null
        //
        if (node == null) {
          return null;
        }

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
      // Don't serialize fields flagged as transient or volatile
      //
      if (Modifier.isTransient(field.getModifiers()) || Modifier.isVolatile(field.getModifiers())) {
        continue;
      }

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
                object,
                fieldType,
                groupNode,
                tagNode,
                tag,
                field,
                defaultBoolean,
                storeWithName,
                metadataProvider,
                password,
                storeWithCode,
                property.intCodeConverter(),
                inlineListTags);

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
    return object;
  }

  private static Object deSerializeFromXml(
      Object parentObject,
      Class<?> fieldType,
      Node groupNode,
      Node elementNode,
      String tag,
      Field field,
      boolean defaultBoolean,
      boolean storeWithName,
      IHopMetadataProvider metadataProvider,
      boolean password,
      boolean storeWithCode,
      Class<? extends IIntCodeConverter> intCodeConverterClass,
      String[] inlineListTags)
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
    } else if (fieldType.equals(java.util.List.class)) {
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
                  listClass,
                  null,
                  itemNode,
                  null,
                  null,
                  false,
                  false,
                  metadataProvider,
                  password,
                  storeWithCode,
                  intCodeConverterClass,
                  inlineListTags);

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
    } else {
      // Load the metadata for this node...
      //
      return deSerializeFromXml(parentObject, elementNode, fieldType, metadataProvider);
    }

    // No value found for the given arguments: return the default value
    //
    return null;
  }
}
