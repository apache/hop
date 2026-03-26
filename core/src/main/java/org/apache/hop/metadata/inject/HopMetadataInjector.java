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

package org.apache.hop.metadata.inject;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.injection.InjectionTypeConverter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.api.IIntCodeConverter;
import org.apache.hop.metadata.api.IStringObjectConverter;
import org.apache.hop.metadata.serializer.xml.MetadataPropertyKeyFunction;
import org.apache.hop.metadata.util.ReflectionUtil;

public class HopMetadataInjector {
  private HopMetadataInjector() {
    // Hidden constructor
  }

  /**
   * For the given object we retrieve the mappings between injection groups and their keys.
   *
   * @param objectClass The class to search
   * @return The mapping giving you the set of keys for each injection group.
   */
  public static Map<String, Set<String>> findInjectionGroupKeys(Class<?> objectClass)
      throws HopException {
    if (objectClass == null) {
      throw new HopException("The class to find injection group key mappings for is null");
    }
    Map<String, Set<String>> map = new HashMap<>();
    findInjectionGroupKeys(objectClass, map);
    return map;
  }

  /**
   * For the given object we retrieve the mappings between injection keys and the group to which
   * they belong.
   *
   * @param objectClass The class to search
   * @param map the map to populate
   */
  public static void findInjectionGroupKeys(Class<?> objectClass, Map<String, Set<String>> map)
      throws HopException {
    try {
      List<Field> fields =
          ReflectionUtil.findAllFields(objectClass, new MetadataPropertyKeyFunction());
      for (Field field : fields) {
        Class<?> fieldClass = field.getType();
        HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
        if (property != null && !property.isExcludedFromInjection()) {
          // Does this have an injection group key?
          //
          String injectionGroup = Const.NVL(property.injectionGroupKey(), property.groupKey());
          if (StringUtils.isNotEmpty(injectionGroup)) {
            if (fieldClass.equals(List.class)) {
              // Search in the generic type of the List
              ParameterizedType type = (ParameterizedType) field.getGenericType();
              Class<?> listItemClass = (Class<?>) type.getActualTypeArguments()[0];
              if (listItemClass.equals(String.class)) {
                // We simply map the single injection key with the group key
                //
                map.put(injectionGroup, new HashSet<>(Set.of(property.injectionKey())));
              } else {
                findInjectionGroupKeys(listItemClass, injectionGroup, map);
              }
            }
          } else if (StringUtils.isEmpty(property.injectionGroupKey())) {
            // No injection key or group: we want to look further down.
            // If the class is no primitive, we look into it.
            //
            findInjectionGroupKeys(fieldClass, map);
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error injecting source data into object", e);
    }
  }

  /**
   * For the given class, find the metadata properties with injection keys below it.
   *
   * @param fieldClass The class to search for injection keys
   * @param injectionGroup The parent injection group key
   * @param map The map to add the keys to
   */
  private static void findInjectionGroupKeys(
      Class<?> fieldClass, String injectionGroup, Map<String, Set<String>> map) {
    List<Field> fields =
        ReflectionUtil.findAllFields(fieldClass, new MetadataPropertyKeyFunction());
    for (Field field : fields) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property != null) {
        String injectionKey =
            Const.coalesce(property.injectionKey(), property.key(), field.getName());
        if (StringUtils.isNotEmpty(injectionKey)) {
          // Add to the set for this injection group
          Set<String> keySet = map.computeIfAbsent(injectionGroup, k -> new HashSet<>());
          keySet.add(injectionKey);
        } else {
          // Perhaps this is a subclass and the properties are located deeper.
          //
          Class<?> childClass = field.getType();
          findInjectionGroupKeys(childClass, injectionGroup, map);
        }
      }
    }
  }

  /**
   * Inject data into an object using HopMetadataProperty annotations on the fields in the class.
   *
   * @param metadataProvider The metadata provider to resolve injected object names.
   * @param object The object to inject data into.
   * @param injectionKeyMap A map between the injection key and the data where it should end up.
   * @param groupKeyMap a mapping between the group key and the data for the underlying list items.
   *     The rows contain values where the names correspond to the injection keys.
   * @throws HopException In case there was an error injecting data.
   */
  public static void inject(
      IHopMetadataProvider metadataProvider,
      Object object,
      Map<String, Object> injectionKeyMap,
      Map<String, RowBuffer> groupKeyMap)
      throws HopException {
    // We find all the with HopMetadataProperty annotated fields in the parent object.
    // For these fields we see if we can find injection instructions.
    // If this is the case, we inject the metadata.
    //
    if (object == null) {
      return;
    }
    Class<?> objectClass = object.getClass();
    try {
      List<Field> fields =
          ReflectionUtil.findAllFields(objectClass, new MetadataPropertyKeyFunction());
      for (Field field : fields) {
        injectField(metadataProvider, object, injectionKeyMap, groupKeyMap, field, objectClass);
      }
    } catch (Exception e) {
      throw new HopException("Error injecting source data into object", e);
    }
  }

  private static void injectField(
      IHopMetadataProvider metadataProvider,
      Object object,
      Map<String, Object> injectionKeyMap,
      Map<String, RowBuffer> groupKeyMap,
      Field field,
      Class<?> objectClass)
      throws Exception {
    HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
    if (property != null && !property.isExcludedFromInjection()) {
      Class<?> fieldClass = field.getType();

      // Sometimes the injection key is identical to the key.
      // This is some legacy or misunderstood functionality.
      // The key is the same as the field if none is given.
      String injectionKey =
          Const.coalesce(property.injectionKey(), property.key(), field.getName());
      String groupKey = Const.coalesce(property.injectionGroupKey(), property.groupKey());

      boolean hasKey = StringUtils.isNotEmpty(injectionKey);
      boolean hasKeyData = hasKey && injectionKeyMap.containsKey(injectionKey);
      boolean hasGroupKey = StringUtils.isNotEmpty(groupKey);
      boolean hasGroupData = hasGroupKey && groupKeyMap.containsKey(groupKey);

      Object valueToSet =
          StringUtils.isEmpty(injectionKey) ? null : injectionKeyMap.get(injectionKey);

      if (hasKey && hasKeyData && !hasGroupData) {
        injectValue(property, metadataProvider, object, field, valueToSet);
      } else if (hasGroupData) {
        injectList(metadataProvider, object, groupKeyMap, field, objectClass, groupKey);
      } else if (!fieldClass.isEnum()
          && !fieldClass.isPrimitive()
          && !fieldClass.equals(String.class)
          && !fieldClass.isArray()) {
        // Typically this is an inline object or one with a group element around it.
        // The class is organized like that to facilitate "correct" XML generation and parsing.
        // In this scenario we simply navigate into the object given by the field getter.
        //
        // We already know that there is no injection key set so we can't inject into primitives
        // or enums at this point.  The developer chose not to want this.
        //
        injectIntoInlineField(
            metadataProvider, object, injectionKeyMap, groupKeyMap, field, objectClass);
      }
    }
  }

  private static void injectIntoInlineField(
      IHopMetadataProvider metadataProvider,
      Object object,
      Map<String, Object> dataMap,
      Map<String, RowBuffer> listItemData,
      Field field,
      Class<?> objectClass)
      throws IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException,
          HopException {
    // Grab the value of this field and inject into this object
    //
    Object fieldObject = findGetter(objectClass, field).invoke(object);
    if (fieldObject == null) {
      throw new HopException(
          "Make sure that field "
              + field.getName()
              + " in class "
              + objectClass.getName()
              + " has a value to inject into");
    }
    inject(metadataProvider, fieldObject, dataMap, listItemData);
  }

  private static void injectValue(
      HopMetadataProperty property,
      IHopMetadataProvider metadataProvider,
      Object object,
      Field field,
      Object valueToSet)
      throws Exception {
    Object fieldValue = valueToSet;
    // Is there a converter defined so that we know how to convert from String to integer?
    //
    Class<? extends IIntCodeConverter> intConverterClass = getIntConverter(property);
    Class<? extends IStringObjectConverter> stringObjectConverterClass =
        getStringObjectConverter(property);

    if (intConverterClass != null && (valueToSet instanceof String string)) {
      // There is an intConverter specified on the HopMetadataProperty.
      // We can use it to convert the String to an integer.
      // For example: String --> 2 (IValueMeta.TYPE_STRING)
      //
      fieldValue = convertStringWithIntConverter(string, intConverterClass);
    } else if (field.getType().isEnum()
        && property.storeWithCode()
        && (valueToSet instanceof String code)) {
      // This is an enum and we need to find its value with the code given.
      // The enum implements IEnumHasCode
      //
      fieldValue = convertCodeToEnumeration(field, code);
    } else if (field.getType().isEnum() && (fieldValue instanceof String name)) {
      // A simple enum stored by its name.
      //
      fieldValue = convertEnumNameToValue(field, name);
    } else if (property.storeWithName() && (valueToSet instanceof String name)) {
      // A reference to a metadata element is stored with its name.
      // We look it up using the metadata provider.
      //
      fieldValue = convertNameToMetadataElement(metadataProvider, field, name);
    } else if (stringObjectConverterClass != null && (valueToSet instanceof String string)) {
      // There is a specific string converter to use.
      //
      fieldValue = convertWithStringToObjectConverter(stringObjectConverterClass, field, string);
    } else if (valueToSet != null) {
      // We'll do some data type conversions for convenience.
      // This allows simple String to Int or Long to int conversions don't cause any issues.
      //
      fieldValue = convertWithInjectionConverter(property, field, valueToSet, fieldValue);
    }

    // We need to set the data on the field
    //
    setValue(object, field, fieldValue);
  }

  private static Object convertWithStringToObjectConverter(
      Class<? extends IStringObjectConverter> stringObjectConverterClass,
      Field field,
      String string)
      throws HopException {
    try {
      IStringObjectConverter converter = stringObjectConverterClass.getConstructor().newInstance();
      return converter.getObject(string);
    } catch (Exception e) {
      throw new HopException(
          "Error converting string to object of field "
              + field.getName()
              + " of type "
              + field.getType().getName()
              + " using string: "
              + string,
          e);
    }
  }

  private static Object convertEnumNameToValue(Field field, String name) throws HopException {
    try {
      Enum<?>[] enumConstants = (Enum<?>[]) field.getType().getEnumConstants();

      // Backward compatibility: if the name is an integer, get the value at the given index.
      //
      int index = Const.toInt(name, -1);
      if (index < 0 || index >= enumConstants.length) {
        for (Enum<?> enumConstant : enumConstants) {
          if (enumConstant.name().equals(name)) {
            return enumConstant;
          }
        }
      } else {
        return enumConstants[index];
      }
      throw new HopException(
          "Unrecognized enum name: " + name + " for class " + field.getType().getName());
    } catch (Exception e) {
      throw new HopException(
          "Error converting enumeration with name '"
              + name
              + "' to its value for Enum class "
              + field.getType().getName());
    }
  }

  private static Object convertNameToMetadataElement(
      IHopMetadataProvider metadataProvider, Field field, String name) throws HopException {
    Object fieldValue;
    Class<?> metadataClass = field.getType();
    IHopMetadataSerializer<? extends IHopMetadata> serializer =
        metadataProvider.getSerializer((Class<? extends IHopMetadata>) metadataClass);
    fieldValue = serializer.load(name);
    return fieldValue;
  }

  private static Object convertWithInjectionConverter(
      HopMetadataProperty property, Field field, Object valueToSet, Object fieldValue)
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException,
          HopValueException {
    Class<? extends InjectionTypeConverter> injectionConverter = property.injectionConverter();
    InjectionTypeConverter converter = injectionConverter.getConstructor().newInstance();

    Class<?> fieldClass = field.getType();
    Class<?> valueClass = valueToSet.getClass();

    // String to boolean
    if (valueClass.equals(String.class)
        && (fieldClass.equals(Boolean.class) || fieldClass.equals(boolean.class))) {
      fieldValue = converter.string2booleanPrimitive((String) fieldValue);
    } else
    // String to integer
    if (valueClass.equals(String.class)
        && (fieldClass.equals(Integer.class) || fieldClass.equals(int.class))) {
      fieldValue = converter.string2intPrimitive((String) fieldValue);
    } else
    // String to long
    if (valueClass.equals(String.class)
        && (fieldClass.equals(Long.class) || fieldClass.equals(long.class))) {
      fieldValue = converter.string2longPrimitive((String) fieldValue);
    } else
    // Integer to String
    //
    if (fieldClass.equals(String.class) && (valueClass.equals(Integer.class))) {
      fieldValue = Integer.toString((int) fieldValue);
    } else
    // long to int
    if (fieldClass.equals(int.class) && (valueClass.equals(Long.class))) {
      //noinspection ReassignedVariable
      fieldValue = Long.valueOf((long) fieldValue).intValue();
    } else
    // int to long
    if (fieldClass.equals(Long.class) && (valueClass.equals(int.class))) {
      //noinspection UnnecessaryBoxing
      fieldValue = Long.valueOf((int) fieldValue);
    } else // anything to String
    if (fieldClass.equals(String.class)) {
      fieldValue = fieldValue.toString();
    }
    return fieldValue;
  }

  private static Class<? extends IIntCodeConverter> getIntConverter(HopMetadataProperty property) {
    Class<? extends IIntCodeConverter> converter = property.intCodeConverter();
    if (converter.equals(IIntCodeConverter.None.class)) {
      return null;
    }
    return converter;
  }

  private static Class<? extends IStringObjectConverter> getStringObjectConverter(
      HopMetadataProperty property) {
    Class<? extends IStringObjectConverter> converter = property.injectionStringObjectConverter();
    if (converter.equals(IStringObjectConverter.None.class)) {
      return null;
    }
    return converter;
  }

  private static Object convertCodeToEnumeration(Field field, String code) throws HopException {
    Object fieldValue;
    final Class<? extends IEnumHasCode> enumerationClass =
        (Class<? extends IEnumHasCode>) field.getType();
    fieldValue = IEnumHasCode.lookupCode(enumerationClass, code, null);
    if (fieldValue == null) {
      // For backward compatibility, retry with the name as well
      //
      try {
        fieldValue = convertEnumNameToValue(field, code);
      } catch (HopException e) {
        throw new HopException(
            "Enumeration code '"
                + code
                + "' was not recognized for class "
                + enumerationClass.getName(),
            e);
      }
    }
    return fieldValue;
  }

  private static Object convertStringWithIntConverter(
      String string, Class<? extends IIntCodeConverter> intConverterClass)
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException {
    Object fieldValue;
    IIntCodeConverter converter = intConverterClass.getConstructor().newInstance();
    fieldValue = converter.getType(string);
    return fieldValue;
  }

  private static void injectList(
      IHopMetadataProvider metadataProvider,
      Object object,
      Map<String, RowBuffer> listItemData,
      Field field,
      Class<?> objectClass,
      String groupKey)
      throws HopException {
    try {
      // What is the list?
      //
      List<Object> list = (List<Object>) findGetter(objectClass, field).invoke(object);
      list.clear();

      // Which class is being listed?
      //
      ParameterizedType type = (ParameterizedType) field.getGenericType();
      Class<?> listItemClass = (Class<?>) type.getActualTypeArguments()[0];

      // We're given a row buffer for the group specified
      //
      RowBuffer rowBuffer = listItemData.get(groupKey);
      if (rowBuffer == null) {
        rowBuffer = new RowBuffer();
      }
      // Loop over the rows in the buffer, then over the keys.
      // The keys (and their types) are in the row metadata.
      //
      IRowMeta rowMeta = rowBuffer.getRowMeta();
      for (Object[] row : rowBuffer.getBuffer()) {
        // Is this a primitive type we're dealing with?
        //
        if (listItemClass.isPrimitive() || listItemClass.equals(String.class)) {
          // The row contains a single value that we simply need to add to the list
          //
          list.add(row[0]);
        } else {
          //  Create a new list item for every row.
          //
          Object listItemObject = listItemClass.getConstructor().newInstance();

          // Add it to the list
          list.add(listItemObject);

          // Now set values on the list item object:
          //
          for (int valueIndex = 0; valueIndex < rowMeta.size(); valueIndex++) {
            IValueMeta valueMeta = rowMeta.getValueMeta(valueIndex);
            String valueKey = valueMeta.getName();
            Object value = row[valueIndex];

            // The map will contain just a single value matching the expected key in the underlying
            // object
            // class field annotation.  Only that field will be set.
            //
            if (StringUtils.isEmpty(valueKey)) {
              throw new HopException("No value key found for list item " + listItemClass.getName());
            } else {
              Map<String, Object> listItemValueMap = new HashMap<>();
              listItemValueMap.put(valueKey, value);
              inject(metadataProvider, listItemObject, listItemValueMap, Map.of());
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error while injecting list items in class "
              + object.getClass().getName()
              + ", field: "
              + field.getName(),
          e);
    }
  }

  private static void setValue(Object object, Field field, Object valueToSet) throws Exception {
    try {
      if (valueToSet == null) {
        return;
      }
      Method setter = findSetter(object.getClass(), field);
      setter.invoke(object, valueToSet);
    } catch (Exception e) {
      throw new HopException(
          "Error while injecting value in class "
              + object.getClass().getName()
              + ", field "
              + field.getName()
              + " with value '"
              + valueToSet
              + " ("
              + valueToSet.getClass()
              + ")",
          e);
    }
  }

  private static Method findGetter(Class<?> clazz, Field field) throws NoSuchMethodException {
    String name = field.getName();
    String methodName;
    if (field.getGenericType().equals(Boolean.class)
        || field.getGenericType().equals(boolean.class)) {
      methodName = "is" + StringUtils.capitalize(name);
    } else {
      methodName = "get" + StringUtils.capitalize(name);
    }
    return clazz.getMethod(methodName);
  }

  private static Method findSetter(Class<?> clazz, Field field) throws NoSuchMethodException {
    String name = field.getName();
    String methodName = "set" + StringUtils.capitalize(name);
    return clazz.getMethod(methodName, field.getType());
  }
}
