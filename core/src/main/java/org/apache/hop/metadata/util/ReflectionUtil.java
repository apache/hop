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

package org.apache.hop.metadata.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.hop.core.exception.HopException;

public class ReflectionUtil {
  /** myAttribute ==> setMyAttribute */
  public static final String getSetterMethodName(String name) {

    StringBuilder setter = new StringBuilder();
    setter.append("set");
    setter.append(name.substring(0, 1).toUpperCase());
    setter.append(name.substring(1));

    return setter.toString();
  }

  /** myAttribute ==> getMyAttribute */
  public static final String getGetterMethodName(String name, boolean isBoolean) {

    StringBuilder setter = new StringBuilder();
    setter.append(isBoolean ? "is" : "get");
    setter.append(name.substring(0, 1).toUpperCase());
    setter.append(name.substring(1));

    return setter.toString();
  }

  /**
   * Find all fields from the given class as well as the fields from all the parent classes. It will
   * recurse all the way to the top class from which the given class inherits from.
   *
   * <p>This means that it's possible to inherit from other classes during serialization.
   *
   * @param clazz
   * @return A set of fields.
   */
  public static final List<Field> findAllFields(Class<?> clazz) {
    Set<Field> fieldsSet = new HashSet<>();

    // Find the fields from the root class
    //
    for (Field classField : clazz.getDeclaredFields()) {
      fieldsSet.add(classField);
    }
    // If this class has a parent class, grab the fields
    //
    Class<?> superClass = clazz.getSuperclass();
    while (superClass != null) {
      for (Field superClassField : superClass.getDeclaredFields()) {
        fieldsSet.add(superClassField);
      }

      // Repeat this process until we have no more super class
      //
      superClass = superClass.getSuperclass();
    }

    List<Field> fields = new ArrayList<>(fieldsSet);

    // Sort the fields by name
    Collections.sort(fields, Comparator.comparing(Field::getName));

    return fields;
  }

  /**
   * Find all fields from the given class as well as the fields from all the parent classes. It will
   * recurse all the way to the top class from which the given class inherits from.
   *
   * <p>This means that it's possible to inherit from other classes during serialization.
   *
   * @param clazz The class to investigate.
   * @param sortFunction the function to extract the key to sort on. If the function returns null
   *     the field is not included.
   * @return A sorted list of fields.
   */
  public static final List<Field> findAllFields(
      Class<?> clazz, Function<Field, String> sortFunction) {
    Set<Field> fieldsSet = new HashSet<>();

    // Find the fields from the root class
    //
    for (Field classField : clazz.getDeclaredFields()) {
      String keyField = sortFunction.apply(classField);
      if (keyField != null) {
        fieldsSet.add(classField);
      }
    }
    // If this class has a parent class, grab the fields
    //
    Class<?> superClass = clazz.getSuperclass();
    while (superClass != null) {
      for (Field superClassField : superClass.getDeclaredFields()) {
        String keyField = sortFunction.apply(superClassField);
        if (keyField != null) {
          fieldsSet.add(superClassField);
        }
      }

      // Repeat this process until we have no more super class
      //
      superClass = superClass.getSuperclass();
    }

    List<Field> fields = new ArrayList<>(fieldsSet);

    // Sort the fields by name
    Collections.sort(fields, Comparator.comparing(sortFunction::apply));

    return fields;
  }

  public static final Object getFieldValue(Object object, String fieldName, boolean isBoolean)
      throws HopException {
    Class<?> objectClass = object.getClass();
    String getterMethodName = ReflectionUtil.getGetterMethodName(fieldName, isBoolean);
    try {
      Method getterMethod = objectClass.getMethod(getterMethodName);
      return getterMethod.invoke(object);
    } catch (Exception e) {
      throw new HopException(
          "Error getting value for field '"
              + fieldName
              + "' using method '"
              + getterMethodName
              + "' in class '"
              + objectClass.getName(),
          e);
    }
  }

  public static final void setFieldValue(
      Object object, String fieldName, Class<?> fieldType, Object fieldValue) throws HopException {
    Class<?> objectClass = object.getClass();
    String setterMethodName = ReflectionUtil.getSetterMethodName(fieldName);
    try {
      Method setterMethod = objectClass.getMethod(setterMethodName, fieldType);
      setterMethod.invoke(object, fieldValue);
    } catch (Exception e) {
      throw new HopException(
          "Error setting value on field '"
              + fieldName
              + "' using method '"
              + setterMethodName
              + "' in class '"
              + objectClass.getName(),
          e);
    }
  }

  public static String getObjectName(Object object) throws HopException {
    try {
      return (String) ReflectionUtil.getFieldValue(object, "name", false);
    } catch (Exception e) {
      throw new HopException(
          "Unable to get the name of Hop metadata class '" + object.getClass().getName() + "'", e);
    }
  }
}
