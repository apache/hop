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
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

public class ReflectionUtil {
  private ReflectionUtil() {}

  /** myAttribute ==> setMyAttribute */
  public static String getSetterMethodName(String name) {
    return "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  /** myAttribute ==> getMyAttribute */
  public static String getGetterMethodName(String name, boolean isBoolean) {
    return (isBoolean ? "is" : "get") + name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  /**
   * Find all fields from the given class as well as the fields from all the parent classes. It will
   * recurse all the way to the top class from which the given class inherits from.
   *
   * <p>This means that it's possible to inherit from other classes during serialization.
   *
   * @param clazz The class to find fields for.
   * @return A set of fields.
   */
  public static List<Field> findAllFields(Class<?> clazz) {
    Set<Field> fieldsSet = new HashSet<>();

    // Find the fields from the root class
    //
    Collections.addAll(fieldsSet, clazz.getDeclaredFields());

    // If this class has a parent class, grab the fields
    //
    Class<?> superClass = clazz.getSuperclass();
    while (superClass != null) {
      Collections.addAll(fieldsSet, superClass.getDeclaredFields());

      // Repeat this process until we have no more super class
      //
      superClass = superClass.getSuperclass();
    }

    List<Field> fields = new ArrayList<>(fieldsSet);

    // Sort the fields by name
    fields.sort(Comparator.comparing(Field::getName));

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
  public static List<Field> findAllFields(Class<?> clazz, Function<Field, String> sortFunction) {
    return findAllFields(clazz, sortFunction, true);
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
  public static List<Field> findAllFields(
      Class<?> clazz, Function<Field, String> sortFunction, boolean sortFields) {
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
    if (sortFields) {
      fields.sort(Comparator.comparing(sortFunction));
    }

    return fields;
  }

  public static Object getFieldValue(Object object, String fieldName, boolean isBoolean)
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

  public static void setFieldValue(
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

  public static Method findGetter(Class<?> clazz, Field field) throws NoSuchMethodException {
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

  public static Method findSetter(Class<?> clazz, Field field) throws NoSuchMethodException {
    String name = field.getName();
    String methodName = "set" + StringUtils.capitalize(name);
    return clazz.getMethod(methodName, field.getType());
  }

  /**
   * Find all fields from the given class as well as the fields from all the parent classes. It will
   * recurse all the way to the top class from which the given class inherits from.
   *
   * <p>This means that it's possible to inherit from other classes during serialization.
   *
   * @param clazz The class to investigate.
   * @param prefix If you specify a non-null value only the methods starting with this prefix will
   *     be returned.
   * @return The list of methods with the given prefix.
   */
  public static List<Method> findAllMethods(Class<?> clazz, String prefix) {
    Set<Method> methodSet = new HashSet<>();

    // Find the methods from the root class
    //
    for (Method classMethod : clazz.getDeclaredMethods()) {
      if (StringUtils.isEmpty(prefix) || classMethod.getName().startsWith(prefix)) {
        methodSet.add(classMethod);
      }
    }

    // If this class has a parent class, grab the methods there as well
    //
    Class<?> superClass = clazz.getSuperclass();
    while (superClass != null) {
      for (Method superClassMethod : superClass.getDeclaredMethods()) {
        if (StringUtils.isEmpty(prefix) || superClassMethod.getName().startsWith(prefix)) {
          methodSet.add(superClassMethod);
        }
      }

      // Repeat this process until we have no more super class
      //
      superClass = superClass.getSuperclass();
    }

    return new ArrayList<>(methodSet);
  }
}
