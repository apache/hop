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

package org.apache.hop.core.gui.plugin;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;

/**
 * Reads {@link GuiTableColumn} fields from the row class of a {@link GuiElementType#TABLE} widget.
 * A column whose type does not match its field is logged and left out. A bad widget is logged and
 * left without columns; startup keeps going.
 */
class GuiTableColumns extends BaseGuiElements {

  private static final GuiTableColumns INSTANCE = new GuiTableColumns();

  private GuiTableColumns() {}

  static void apply(GuiElements target, GuiWidgetElement annotation, Field field) {
    INSTANCE.populate(target, annotation, field);
  }

  /** Registry scans run in unit tests before {@link HopLogStore} exists. Do not fail those. */
  private static void logError(String message) {
    if (HopLogStore.isInitialized()) {
      LogChannel.GENERAL.logError(message);
    }
  }

  private void populate(GuiElements target, GuiWidgetElement annotation, Field field) {
    target.setTableRows(annotation.tableRows());
    if (annotation.type() != GuiElementType.TABLE) {
      return;
    }

    Class<?> rowClass = rowClass(target, field);
    if (rowClass == null) {
      return;
    }
    target.setTableRowClass(rowClass);

    List<GuiTableColumnElement> columns = columnsOf(rowClass);
    target.setTableColumns(columns);
    if (columns.isEmpty()) {
      logError(
          "TABLE widget '"
              + target.getId()
              + "' has no @GuiTableColumn fields on "
              + rowClass.getName());
    }
  }

  private Class<?> rowClass(GuiElements target, Field field) {
    if (!List.class.isAssignableFrom(field.getType())) {
      logError(
          "TABLE widget '"
              + target.getId()
              + "' on "
              + field.getDeclaringClass().getName()
              + "."
              + field.getName()
              + " must be a List");
      return null;
    }

    Type generic = field.getGenericType();
    if (!(generic instanceof ParameterizedType parameterized)) {
      logError(
          "TABLE widget '"
              + target.getId()
              + "' on "
              + field.getName()
              + " uses a raw List. Declare List<Row>.");
      return null;
    }

    Class<?> rowClass = classArgument(parameterized.getActualTypeArguments()[0]);
    if (rowClass == null) {
      logError(
          "TABLE widget '"
              + target.getId()
              + "' on "
              + field.getName()
              + " must be a List of a concrete row class");
    }
    return rowClass;
  }

  private Class<?> classArgument(Type argument) {
    if (argument instanceof Class<?> type) {
      return type;
    }
    if (argument instanceof WildcardType wildcard) {
      Type[] bounds = wildcard.getUpperBounds();
      if (bounds.length == 1 && bounds[0] instanceof Class<?> type) {
        return type;
      }
    }
    return null;
  }

  private List<GuiTableColumnElement> columnsOf(Class<?> rowClass) {
    // Walk from the row class upward so a subclass field of the same name hides the superclass
    // field. An unannotated subclass field hides it too: the subclass replaced the field.
    List<GuiTableColumnElement> columns = new ArrayList<>();
    Set<String> fieldNames = new HashSet<>();
    Set<String> ids = new HashSet<>();
    Class<?> type = rowClass;
    while (type != null && type != Object.class) {
      for (Field field : type.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
          continue;
        }
        if (!fieldNames.add(field.getName())) {
          continue;
        }
        GuiTableColumn annotation = field.getAnnotation(GuiTableColumn.class);
        if (annotation == null) {
          continue;
        }
        GuiTableColumnElement column = columnElement(annotation, field, rowClass);
        if (column == null) {
          continue;
        }
        if (!ids.add(column.getId())) {
          logError(
              "Skipping @GuiTableColumn on "
                  + field.getDeclaringClass().getSimpleName()
                  + "."
                  + field.getName()
                  + ": duplicate column id '"
                  + column.getId()
                  + "'");
          continue;
        }
        columns.add(column);
      }
      type = type.getSuperclass();
    }

    columns.sort(
        Comparator.comparing((GuiTableColumnElement column) -> Const.NVL(column.getOrder(), ""))
            .thenComparing(column -> Const.NVL(column.getId(), "")));
    return columns;
  }

  private GuiTableColumnElement columnElement(
      GuiTableColumn annotation, Field field, Class<?> rowClass) {
    if (!typeMatches(annotation, field)) {
      return null;
    }

    GuiTableColumnElement column = new GuiTableColumnElement();
    column.setId(StringUtils.isEmpty(annotation.id()) ? field.getName() : annotation.id());
    column.setOrder(annotation.order());
    column.setType(annotation.type());
    column.setFieldName(field.getName());
    column.setFieldClass(field.getType());
    column.setVariables(annotation.variables());
    column.setPassword(annotation.password());
    column.setWidth(annotation.width() < 0 ? -1 : annotation.width());
    column.setComboValuesMethod(annotation.comboValuesMethod());

    String label =
        getTranslation(
            annotation.label(),
            field.getDeclaringClass().getPackage().getName(),
            field.getDeclaringClass());
    column.setLabel(StringUtils.isEmpty(label) ? field.getName() : label);
    column.setToolTip(
        getTranslation(
            annotation.toolTip(),
            field.getDeclaringClass().getPackage().getName(),
            field.getDeclaringClass()));

    String getter = annotation.getterMethod();
    String setter = annotation.setterMethod();
    if (StringUtils.isEmpty(getter) || StringUtils.isEmpty(setter)) {
      try {
        PropertyDescriptor descriptor = new PropertyDescriptor(field.getName(), rowClass);
        if (StringUtils.isEmpty(getter) && descriptor.getReadMethod() != null) {
          getter = descriptor.getReadMethod().getName();
        }
        if (StringUtils.isEmpty(setter) && descriptor.getWriteMethod() != null) {
          setter = descriptor.getWriteMethod().getName();
        }
      } catch (IntrospectionException e) {
        // Fall through to the bean name. The method may still exist.
      }
    }
    if (StringUtils.isEmpty(getter)) {
      String suffix = StringUtil.initCap(field.getName());
      boolean flag = field.getType() == boolean.class || field.getType() == Boolean.class;
      getter = (flag ? "is" : "get") + suffix;
    }
    if (StringUtils.isEmpty(setter)) {
      setter = "set" + StringUtil.initCap(field.getName());
    }
    column.setGetterMethod(getter);
    column.setSetterMethod(setter);
    return column;
  }

  private boolean typeMatches(GuiTableColumn annotation, Field field) {
    Class<?> fieldType = field.getType();
    String where = field.getDeclaringClass().getSimpleName() + "." + field.getName();
    switch (annotation.type()) {
      case TEXT:
        if (fieldType != String.class) {
          logError("Skipping @GuiTableColumn on " + where + ": TEXT columns must be String");
          return false;
        }
        return true;
      case COMBO:
        if (fieldType != String.class && !fieldType.isEnum()) {
          logError(
              "Skipping @GuiTableColumn on " + where + ": COMBO columns must be String or an enum");
          return false;
        }
        return true;
      case CHECKBOX:
        if (fieldType != boolean.class && fieldType != Boolean.class) {
          logError("Skipping @GuiTableColumn on " + where + ": CHECKBOX columns must be boolean");
          return false;
        }
        return true;
      default:
        logError(
            "Skipping @GuiTableColumn on "
                + where
                + ": unsupported column type "
                + annotation.type());
        return false;
    }
  }
}
