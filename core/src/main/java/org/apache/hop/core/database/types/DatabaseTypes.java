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
package org.apache.hop.core.database.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;

/**
 * Builds a dialect's type rules as a table of declarations rather than a switch statement.
 *
 * <pre>
 * DatabaseTypes.rules()
 *     .readNative("json", "jsonb").as(IValueMeta.TYPE_JSON)
 *     .read(Types.TIME).as(IValueMeta.TYPE_DATE)
 *     .write(IValueMeta.TYPE_JSON).as("JSONB")
 *     .write(IValueMeta.TYPE_BINARY).as("BYTEA")
 *     .write(IValueMeta.TYPE_INTEGER)
 *         .as(v -&gt; v.getLength() &lt; 5 ? "SMALLINT" : v.getLength() &lt;= 9 ? "INTEGER" : "BIGINT")
 *     .build();
 * </pre>
 *
 * <p>Rules are matched in declaration order, first match wins. Anything the table cannot express
 * can be added as a hand-written {@link IDatabaseTypeRule} through {@link Builder#rule}, and a
 * dialect can inherit another's table with {@link Builder#include}.
 */
public final class DatabaseTypes {

  /** A read rule condition with access to everything the mapping can depend on. */
  @FunctionalInterface
  public interface RuleCondition {
    boolean test(IVariables variables, DatabaseMeta databaseMeta, DatabaseColumn column);
  }

  private DatabaseTypes() {
    // Utility class.
  }

  /** Starts a new rule table. */
  public static Builder rules() {
    return new Builder();
  }

  /** Collects rules in declaration order. */
  public static final class Builder {
    private final List<IDatabaseTypeRule> rules = new ArrayList<>();

    /** Matches columns reported as any of these {@link java.sql.Types} constants. */
    public ReadBuilder read(int... sqlTypes) {
      return new ReadBuilder(this, sqlTypes, null, null);
    }

    /** Matches columns whose database type name is any of these, ignoring case. */
    public ReadBuilder readNative(String... nativeTypeNames) {
      Set<String> names = new LinkedHashSet<>();
      for (String name : nativeTypeNames) {
        names.add(name.toUpperCase(Locale.ROOT));
      }
      return new ReadBuilder(this, null, names, null);
    }

    /** Matches columns whose database type name matches this regular expression, ignoring case. */
    public ReadBuilder readNativeMatching(String regex) {
      return new ReadBuilder(this, null, null, Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }

    /** Declares how one or more Hop types are written as a column definition. */
    public WriteBuilder write(int... hopTypes) {
      return new WriteBuilder(this, hopTypes);
    }

    /** Declares how values of a Hop type move across JDBC on this database. */
    public Builder bind(int hopType, IValueBinding binding) {
      return bind(hopType, (database, valueMeta) -> true, binding);
    }

    /**
     * Declares how values of a Hop type move across JDBC, when the given condition holds. The
     * condition sees the dialect and the value metadata, which is everything available once rows
     * are moving.
     */
    public Builder bind(
        int hopType, BiPredicate<IDatabase, IValueMeta> when, IValueBinding binding) {
      rules.add(new BindingRule(hopType, when, binding));
      return this;
    }

    /** Adds a rule the table cannot express. */
    public Builder rule(IDatabaseTypeRule rule) {
      rules.add(rule);
      return this;
    }

    /** Appends another dialect's rules, for dialects that borrow rather than inherit. */
    public Builder include(List<IDatabaseTypeRule> other) {
      rules.addAll(other);
      return this;
    }

    public List<IDatabaseTypeRule> build() {
      return List.copyOf(rules);
    }
  }

  /** Declares how a database column is read into Hop value metadata. */
  public static final class ReadBuilder {
    private final Builder parent;
    private final int[] sqlTypes;
    private final Set<String> nativeNames;
    private final Pattern namePattern;
    private Set<String> extraNativeNames;
    private Predicate<DatabaseColumn> condition;
    private RuleCondition ruleCondition;
    private IValueBinding binding;

    private ReadBuilder(
        Builder parent, int[] sqlTypes, Set<String> nativeNames, Pattern namePattern) {
      this.parent = parent;
      this.sqlTypes = sqlTypes;
      this.nativeNames = nativeNames;
      this.namePattern = namePattern;
    }

    /** Narrows the match, typically on precision, scale or signedness. */
    public ReadBuilder where(Predicate<DatabaseColumn> condition) {
      this.condition = condition;
      return this;
    }

    /**
     * Narrows the match on the connection as well as the column, for rules that depend on a
     * connection property or on another of the dialect's capabilities.
     */
    public ReadBuilder where(RuleCondition condition) {
      this.ruleCondition = condition;
      return this;
    }

    /** Additionally requires one of these database type names, ignoring case. */
    public ReadBuilder nativeName(String... typeNames) {
      Set<String> names = new LinkedHashSet<>();
      for (String typeName : typeNames) {
        names.add(typeName.toUpperCase(Locale.ROOT));
      }
      this.extraNativeNames = names;
      return this;
    }

    /** Supplies driver-specific handling for values of columns this rule claims. */
    public ReadBuilder bind(IValueBinding binding) {
      this.binding = binding;
      return this;
    }

    /** Maps to a Hop type with no length or precision. Terminal. */
    public Builder as(int hopType) {
      return as(hopType, column -> -1, column -> -1);
    }

    /** Maps to a Hop type with a fixed length and precision. Terminal. */
    public Builder as(int hopType, int length, int precision) {
      return as(hopType, column -> length, column -> precision);
    }

    /** Maps to a Hop type whose length and precision are derived from the column. Terminal. */
    public Builder as(
        int hopType,
        ToIntFunction<DatabaseColumn> length,
        ToIntFunction<DatabaseColumn> precision) {
      Set<String> names = nativeNames;
      if (extraNativeNames != null) {
        names = extraNativeNames;
      }
      parent.rules.add(
          new ReadRule(
              sqlTypes,
              names,
              namePattern,
              condition,
              ruleCondition,
              hopType,
              length,
              precision,
              binding));
      return parent;
    }
  }

  /** Declares how a Hop value is written as a column definition. */
  public static final class WriteBuilder {
    private final Builder parent;
    private final int[] hopTypes;
    private Predicate<IValueMeta> condition;

    private WriteBuilder(Builder parent, int[] hopTypes) {
      this.parent = parent;
      this.hopTypes = hopTypes;
    }

    /** Narrows the match, typically on length or precision. */
    public WriteBuilder where(Predicate<IValueMeta> condition) {
      this.condition = condition;
      return this;
    }

    /** Uses a fixed column type. Terminal. */
    public Builder as(String columnType) {
      return as(valueMeta -> columnType);
    }

    /** Derives the column type from the value, for size-dependent types. Terminal. */
    public Builder as(Function<IValueMeta, String> columnType) {
      parent.rules.add(new WriteRule(hopTypes, condition, columnType));
      return parent;
    }
  }

  /** A declared read rule. */
  private static final class ReadRule implements IDatabaseTypeRule {
    private final int[] sqlTypes;
    private final Set<String> nativeNames;
    private final Pattern namePattern;
    private final Predicate<DatabaseColumn> condition;
    private final RuleCondition ruleCondition;
    private final int hopType;
    private final ToIntFunction<DatabaseColumn> length;
    private final ToIntFunction<DatabaseColumn> precision;
    private final IValueBinding binding;

    private ReadRule(
        int[] sqlTypes,
        Set<String> nativeNames,
        Pattern namePattern,
        Predicate<DatabaseColumn> condition,
        RuleCondition ruleCondition,
        int hopType,
        ToIntFunction<DatabaseColumn> length,
        ToIntFunction<DatabaseColumn> precision,
        IValueBinding binding) {
      this.sqlTypes = sqlTypes;
      this.nativeNames = nativeNames;
      this.namePattern = namePattern;
      this.condition = condition;
      this.ruleCondition = ruleCondition;
      this.hopType = hopType;
      this.length = length;
      this.precision = precision;
      this.binding = binding;
    }

    @Override
    public IValueMeta getValueMeta(
        IVariables variables, DatabaseMeta databaseMeta, DatabaseColumn column)
        throws HopDatabaseException {
      if (!matches(column)
          || (ruleCondition != null && !ruleCondition.test(variables, databaseMeta, column))) {
        return null;
      }
      try {
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(column.getName(), hopType);
        valueMeta.setLength(length.applyAsInt(column));
        valueMeta.setPrecision(precision.applyAsInt(column));
        StandardJdbcTypeMapper.setOriginalColumnMetadata(valueMeta, column, false);
        return valueMeta;
      } catch (HopPluginException e) {
        throw new HopDatabaseException(
            "Unable to create value metadata of type "
                + hopType
                + " for column "
                + column.getName(),
            e);
      }
    }

    private boolean matches(DatabaseColumn column) {
      if (sqlTypes != null && Arrays.stream(sqlTypes).noneMatch(t -> t == column.getSqlType())) {
        return false;
      }
      String nativeName = column.getNativeTypeName();
      if (nativeNames != null
          && (nativeName == null || !nativeNames.contains(nativeName.toUpperCase(Locale.ROOT)))) {
        return false;
      }
      if (namePattern != null
          && (nativeName == null || !namePattern.matcher(nativeName).matches())) {
        return false;
      }
      return condition == null || condition.test(column);
    }

    @Override
    public IValueBinding getBinding(IDatabase database, IValueMeta valueMeta) {
      return valueMeta.getType() == hopType ? binding : null;
    }

    @Override
    public boolean suppliesBindings() {
      return binding != null;
    }
  }

  /** A declared binding. */
  private static final class BindingRule implements IDatabaseTypeRule {
    private final int hopType;
    private final BiPredicate<IDatabase, IValueMeta> when;
    private final IValueBinding binding;

    private BindingRule(
        int hopType, BiPredicate<IDatabase, IValueMeta> when, IValueBinding binding) {
      this.hopType = hopType;
      this.when = when;
      this.binding = binding;
    }

    @Override
    public IValueBinding getBinding(IDatabase database, IValueMeta valueMeta) {
      return valueMeta.getType() == hopType && when.test(database, valueMeta) ? binding : null;
    }

    @Override
    public boolean suppliesBindings() {
      return true;
    }
  }

  /** A declared write rule. */
  private static final class WriteRule implements IDatabaseTypeRule {
    private final int[] hopTypes;
    private final Predicate<IValueMeta> condition;
    private final Function<IValueMeta, String> columnType;

    private WriteRule(
        int[] hopTypes, Predicate<IValueMeta> condition, Function<IValueMeta, String> columnType) {
      this.hopTypes = hopTypes;
      this.condition = condition;
      this.columnType = columnType;
    }

    @Override
    public String getColumnType(
        IVariables variables, IDatabase database, IValueMeta valueMeta, ColumnContext context) {
      if (Arrays.stream(hopTypes).noneMatch(t -> t == valueMeta.getType())) {
        return null;
      }
      if (condition != null && !condition.test(valueMeta)) {
        return null;
      }
      return columnType.apply(valueMeta);
    }
  }
}
