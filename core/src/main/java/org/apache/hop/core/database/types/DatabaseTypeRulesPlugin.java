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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Contributes column-type rules to one or more database dialects, from outside those dialects.
 *
 * <p>This is what lets a plugin Hop does not ship teach an existing dialect a new type. A geometry
 * plugin, for example, can supply the Oracle mapping for SDO_GEOMETRY without any change to the
 * Oracle plugin:
 *
 * <pre>
 * &#64;DatabaseTypeRulesPlugin(
 *     id = "oracle-geometry",
 *     dialects = {"ORACLE"},
 *     valueTypes = {"Geometry"})
 * public class OracleGeometryTypeRules implements IDatabaseTypeRuleProvider { ... }
 * </pre>
 *
 * <p>Rules from here are consulted before the dialect's own, so a third party can also correct a
 * built-in dialect without patching it.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DatabaseTypeRulesPlugin {

  String id();

  String name() default "";

  String description() default "";

  /**
   * The database dialects these rules apply to, named by {@code DatabaseMetaPlugin.type()} — for
   * example ORACLE or POSTGRESQL. Naming them as strings rather than classes is deliberate: a
   * contributor must not have to compile against the dialect it extends.
   *
   * <p>Matching follows the dialect's class hierarchy, so POSTGRESQL also covers Redshift,
   * Greenplum and CrateDB. Empty means every dialect.
   */
  String[] dialects() default {};

  /**
   * Value type names these rules depend on, as reported by {@code ValueMetaFactory}. When one is
   * not installed the whole plugin is skipped rather than failing, so a dialect can ship optional
   * rules for a value type that may or may not be present.
   */
  String[] valueTypes() default {};

  boolean isSeparateClassLoaderNeeded() default false;

  String classLoaderGroup() default "";

  String documentationUrl() default "";

  String casesUrl() default "";

  String forumUrl() default "";
}
