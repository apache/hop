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

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;

/**
 * One bidirectional column-type rule, owned by whoever knows the database rather than by core.
 *
 * <p>Rules are contributed from three places, and consulted in this order:
 *
 * <ol>
 *   <li>a {@code DatabaseTypeRulesPlugin}, which anyone can ship, including for a dialect they do
 *       not own
 *   <li>the dialect itself, through {@code IDatabase.getTypeRules()}
 *   <li>the value types, for their own neutral defaults
 * </ol>
 *
 * <p>Falling off the end of that list lands on {@link StandardJdbcTypeMapper}, the plain JDBC
 * behaviour. Every method here returns null to mean "not mine, ask the next rule", so a rule only
 * has to describe what it actually knows.
 */
public interface IDatabaseTypeRule {

  /**
   * Read path: claim this column and describe it as Hop value metadata.
   *
   * @return the value metadata, or null to defer to the next rule
   */
  default IValueMeta getValueMeta(
      IVariables variables, DatabaseMeta databaseMeta, DatabaseColumn column)
      throws HopDatabaseException {
    return null;
  }

  /**
   * Write path: render the column type for this value, without the column name.
   *
   * <p>The dialect is handed over rather than the {@link DatabaseMeta} wrapper, because the write
   * path is also reached from inside the dialect itself, when it assembles an ALTER TABLE
   * statement, and there is no wrapper in sight there. Nothing is lost by it: a connection option
   * that a type decision depends on lives on the dialect instance, which is where {@link
   * IDatabase#getTypeRules()} is answered from as well.
   *
   * @return the DDL type, or null to defer to the next rule
   */
  default String getColumnType(
      IVariables variables, IDatabase database, IValueMeta valueMeta, ColumnContext context) {
    return null;
  }

  /**
   * How values of this type move across JDBC on this database.
   *
   * <p>Chosen from the dialect and the value metadata rather than from column metadata, because by
   * the time rows are moving there is no column metadata left. Anything the decision depends on — a
   * driver quirk, a connection capability, the value's own precision — has to be answered here, so
   * that the binding itself only has to do the work.
   *
   * @return the binding, or null to use the default JDBC handling for the Hop type
   */
  default IValueBinding getBinding(IDatabase database, IValueMeta valueMeta) {
    return null;
  }

  /**
   * Whether this rule can ever supply a binding.
   *
   * <p>Answered once per dialect so that the per value lookup can skip every rule that never binds,
   * which is nearly all of them.
   */
  default boolean suppliesBindings() {
    return false;
  }
}
