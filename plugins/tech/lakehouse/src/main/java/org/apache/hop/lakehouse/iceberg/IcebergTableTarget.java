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
 */

package org.apache.hop.lakehouse.iceberg;

import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/** A table to write to: one that may exist already, or that a write creates. */
public interface IcebergTableTarget {

  /** A readable name for messages. */
  String name();

  boolean exists() throws HopException;

  /** The table, to write to. */
  Table load() throws HopException;

  /** The table, to read from. */
  default Table read() throws HopException {
    return load();
  }

  /**
   * Starts creating the table. Nothing is visible until the transaction is committed, together with
   * the first snapshot.
   */
  Transaction create(Schema schema, PartitionSpec spec, Map<String, String> properties)
      throws HopException;

  /** A table in a folder, in the layout of Hadoop catalogs and Spark path tables. */
  static IcebergTableTarget atPath(String location) {
    PathTableOperations ops = new PathTableOperations(location);
    return new IcebergTableTarget() {
      @Override
      public String name() {
        return ops.location();
      }

      @Override
      public boolean exists() {
        return ops.current() != null;
      }

      @Override
      public Table load() throws HopException {
        if (!ops.isPathTable()) {
          throw new HopException(
              "The Iceberg table at "
                  + ops.location()
                  + " is managed by a catalog. Write to it through that catalog (table mode), so"
                  + " the catalog sees the change.");
        }
        return new BaseTable(ops, ops.location());
      }

      @Override
      public Table read() throws HopException {
        return IcebergTables.loadFromPath(ops.location());
      }

      @Override
      public Transaction create(Schema schema, PartitionSpec spec, Map<String, String> properties) {
        TableMetadata metadata =
            TableMetadata.newTableMetadata(schema, spec, ops.location(), properties);
        return Transactions.createTableTransaction(ops.location(), ops, metadata);
      }
    };
  }

  /** A table in a catalog. */
  static IcebergTableTarget inCatalog(Catalog catalog, TableIdentifier identifier) {
    return new IcebergTableTarget() {
      @Override
      public String name() {
        return catalog.name() + "." + identifier;
      }

      @Override
      public boolean exists() {
        return catalog.tableExists(identifier);
      }

      @Override
      public Table load() {
        return catalog.loadTable(identifier);
      }

      @Override
      public Transaction create(Schema schema, PartitionSpec spec, Map<String, String> properties) {
        return catalog
            .buildTable(identifier, schema)
            .withPartitionSpec(spec)
            .withProperties(properties)
            .createTransaction();
      }
    };
  }
}
