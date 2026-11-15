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

package org.apache.hop.lineage.api;

import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.RelationalIoOperation;

/**
 * Implemented by transform metadata that can tell, without running, which files it reads or writes
 * and what they hold, or which SQL it runs. Publishing a project's pipelines to a catalog uses it,
 * so their datasets are known before a run reports them. Tables a transform writes are declared
 * with {@link RelationalLineage} instead.
 *
 * <p>Return what a run would report: the same file names, and the same layout, so the catalog holds
 * one dataset per file whether a run or a publication described it.
 */
public interface IDeclaresLineage {

  /** One thing a transform reads or writes. */
  sealed interface Declaration permits DeclaredFile, DeclaredSql {}

  /**
   * A file the transform reads or writes.
   *
   * @param operation {@link FileIoOperation#READ} or {@link FileIoOperation#WRITE}
   * @param filename the file name or URL, variables resolved
   * @param content the file's layout, or null when it is not known
   */
  record DeclaredFile(FileIoOperation operation, String filename, FileIoContentSchema content)
      implements Declaration {}

  /**
   * SQL the transform runs on a relational connection; a catalog finds the tables from the SQL.
   *
   * @param operation what the SQL does, such as {@link RelationalIoOperation#READ}
   * @param connectionName the name of the relational connection, variables resolved
   * @param sql the SQL, with variables resolved when the transform resolves them at runtime
   */
  record DeclaredSql(RelationalIoOperation operation, String connectionName, String sql)
      implements Declaration {}

  /**
   * What the transform reads and writes, as far as its metadata tells. Empty when that is only
   * known at runtime, such as a file name that comes from a field.
   *
   * @param variables the variables to resolve names with
   * @param inputRowMeta the fields that reach the transform, for a layout that depends on them
   */
  List<Declaration> declaredLineage(IVariables variables, IRowMeta inputRowMeta);
}
