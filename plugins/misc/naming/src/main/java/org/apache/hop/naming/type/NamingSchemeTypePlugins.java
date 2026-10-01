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

package org.apache.hop.naming.type;

import org.apache.hop.core.naming.NamingSchemeTypePlugin;

/** Built-in naming-scheme kinds shipped with Hop. */
public final class NamingSchemeTypePlugins {

  private NamingSchemeTypePlugins() {
    // holder
  }

  @NamingSchemeTypePlugin(id = "general", name = "General")
  public static final class General extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-field", name = "Hop field names")
  public static final class HopField extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-transform", name = "Hop transform names")
  public static final class HopTransform extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-action", name = "Hop action names")
  public static final class HopAction extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-pipeline", name = "Hop pipeline names")
  public static final class HopPipeline extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-workflow", name = "Hop workflow names")
  public static final class HopWorkflow extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-metadata", name = "Hop metadata names")
  public static final class HopMetadata extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "hop-variable", name = "Hop variable names")
  public static final class HopVariable extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "database-table", name = "Database tables")
  public static final class DatabaseTable extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "database-column", name = "Database columns")
  public static final class DatabaseColumn extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "file", name = "Files")
  public static final class File extends BuiltinNamingSchemeType {}

  @NamingSchemeTypePlugin(id = "folder", name = "Folders")
  public static final class Folder extends BuiltinNamingSchemeType {}
}
