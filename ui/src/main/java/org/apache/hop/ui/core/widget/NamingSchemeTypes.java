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

package org.apache.hop.ui.core.widget;

/**
 * String codes for {@code ColumnInfo#setNamingSchemeType(String)} and {@link
 * TextVar#enableNamingSchemes(String)}. Values must match the naming plugin's {@code
 * NamingSchemeType} codes. Kept in {@code ui} so dialogs do not depend on that plugin.
 */
public final class NamingSchemeTypes {

  public static final String GENERAL = "general";
  public static final String HOP_FIELD = "hop-field";
  public static final String HOP_TRANSFORM = "hop-transform";
  public static final String HOP_ACTION = "hop-action";
  public static final String HOP_PIPELINE = "hop-pipeline";
  public static final String HOP_WORKFLOW = "hop-workflow";
  public static final String HOP_METADATA = "hop-metadata";
  public static final String HOP_VARIABLE = "hop-variable";
  public static final String DATABASE_TABLE = "database-table";
  public static final String DATABASE_COLUMN = "database-column";
  public static final String FILE = "file";
  public static final String FOLDER = "folder";

  private NamingSchemeTypes() {
    // constants
  }
}
