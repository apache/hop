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

import java.util.Locale;
import java.util.Set;

/**
 * What the driver said about the server on the other end of a connection.
 *
 * <p>A dialect covers every version of its database; a type does not. SQL Server grew a JSON type
 * in 2025 and Oracle in 21c, so the same dialect connected to an older server has to write
 * something else. This is what a dialect answers that question from, in {@link
 * org.apache.hop.core.database.IDatabase#isColumnTypeAvailable}.
 *
 * <p>Read once per connection, the first time a column definition is generated, so nothing pays for
 * it on the row paths.
 *
 * @param majorVersion the server's major version, or -1 when the driver would not say
 * @param minorVersion the server's minor version, or -1 when the driver would not say
 * @param typeNames the type names {@code DatabaseMetaData.getTypeInfo()} reported, upper case.
 *     Treat as a hint rather than as truth: several drivers answer this from a list compiled into
 *     the driver rather than by asking the server, so a type missing from it may still exist.
 */
public record ServerInfo(int majorVersion, int minorVersion, Set<String> typeNames) {

  /** Nothing known, which is how every check answers "available". */
  public static final ServerInfo UNKNOWN = new ServerInfo(-1, -1, Set.of());

  /** True when the server is at least this major version. Unknown versions answer true. */
  public static boolean atLeast(ServerInfo serverInfo, int major) {
    return serverInfo == null
        || serverInfo.majorVersion() < 0
        || serverInfo.majorVersion() >= major;
  }

  /** Whether the driver's type list mentions this name. */
  public boolean hasTypeName(String typeName) {
    return typeName != null && typeNames.contains(typeName.toUpperCase(Locale.ROOT));
  }
}
