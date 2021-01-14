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
package org.apache.hop.i18n;

public class PackageMessages {

  private final Class<?> packageClass;
  private final String prefix;

  public PackageMessages( final Class<?> packageClass ) {
    this( packageClass, null );
  }

  public PackageMessages( final Class<?> packageClass, String prefix ) {
    this.packageClass = packageClass;
    this.prefix = prefix == null ? packageClass.getSimpleName() + "." : prefix;
  }

  public String getString( final String key, final String... parameters ) {
    return getString( prefix, key, parameters );
  }

  public String getString( final String prefix, final String key, final String... parameters ) {
    return BaseMessages.getString( packageClass, prefix + key, parameters );
  }
}
