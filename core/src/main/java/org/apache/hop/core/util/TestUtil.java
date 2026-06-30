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
 *
 */

package org.apache.hop.core.util;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaSerializable;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;

public class TestUtil {
  public static void registerTestPluginTypes() throws HopException {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        ValueMetaNone.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaString.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaInteger.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaNumber.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaDate.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaBigNumber.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaBoolean.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaSerializable.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaBinary.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaTimestamp.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaInternetAddress.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaAvroRecord.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);

    registry.registerPluginClass(
        NoneDatabaseMeta.class.getName(), DatabasePluginType.class, DatabaseMetaPlugin.class);

    registry.registerPluginClass(
        HopTwoWayPasswordEncoder.class.getName(),
        TwoWayPasswordEncoderPluginType.class,
        TwoWayPasswordEncoderPlugin.class);
  }
}
