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

package org.apache.hop.pipeline.transforms.memgroupby;

import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.None;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.getTypeWithCode;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.getTypeWithDescription;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.values;

import org.apache.hop.core.Const;
import org.apache.hop.core.injection.InjectionTypeConverter;

public class GroupTypeConverter extends InjectionTypeConverter {
  public GroupTypeConverter() {
    // Do nothing
  }

  @Override
  public Enum<?> string2enum(Class<?> enumClass, String value) {
    // This is the old unusual way of setting the type: with the internal ID
    // It's hard to imagine anyone used this to inject information but, there you go.
    //
    int type = Const.toInt(value, -1);
    if (type >= 0 && type < values().length) {
      return values()[type];
    }

    // We expect either a code (MIN, MAX, ...) like it's shown in the metadata (XML)
    // or a description as is shown in the GUI
    //
    MemoryGroupByMeta.GroupType groupType = getTypeWithCode(value);
    if (groupType == None) {
      return getTypeWithDescription(value);
    } else {
      return groupType;
    }
  }
}
