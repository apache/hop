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
package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class MappingIODefinitionLoadSaveValidator implements IFieldLoadSaveValidator<MappingIODefinition> {
  final Random rand = new Random();
  @Override
  public MappingIODefinition getTestObject() {
    MappingIODefinition rtn = new MappingIODefinition();
    rtn.setDescription( UUID.randomUUID().toString() );
    rtn.setInputTransformName( UUID.randomUUID().toString() );
    rtn.setMainDataPath( rand.nextBoolean() );
    rtn.setOutputTransformName( UUID.randomUUID().toString() );
    rtn.setRenamingOnOutput( rand.nextBoolean() );
    List<MappingValueRename> renames = Arrays.asList(
        new MappingValueRename( UUID.randomUUID().toString(), UUID.randomUUID().toString() ),
        new MappingValueRename( UUID.randomUUID().toString(), UUID.randomUUID().toString() ),
        new MappingValueRename( UUID.randomUUID().toString(), UUID.randomUUID().toString() )
    );

    rtn.setValueRenames( renames );
    return rtn;
  }

  @Override
  public boolean validateTestObject( MappingIODefinition testObject, Object actual ) {
    if ( !( actual instanceof MappingIODefinition ) ) {
      return false;
    }
    MappingIODefinition actualInput = (MappingIODefinition) actual;
    return ( testObject.getXml().equals( actualInput.getXml() ) );
  }
}
