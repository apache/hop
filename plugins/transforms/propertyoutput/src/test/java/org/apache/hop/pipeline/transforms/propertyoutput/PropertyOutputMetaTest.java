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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PropertyOutputMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testSerialization() throws HopException {
    List<String> attributes = Arrays.asList( "KeyField", "ValueField", "Comment", "FileNameInField",
      "FileNameField", "FileName", "Extension", "TransformNrInFilename",
      //
      // Note - "partNrInFilename" not included above because while it seems to be serialized/deserialized in the meta,
      // there are no getters/setters and it's a private variable. Also, it's not included in the dialog. So it is
      // always serialized/deserialized as "false" (N).
      // MB - 5/2016
      "DateInFilename", "TimeInFilename", "CreateParentFolder", "AddToResult", "Append" );

    LoadSaveTester<PropertyOutputMeta> tester = new LoadSaveTester<>(
      PropertyOutputMeta.class, attributes );

    tester.testSerialization();
  }
}
