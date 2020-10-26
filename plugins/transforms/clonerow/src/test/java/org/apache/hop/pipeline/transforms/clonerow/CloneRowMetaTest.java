/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.clonerow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloneRowMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList(
      "nrclones",
      "addcloneflag",
      "cloneflagfield",
      "nrcloneinfield",
      "nrclonefield",
      "addclonenum",
      "clonenumfield" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "nrclones", "getNrClones" );
    getterMap.put( "addcloneflag", "isAddCloneFlag" );
    getterMap.put( "cloneflagfield", "getCloneFlagField" );
    getterMap.put( "nrcloneinfield", "isNrCloneInField" );
    getterMap.put( "nrclonefield", "getNrCloneField" );
    getterMap.put( "addclonenum", "isAddCloneNum" );
    getterMap.put( "clonenumfield", "getCloneNumField" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "nrclones", "setNrClones" );
    setterMap.put( "addcloneflag", "setAddCloneFlag" );
    setterMap.put( "cloneflagfield", "setCloneFlagField" );
    setterMap.put( "nrcloneinfield", "setNrCloneInField" );
    setterMap.put( "nrclonefield", "setNrCloneField" );
    setterMap.put( "addclonenum", "setAddCloneNum" );
    setterMap.put( "clonenumfield", "setCloneNumField" );

    LoadSaveTester loadSaveTester = new LoadSaveTester( CloneRowMeta.class, attributes, getterMap, setterMap );
    loadSaveTester.testSerialization();
  }
}
