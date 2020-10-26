/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.univariatestats;

import org.apache.hop.core.row.IRowMeta;
import org.apache.test.util.GetterSetterTester;
import org.apache.test.util.ObjectTesterBuilder;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class UnivariateStatsDataTest {
  @Test
  public void testGettersAndSetters() {
    GetterSetterTester<UnivariateStatsData> getterSetterTester =
      new GetterSetterTester<UnivariateStatsData>( UnivariateStatsData.class );

    getterSetterTester.addObjectTester( "fieldIndexes", new ObjectTesterBuilder<FieldIndex[]>().addObject( null )
      .addObject( new FieldIndex[] {} ).useEqualsEquals().build() );
    IRowMeta mockRowMetaInterface = mock( IRowMeta.class );

    getterSetterTester.addObjectTester( "inputRowMeta", new ObjectTesterBuilder<IRowMeta>().addObject( null )
      .addObject( mockRowMetaInterface ).useEqualsEquals().build() );
    mockRowMetaInterface = mock( IRowMeta.class );

    getterSetterTester.addObjectTester( "outputRowMeta", new ObjectTesterBuilder<IRowMeta>().addObject( null )
      .addObject( mockRowMetaInterface ).useEqualsEquals().build() );
    getterSetterTester.test( new UnivariateStatsData() );
  }
}
