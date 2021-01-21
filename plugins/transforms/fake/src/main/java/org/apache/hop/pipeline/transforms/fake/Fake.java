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

package org.apache.hop.pipeline.transforms.fake;

import com.github.javafaker.Faker;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Locale;

/**
 * Generates a number of (empty or the same) rows
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class Fake extends BaseTransform<FakeMeta, FakeData> implements ITransform<FakeMeta, FakeData> {

  private static final Class<?> PKG = FakeMeta.class; // For Translator

  public Fake( TransformMeta transformMeta, FakeMeta meta, FakeData data, int copyNr, PipelineMeta pipelineMeta,
               Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean init() {

    if ( StringUtils.isNotEmpty(meta.getLocale())) {
      data.faker = new Faker(new Locale( resolve(meta.getLocale())));
    } else {
      data.faker = new Faker();
    }

    data.fakerTypes = new ArrayList<>();
    data.fakerMethods = new ArrayList<>();
    for (FakeField field : meta.getFields()) {
      if ( field.isValid() ) {
        try {
          FakerType type = FakerType.valueOf( field.getType() );
          Method fakerMethod = data.faker.getClass().getMethod( type.getFakerMethod() );
          Object fakerType = fakerMethod.invoke( data.faker );
          data.fakerTypes.add( fakerType );
          Method topicMethod = fakerType.getClass().getMethod( field.getTopic() );
          data.fakerMethods.add( topicMethod );
        } catch ( Exception e ) {
          log.logError( "Error getting faker object or method for type " + field.getType() + " and topic " + field.getTopic(), e );
          return false;
        }
      }
    }

    return super.init();
  }

  public boolean processRow() throws HopException {

    Object[] row = getRow();

    if (row==null) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      // The output meta is the original input meta + the additional fake data fields.

      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    }

    Object[] outputRowData = RowDataUtil.resizeArray( row, data.outputRowMeta.size() );
    int rowIndex = getInputRowMeta().size();
    int index = 0;
    for ( FakeField field : meta.getFields() ) {
      if ( field.isValid() ) {
        Object fakerType = data.fakerTypes.get(index);
        Method fakerMethod = data.fakerMethods.get(index);
        index++;
        try {
          outputRowData[rowIndex++] = fakerMethod.invoke( fakerType );
        } catch ( Exception e ) {
          throw new HopException("Error getting faker value for field "+field.getName()+", type "+field.getType()+" and topic "+field.getTopic(), e);
        }
      }
    }

    putRow( data.outputRowMeta, outputRowData );

    return true;
  }

}
