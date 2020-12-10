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

package org.apache.test.util;

import org.apache.test.util.impl.CollectionObjectProvider;
import org.apache.test.util.impl.DefaultObjectTester;
import org.apache.test.util.impl.DotEqualsValidator;
import org.apache.test.util.impl.EqualsEqualsValidator;

import java.util.ArrayList;
import java.util.Collection;

public class ObjectTesterBuilder<T> {
  private Collection<T> objects;

  private IObjectProvider<T> provider;

  private boolean useEqualsEquals = false;

  private IObjectValidator<T> validator;

  public IObjectTester<T> build() {
    IObjectProvider<T> provider = this.provider;
    if ( provider == null ) {
      if ( objects != null ) {
        provider = new CollectionObjectProvider<>( objects );
      }
    }
    IObjectValidator<T> validator = this.validator;
    if ( validator == null ) {
      if ( useEqualsEquals ) {
        validator = new EqualsEqualsValidator<>();
      } else {
        validator = new DotEqualsValidator<>();
      }
    }
    return new DefaultObjectTester<>( provider, validator );
  }

  public ObjectTesterBuilder<T> setObjects( Collection<T> objects ) {
    this.objects = objects;
    return this;
  }

  public ObjectTesterBuilder<T> addObject( T object ) {
    if ( this.objects == null ) {
      this.objects = new ArrayList<>();
    }
    this.objects.add( object );
    return this;
  }

  public ObjectTesterBuilder<T> setProvider( IObjectProvider<T> provider ) {
    this.provider = provider;
    return this;
  }

  public ObjectTesterBuilder<T> setUseEqualsEquals( boolean useDotEquals ) {
    this.useEqualsEquals = useDotEquals;
    return this;
  }

  public ObjectTesterBuilder<T> useEqualsEquals() {
    return setUseEqualsEquals( true );
  }

  public ObjectTesterBuilder<T> setValidator( IObjectValidator<T> validator ) {
    this.validator = validator;
    return this;
  }
}
