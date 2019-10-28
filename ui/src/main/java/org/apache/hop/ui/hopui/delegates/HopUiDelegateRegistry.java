/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.ui.hopui.delegates;

import java.lang.reflect.Constructor;

import org.apache.hop.ui.hopui.InstanceCreationException;
import org.apache.hop.ui.hopui.HopUi;

public class HopUiDelegateRegistry {

  public static final Class<?> DEFAULT_SPOONJOBDELEGATE_CLASS = HopUiJobDelegate.class;
  public static final Class<?> DEFAULT_SPOONTRANSDELEGATE_CLASS = HopUiTransformationDelegate.class;
  private static HopUiDelegateRegistry instance;
  private Class<?> jobDelegateClass;
  private Class<?> transDelegateClass;

  private HopUiDelegateRegistry() {

  }

  public static HopUiDelegateRegistry getInstance() {
    if ( instance == null ) {
      instance = new HopUiDelegateRegistry();
    }
    return instance;
  }

  public void registerSpoonJobDelegateClass( Class<?> jobDelegateClass ) {
    this.jobDelegateClass = jobDelegateClass;
  }

  public void registerSpoonTransDelegateClass( Class<?> transDelegateClass ) {
    this.transDelegateClass = transDelegateClass;
  }

  public Class<?> getRegisteredSpoonJobDelegateClass() {
    return this.jobDelegateClass;
  }

  public Class<?> getRegisteredSpoonTransDelegateClass() {
    return this.transDelegateClass;
  }

  public HopUiDelegate constructSpoonJobDelegate( HopUi hopUi ) throws InstanceCreationException {
    try {
      if ( jobDelegateClass == null ) {
        Constructor<?> constructor = DEFAULT_SPOONJOBDELEGATE_CLASS.getConstructor( HopUi.class );
        if ( constructor != null ) {
          return (HopUiDelegate) constructor.newInstance( hopUi );
        } else {
          throw new InstanceCreationException( "Unable to get the constructor for " + jobDelegateClass );
        }
      }
      Constructor<?> constructor = jobDelegateClass.getConstructor( HopUi.class );
      if ( constructor != null ) {
        return (HopUiDelegate) constructor.newInstance( hopUi );
      } else {
        throw new InstanceCreationException( "Unable to get the constructor for " + jobDelegateClass );
      }
    } catch ( Exception e ) {
      throw new InstanceCreationException( "Unable to instantiate object for " + jobDelegateClass, e );
    }
  }

  public HopUiDelegate constructSpoonTransDelegate( HopUi hopUi ) throws InstanceCreationException {
    try {
      if ( transDelegateClass == null ) {
        Constructor<?> constructor = DEFAULT_SPOONTRANSDELEGATE_CLASS.getConstructor( HopUi.class );
        if ( constructor != null ) {
          return (HopUiDelegate) constructor.newInstance( hopUi );
        } else {
          throw new InstanceCreationException( "Unable to get the constructor for " + transDelegateClass );
        }
      }
      Constructor<?> constructor = transDelegateClass.getConstructor( HopUi.class );
      if ( constructor != null ) {
        return (HopUiDelegate) constructor.newInstance( hopUi );
      } else {
        throw new InstanceCreationException( "Unable to get the constructor for " + transDelegateClass );
      }
    } catch ( Exception e ) {
      throw new InstanceCreationException( "Unable to instantiate object for " + transDelegateClass, e );
    }
  }
}
