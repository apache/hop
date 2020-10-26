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

package org.apache.hop.core.injection.bean;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Objects.requireNonNull;

/**
 * Engine for get/set metadata injection properties from bean.
 */
public class BeanInjector<Meta extends ITransformMeta> {
  private final BeanInjectionInfo<Meta> info;

  public BeanInjector( BeanInjectionInfo<Meta> info ) {
    this.info = info;
  }

  public Object getObject( Object root, String propName ) throws Exception {
    BeanInjectionInfo<Meta>.Property prop = info.getProperties().get( propName );
    if ( prop == null ) {
      throw new RuntimeException( "Property not found" );
    }
    BeanLevelInfo<Meta> beanLevelInfo = prop.path.get( 1 );
    return beanLevelInfo.field.get( root );
  }

  /**
   * Retrieves the raw prop value from root object.
   * <p>
   * The similar {@link #getProperty(Object, String)} method (also in this class )should be
   * revisited and possibly eliminated.  That version attempts to retrieve indexed prop
   * vals from lists/arrays, but doesn't provide a way to retrieve the list or array objects
   * themselves.
   */
  public Object getPropVal( Object root, String propName ) {
    Queue<BeanLevelInfo> beanInfos =
      newLinkedList( Optional.ofNullable( info.getProperties().get( propName ) )
        .orElseThrow( () -> new IllegalArgumentException( "Property not found: " + propName ) )
        .path );
    beanInfos.remove();  // pop off root
    return getPropVal( root, propName, beanInfos );
  }

  @SuppressWarnings( "unchecked" )
  private Object getPropVal( Object obj, String propName, Queue<BeanLevelInfo> beanInfos ) {
    BeanLevelInfo info = beanInfos.remove();
    if ( beanInfos.isEmpty() ) {
      return getObjFromBeanInfo( obj, info );
    }
    obj = getObjFromBeanInfo( obj, info );
    switch ( info.dim ) {
      case LIST:
        return ( (List) requireNonNull( obj ) ).stream()
          .map( o -> getPropVal( o, propName, newLinkedList( beanInfos ) ) )
          .collect( Collectors.toList() );
      case ARRAY:
        return Arrays.stream( (Object[]) requireNonNull( obj ) )
          .map( o -> getPropVal( o, propName, newLinkedList( beanInfos ) ) )
          .toArray( Object[]::new );
      case NONE:
        return getPropVal( obj, propName, beanInfos );
    }
    throw new IllegalStateException( "Unexpected value of BeanLevelInfo.dim " + info.dim );
  }


  private Object getObjFromBeanInfo( Object obj, BeanLevelInfo beanLevelInfo ) {
    try {
      return beanLevelInfo.field == null ? null : beanLevelInfo.field.get( obj );
    } catch ( IllegalAccessException e ) {
      throw new RuntimeException( e );
    }
  }


  public Object getProperty( Object root, String propName ) throws Exception {
    List<Integer> extractedIndexes = new ArrayList<>();

    BeanInjectionInfo<Meta>.Property prop = info.getProperties().get( propName );
    if ( prop == null ) {
      throw new RuntimeException( "Property not found" );
    }

    Object obj = root;
    for ( int i = 1, arrIndex = 0; i < prop.path.size(); i++ ) {
      BeanLevelInfo<Meta> s = prop.path.get( i );
      obj = s.field.get( obj );
      if ( obj == null ) {
        return null; // some value in path is null - return empty
      }

      switch ( s.dim ) {
        case ARRAY:
          int indexArray = extractedIndexes.get( arrIndex++ );
          if ( Array.getLength( obj ) <= indexArray ) {
            return null;
          }
          obj = Array.get( obj, indexArray );
          if ( obj == null ) {
            return null; // element is empty
          }
          break;
        case LIST:
          int indexList = extractedIndexes.get( arrIndex++ );
          List<?> list = (List<?>) obj;
          if ( list.size() <= indexList ) {
            return null;
          }
          obj = list.get( indexList );
          if ( obj == null ) {
            return null; // element is empty
          }
          break;
        case NONE:
          break;
      }
    }
    return obj;
  }

  public boolean hasProperty( Object root, String propName ) {
    BeanInjectionInfo.Property prop = info.getProperties().get( propName );
    return prop != null;
  }

  public void setProperty( Object root, String propName, List<RowMetaAndData> data, String dataN )
    throws HopException {
    BeanInjectionInfo.Property prop = info.getProperties().get( propName );
    if ( prop == null ) {
      throw new HopException( "Property '" + propName + "' not found for injection to " + root.getClass() );
    }

    String dataName, dataValue;
    if ( data != null ) {
      dataName = dataN;
      dataValue = null;
    } else {
      dataName = null;
      dataValue = dataN;
    }
    if ( prop.pathArraysCount == 0 ) {
      // no arrays in path
      try {
        setProperty( root, prop, 0, data != null ? data.get( 0 ) : null, dataName, dataValue );
      } catch ( Exception ex ) {
        throw new HopException( "Error inject property '" + propName + "' into " + root.getClass(), ex );
      }
    } else if ( prop.pathArraysCount == 1 ) {
      // one array in path
      try {
        if ( data != null ) {
          for ( int i = 0; i < data.size(); i++ ) {
            setProperty( root, prop, i, data.get( i ), dataName, dataValue );
          }
        } else {
          for ( int i = 0; ; i++ ) {
            boolean found = setProperty( root, prop, i, null, null, dataValue );
            if ( !found ) {
              break;
            }
          }
        }
      } catch ( Exception ex ) {
        throw new HopException( "Error inject property '" + propName + "' into " + root.getClass(), ex );
      }
    } else {
      if ( prop.pathArraysCount > 1 ) {
        throw new HopException( "Property '" + propName + "' has more than one array in path for injection to "
          + root.getClass() );
      }
    }
  }

  /**
   * Sets data from RowMetaAndData, or constant value from dataValue depends on 'data != null'.
   */
  private boolean setProperty( Object root, BeanInjectionInfo<Meta>.Property prop, int index, RowMetaAndData data,
                               String dataName, String dataValue ) throws Exception {
    Object obj = root;
    for ( int i = 1; i < prop.path.size(); i++ ) {
      BeanLevelInfo<Meta> s = prop.path.get( i );
      if ( i < prop.path.size() - 1 ) {
        // get path
        Object next;
        switch ( s.dim ) {
          case ARRAY:
            // array
            Object existArray = data != null ? extendArray( s, obj, index + 1 ) : checkArray( s, obj, index );
            if ( existArray == null ) {
              // out of array for constant
              return false;
            }
            next = Array.get( existArray, index ); // get specific element
            if ( next == null ) {
              next = createObject( s.leafClass, root );
              Array.set( existArray, index, next );
            }
            obj = next;
            break;
          case LIST:
            // list
            List<Object> existList = data != null ? extendList( s, obj, index + 1 ) : checkList( s, obj, index );
            if ( existList == null ) {
              // out of array for constant
              return false;
            }
            next = existList.get( index ); // get specific element
            if ( next == null ) {
              next = createObject( s.leafClass, root );
              existList.set( index, next );
            }
            obj = next;
            break;
          case NONE:
            // plain field
            if ( s.field != null ) {
              next = s.field.get( obj );
              if ( next == null ) {
                next = createObject( s.leafClass, root );
                s.field.set( obj, next );
              }
              obj = next;
            } else if ( s.getter != null ) {
              next = s.getter.invoke( obj );
              if ( next == null ) {
                if ( s.setter == null ) {
                  throw new HopException( "No setter defined for " + root.getClass() );
                }
                next = s.leafClass.newInstance();
                s.setter.invoke( obj, next );
              }
              obj = next;
            } else {
              throw new HopException( "No field or getter defined for " + root.getClass() );
            }
            break;
        }
      } else {
        // set to latest field
        if ( !s.convertEmpty ) {
          if ( data != null ) {
            if ( data.isEmptyValue( dataName ) ) {
              return true;
            }
          } else {
            if ( dataValue == null ) {
              return true;
            }
          }
        }
        if ( s.setter != null ) {
          // usual setter
          Object value;
          if ( data != null ) {
            value = data.getAsJavaType( dataName, s.leafClass, s.converter );
          } else {
            value = RowMetaAndData.getStringAsJavaType( dataValue, s.leafClass, s.converter );
          }
          s.setter.invoke( obj, value );
        } else if ( s.field != null ) {
          Object value;
          if ( data != null ) {
            value = data.getAsJavaType( dataName, s.leafClass, s.converter );
          } else {
            value = RowMetaAndData.getStringAsJavaType( dataValue, s.leafClass, s.converter );
          }
          switch ( s.dim ) {
            case ARRAY:
              Object existArray = data != null ? extendArray( s, obj, index + 1 ) : checkArray( s, obj, index );
              if ( existArray == null ) {
                // out of array for constant
                return false;
              }
              Array.set( existArray, index, value );
              break;
            case LIST:
              List<Object> existList = data != null ? extendList( s, obj, index + 1 ) : checkList( s, obj, index );
              if ( existList == null ) {
                // out of array for constant
                return false;
              }
              existList.set( index, value );
              break;
            case NONE:
              s.field.set( obj, value );
              break;
          }
        } else {
          throw new HopException( "No field or setter defined for " + root.getClass() );
        }
      }
    }
    return true;
  }

  private Object createObject( Class<?> clazz, Object root ) throws HopException {
    try {
      // Object can be inner of metadata class. In this case constructor will require parameter
      for ( Constructor<?> c : clazz.getConstructors() ) {
        if ( c.getParameterTypes().length == 0 ) {
          return clazz.newInstance();
        } else if ( c.getParameterTypes().length == 1 && c.getParameterTypes()[ 0 ].isAssignableFrom( info.clazz ) ) {
          return c.newInstance( root );
        }
      }
    } catch ( Throwable ex ) {
      throw new HopException( "Can't create object " + clazz, ex );
    }
    throw new HopException( "Constructor not found for " + clazz );
  }

  private Object extendArray( BeanLevelInfo s, Object obj, int newSize ) throws Exception {
    Object existArray = s.field.get( obj );
    if ( existArray == null ) {
      existArray = Array.newInstance( s.leafClass, newSize );
      s.field.set( obj, existArray );
    }
    int existSize = Array.getLength( existArray );
    if ( existSize < newSize ) {
      Object newSized = Array.newInstance( s.leafClass, newSize );
      System.arraycopy( existArray, 0, newSized, 0, existSize );
      existArray = newSized;
      s.field.set( obj, existArray );
    }

    return existArray;
  }

  private Object checkArray( BeanLevelInfo s, Object obj, int index ) throws Exception {
    Object existArray = s.field.get( obj );
    if ( existArray == null ) {
      return null;
    }
    int existSize = Array.getLength( existArray );
    return index < existSize ? existArray : null;
  }

  private List<Object> extendList( BeanLevelInfo s, Object obj, int newSize ) throws Exception {
    @SuppressWarnings( "unchecked" )
    List<Object> existList = (List<Object>) s.field.get( obj );
    if ( existList == null ) {
      existList = new ArrayList<>();
      s.field.set( obj, existList );
    }
    while ( existList.size() < newSize ) {
      existList.add( null );
    }

    return existList;
  }

  private List<Object> checkList( BeanLevelInfo s, Object obj, int index ) throws Exception {
    @SuppressWarnings( "unchecked" )
    List<Object> existList = (List<Object>) s.field.get( obj );
    if ( existList == null ) {
      return null;
    }

    return index < existList.size() ? existList : null;
  }

  public void runPostInjectionProcessing( Object object ) {
    Method[] methods = object.getClass().getDeclaredMethods();
    for ( Method m : methods ) {
      AfterInjection annotationAfterInjection = m.getAnnotation( AfterInjection.class );
      if ( annotationAfterInjection == null ) {
        // no after injection annotations
        continue;
      }
      if ( m.isSynthetic() || Modifier.isStatic( m.getModifiers() ) ) {
        // method is static
        throw new RuntimeException( "Wrong modifier for annotated method " + m );
      }
      try {
        m.invoke( object );
      } catch ( Exception e ) {
        throw new RuntimeException( "Can not invoke after injection method " + m, e );
      }
    }
  }
}
