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

package org.apache.hop.core.injection.bean;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Storage for bean annotations info for Metadata Injection and Load/Save.
 */
public class BeanInjectionInfo<Meta extends ITransformMeta> {
  private static ILogChannel LOG = HopLogStore.getLogChannelFactory().create( BeanInjectionInfo.class );

  protected final Class<Meta> clazz;
  private final InjectionSupported clazzAnnotation;
  private Map<String, Property> properties = new HashMap<>();
  private List<Group> groupsList = new ArrayList<>();
  /**
   * Used only for fast group search during initialize.
   */
  private Map<String, Group> groupsMap = new HashMap<>();
  private Set<String> hideProperties = new HashSet<>();

  public  static <Meta extends ITransformMeta> boolean isInjectionSupported( Class<Meta> clazz ) {
    InjectionSupported annotation = clazz.getAnnotation( InjectionSupported.class );
    return annotation != null;
  }

  public BeanInjectionInfo( Class<Meta> clazz ) {
    if (LOG.isDebug()) {
      LOG.logDebug( "Collect bean injection info for " + clazz );
    }
    try {
      this.clazz = clazz;
      clazzAnnotation = clazz.getAnnotation( InjectionSupported.class );
      if ( clazzAnnotation == null ) {
        throw new RuntimeException( "Injection not supported in " + clazz );
      }

      Map<String, Type> parameterTypesMap = new HashMap<>(  );
      TypeVariable<? extends Class<?>>[] typeParameters = clazz.getTypeParameters();
      for (TypeVariable<? extends Class<?>> typeParameter : typeParameters) {
        String parameterTypeName = typeParameter.getName();
        Type parameterType = typeParameter.getBounds()[0];
        parameterTypesMap.put(parameterTypeName, parameterType);
      }

      Group gr0 = new Group( "" );
      groupsList.add( gr0 );
      groupsMap.put( gr0.getName(), gr0 );
      for ( String group : clazzAnnotation.groups() ) {
        Group gr = new Group( group );
        groupsList.add( gr );
        groupsMap.put( gr.getName(), gr );
      }
      for ( String p : clazzAnnotation.hide() ) {
        hideProperties.add( p );
      }

      BeanLevelInfo root = new BeanLevelInfo();
      root.leafClass = clazz;
      if (parameterTypesMap.isEmpty()) {
        root.init( this );
      } else {
        root.init( this, parameterTypesMap );
      }
      properties = Collections.unmodifiableMap( properties );
      groupsList = Collections.unmodifiableList( groupsList );
      groupsMap = null;
    } catch ( Throwable ex ) {
      LOG.logError( "Error bean injection info collection for " + clazz + ": " + ex.getMessage(), ex );
      throw ex;
    }
  }

  public String getLocalizationPrefix() {
    return clazzAnnotation.localizationPrefix();
  }

  public Map<String, Property> getProperties() {
    return properties;
  }

  public List<Group> getGroups() {
    return groupsList;
  }

  protected void addInjectionProperty( Injection metaInj, BeanLevelInfo leaf ) {
    if ( StringUtils.isBlank( metaInj.name() ) ) {
      throw new RuntimeException( "Property name shouldn't be blank in the " + clazz );
    }

    String propertyName = calcPropertyName( metaInj, leaf );
    if ( properties.containsKey( propertyName ) ) {
      throw new RuntimeException( "Property '" + propertyName + "' already defined for " + clazz );
    }

    // probably hided
    if ( hideProperties.contains( propertyName ) ) {
      return;
    }

    Property prop = new Property( propertyName, metaInj.group(), leaf.createCallStack() );
    properties.put( prop.name, prop );
    Group gr = groupsMap.get( metaInj.group() );
    if ( gr == null ) {
      throw new RuntimeException( "Group '" + metaInj.group() + "' for property '" + metaInj.name() + "' is not defined " + clazz );
    }
    gr.groupProperties.add( prop );
  }

  public String getDescription( String name ) {
    String description = BaseMessages.getString( clazz, clazzAnnotation.localizationPrefix() + name );
    if ( description != null && description.startsWith( "!" ) && description.endsWith( "!" ) ) {
      Class<?> baseClass = clazz.getSuperclass();
      while ( baseClass != null ) {
        InjectionSupported baseAnnotation = (InjectionSupported) baseClass.getAnnotation( InjectionSupported.class );
        if ( baseAnnotation != null ) {
          description = BaseMessages.getString( baseClass, baseAnnotation.localizationPrefix() + name );
          if ( description != null && !description.startsWith( "!" ) && !description.endsWith( "!" ) ) {
            return description;
          }
        }
        baseClass = baseClass.getSuperclass();
      }
    }
    return description;
  }

  private String calcPropertyName( Injection metaInj, BeanLevelInfo leaf ) {
    String name = metaInj.name();
    while ( leaf != null ) {
      if ( StringUtils.isNotBlank( leaf.prefix ) ) {
        name = leaf.prefix + '.' + name;
      }
      leaf = leaf.parent;
    }
    if ( !name.equals( metaInj.name() ) && !metaInj.group().isEmpty() ) {
      // group exist with prefix
      throw new RuntimeException( "Group shouldn't be declared with prefix in " + clazz );
    }
    return name;
  }

  public class Property {
    private final String name;
    private final String groupName;
    protected final List<BeanLevelInfo> path;
    public final int pathArraysCount;

    public Property( String name, String groupName, List<BeanLevelInfo> path ) {
      this.name = name;
      this.groupName = groupName;
      this.path = path;
      int ac = 0;
      for ( BeanLevelInfo level : path ) {
        if ( level.dim != BeanLevelInfo.DIMENSION.NONE ) {
          ac++;
        }
      }
      pathArraysCount = ac;
    }

    public String getName() {
      return name;
    }

    public String getGroupName() {
      return groupName;
    }

    public String getDescription() {
      return BeanInjectionInfo.this.getDescription( name );
    }

    public Class<?> getPropertyClass() {
      return path.get( path.size() - 1 ).leafClass;
    }

    public boolean hasMatch( String filterString ) {
      if (StringUtils.isEmpty(filterString)) {
        return true;
      }
      if (getName().toUpperCase().contains( filterString.toUpperCase() )) {
        return true;
      }
      if (getDescription().toUpperCase().contains( filterString.toUpperCase() )) {
        return true;
      }
      return false;
    }
  }

  public class Group {
    private final String name;
    protected final List<Property> groupProperties = new ArrayList<>();

    public Group( String name ) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public List<Property> getGroupProperties() {
      return Collections.unmodifiableList( groupProperties );
    }

    public String getDescription() {
      return BeanInjectionInfo.this.getDescription( name );
    }

    public boolean hasMatchingProperty(String filterString) {
      // Empty string always matches
      if (StringUtils.isEmpty( filterString )) {
        return true;
      }
      // The group name also matches
      //
      if (name.toUpperCase().contains( filterString.toUpperCase() )) {
        return true;
      }
      for (Property property : groupProperties) {
        if (property.hasMatch(filterString)) {
          return true;
        }
      }
      return false;
    }
  }
}
