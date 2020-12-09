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

package org.apache.hop.core.util.serialization;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.ITransformMeta;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Stream.concat;
import static org.apache.hop.core.util.serialization.TransformMetaProps.TRANSFORM_TAG;

/**
 * A slim representation of ITransformMeta properties, used as a
 * way to leverage alternative serialization strategies (e.g. {@link MetaXmlSerializer}
 * <p>
 * Public methods allow conversion:
 * {@link #from(ITransformMeta)} and
 * {@link #to(ITransformMeta)}
 * <p>
 * <p>
 * Internally, the TransformMetaProps holds a list of {@link PropGroup} elements, each corresponding
 * to a MetadataInjection group.
 * <p>
 * InjectionDeep not yet supported.
 */
@XmlRootElement( name = TRANSFORM_TAG )
public class TransformMetaProps<Meta extends ITransformMeta> {

  static final String TRANSFORM_TAG = "transform-props";

  @XmlAttribute( name = "secure" )
  private List<String> secureFields;

  private Meta transformMeta;
  private IVariables variables = new Variables();

  @SuppressWarnings( "unused" )
  private TransformMetaProps() {
  }

  private TransformMetaProps( Meta smi ) {
    secureFields = sensitiveFields( smi.getClass() );
  }

  @XmlElement( name = "group" )
  private List<PropGroup> groups = new ArrayList<>();

  /**
   * Retuns an instance of this class with transformMeta properties mapped
   * to a list of {@link PropGroup}
   */
  public static <Meta extends ITransformMeta> TransformMetaProps from( Meta transformMeta ) {
    TransformMetaProps propMap = new TransformMetaProps( transformMeta );
    propMap.transformMeta = transformMeta;

    // use metadata injection to extract properties
    BeanInjectionInfo info = new BeanInjectionInfo( transformMeta.getClass() );
    BeanInjector injector = new BeanInjector( info );

    propMap.populateGroups( transformMeta, info, injector );

    return propMap;
  }

  /**
   * Sets the properties of this TransformMetaProps on {@param transformMetaInterface}
   * <p>
   * This method mutates the transformMeta, as opposed to returning a new instance, to match
   * more cleanly to Hop's {@link ITransformMeta#loadXml} design, which loads props into
   * an instance.
   */
  public Meta to( Meta meta ) {
    BeanInjectionInfo<Meta> info = new BeanInjectionInfo( meta.getClass() );

    BeanInjector<Meta> injector = new BeanInjector( info );
    info.getProperties().values().forEach( property -> assignValueForProp( property, meta, injector ) );
    return meta;
  }

  /**
   * Allows specifying a variable variables to be used when applying property values to
   * a transformMeta.
   */
  public TransformMetaProps withVariables( IVariables variables ) {
    TransformMetaProps propCopy = from( this.transformMeta );
    propCopy.variables = variables;
    return propCopy;
  }

  private void populateGroups( Meta meta, BeanInjectionInfo<Meta> info, BeanInjector injector ) {
    groups = info.getGroups().stream()  // get metadata injection groups
      .flatMap( group -> group.getGroupProperties().stream() ) // expand to all properties
      .map( getProp( meta, injector ) ) // map to  property/value
      .collect( groupingBy( Prop::getGroup ) ).entrySet().stream()  // group by group name
      .map( entry -> new PropGroup( entry.getKey(), entry.getValue() ) ) // map the set of properties to a group
      .collect( Collectors.toList() );
  }

  /**
   * Collects the list of declared fields with the {@link Sensitive} annotation
   * for the class.  Values for these fields will be encrypted.
   * <p>
   * Checks the top level fields of clazz, and recurses into
   * {@link InjectionDeep} classes.
   */
  @VisibleForTesting
  static List<String> sensitiveFields( Class clazz ) {
    Field[] declaredFields = clazz.getDeclaredFields();

    return concat( stream( declaredFields ), recurseDeep( declaredFields ) )
      .filter( field -> field.getAnnotation( Sensitive.class ) != null )
      .filter( field -> field.getAnnotation( Injection.class ) != null )
      .map( field -> field.getAnnotation( Injection.class ).name() )
      .collect( Collectors.toList() );
  }

  private static Stream<Field> recurseDeep( Field[] topLevelFields ) {
    Stream<Field> deepInjectionFields = Stream.empty();
    if ( stream( topLevelFields ).anyMatch( isInjectionDeep() ) ) {
      deepInjectionFields = stream( topLevelFields )
        .filter( isInjectionDeep() )
        .flatMap( field -> stream( field.getType().getDeclaredFields() ) );
      List<Field> deepFields = deepInjectionFields.collect( Collectors.toList() );
      return concat( deepFields.stream(), recurseDeep( deepFields.toArray( new Field[ 0 ] ) ) );
    }
    return deepInjectionFields;
  }

  private static Predicate<Field> isInjectionDeep() {
    return field -> field.getAnnotation( InjectionDeep.class ) != null;
  }

  private Function<BeanInjectionInfo.Property, Prop> getProp( ITransformMeta transformMeta,
                                                              BeanInjector injector ) {
    return prop ->
      new Prop( prop.getName(),
        getPropVal( transformMeta, injector, prop ),
        prop.getGroupName() );
  }

  @SuppressWarnings( "unchecked" )
  private List<Object> getPropVal( ITransformMeta transformMeta, BeanInjector injector,
                                   BeanInjectionInfo.Property prop ) {
    try {
      List ret;

      Object o = injector.getPropVal( transformMeta, prop.getName() );
      if ( o instanceof List ) {
        ret = (List<Object>) o;
      } else if ( o instanceof Object[] ) {
        ret = asList( (Object[]) o );
      } else {
        ret = singletonList( o );
      }
      return maybeEncrypt( prop.getName(), ret );
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  private List<Object> maybeEncrypt( String name, List<Object> ret ) {
    if ( secureFields.contains( name ) ) {
      return ret.stream()
        .map( val ->
          ( val == null ) || ( val.toString().isEmpty() )
            ? "" : Encr.encryptPasswordIfNotUsingVariables( val.toString() ) )
        .collect( Collectors.toList() );
    }
    return ret;
  }

  private void assignValueForProp( BeanInjectionInfo.Property beanInfoProp, ITransformMeta transformMetaInterface,
                                   BeanInjector injector ) {
    List<Prop> props = groups.stream()
      .filter( group -> beanInfoProp.getGroupName().equals( group.name ) )
      .flatMap( group -> group.props.stream() )
      .filter( prop -> beanInfoProp.getName().equals( prop.name ) )
      .collect( Collectors.toList() );

    decryptVals( props );
    props.forEach( entry -> injectVal( beanInfoProp, entry, transformMetaInterface, injector ) );
  }

  private void decryptVals( List<Prop> props ) {
    props.stream()
      .filter( prop -> secureFields.contains( prop.getName() ) )
      .forEach( prop -> prop.value = prop.value.stream()
        .map( Object::toString )
        .map( Encr::decryptPasswordOptionallyEncrypted )
        .collect( Collectors.toList() ) );
  }

  private void injectVal( BeanInjectionInfo.Property beanInfoProp, Prop prop,
                          ITransformMeta transformMetaInterface,
                          BeanInjector injector ) {

    if ( prop.value == null || prop.value.size() == 0 ) {
      prop.value = singletonList( null );
    }
    try {
      injector.setProperty( transformMetaInterface,
        beanInfoProp.getName(),
        prop.value.stream()
          .map( value -> {
            RowMetaAndData rmad = new RowMetaAndData();
            rmad.addValue( new ValueMetaString( prop.getName() ), envSubs( value ) );
            return rmad;
          } ).collect( Collectors.toList() ),
        beanInfoProp.getName() );
    } catch ( HopException e ) {
      throw new RuntimeException( e );
    }
  }

  private Object envSubs( Object value ) {
    if ( value instanceof String ) {
      return variables.resolve( value.toString() );
    }
    return value;
  }

  @Override public String toString() {
    return "TransformMetaProps{" + "groups=" + groups + '}';
  }

  /**
   * Represents a named grouping of properties, corresponding to a metadata injection group.
   */
  private static class PropGroup {
    @XmlAttribute String name;

    @XmlElement( name = "property" ) List<Prop> props;

    @SuppressWarnings( "unused" )
    public PropGroup() {
    } // needed for deserialization

    PropGroup( String name, List<Prop> propList ) {
      this.name = name;
      this.props = propList;
    }

    @Override public String toString() {
      return "PropGroup{" + "name='" + name + '\'' + ", props=" + props + '}';
    }
  }

  /**
   * Represents a single property from a ITransformMeta impl.
   * Values are captured as a List<Object> to consistently handle both List properties and single items.
   */
  private static class Prop {
    @XmlAttribute String group;
    @XmlAttribute String name;

    @XmlElement( name = "value" ) List<Object> value = new ArrayList<>();

    @SuppressWarnings( "unused" )
    public Prop() {
    } // needed for deserialization

    private Prop( String name, List<Object> value, String group ) {
      this.group = group;
      this.name = name;
      this.value = value;
    }

    String getName() {
      return name;
    }


    String getGroup() {
      return group;
    }

    @Override public String toString() {
      return "\n  Prop{" + "group='" + group + '\'' + ", name='" + name + '\'' + ", value=" + value + '}';
    }
  }


}
