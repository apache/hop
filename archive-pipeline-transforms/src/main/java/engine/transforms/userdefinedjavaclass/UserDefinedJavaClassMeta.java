/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.injection.NullNumberConverter;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.fieldsplitter.DataTypeConverter;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassDef.ClassType;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.w3c.dom.Node;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@InjectionSupported( localizationPrefix = "UserDefinedJavaClass.Injection.", groups = {
  "PARAMETERS", "TARGET_TRANSFORMS", "INFO_TRANSFORMS", "JAVA_CLASSES", "FIELD_INFO" } )
public class UserDefinedJavaClassMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = UserDefinedJavaClassMeta.class; // for i18n purposes, needed by Translator!!

  public enum ElementNames {
    class_type, class_name, class_source, definitions, definition, fields, field, field_name, field_type,
    field_length, field_precision, clear_result_fields,

    infoTransforms, infoTransform, info_, target_transforms, target_transform, target_,

    transform_tag, transform_name, transform_description,

    usage_parameters, usage_parameter, parameter_tag, parameter_value, parameter_description,
  }

  @InjectionDeep
  private List<FieldInfo> fields = new ArrayList<FieldInfo>();

  @InjectionDeep
  private List<UserDefinedJavaClassDef> definitions = new ArrayList<UserDefinedJavaClassDef>();
  public Class<TransformClassBase> cookedTransformClass;
  public List<Exception> cookErrors = new ArrayList<Exception>( 0 );
  private static final Cache<String, Class<?>> classCache;

  @Injection( name = "CLEAR_RESULT_FIELDS" )
  private boolean clearingResultFields;

  private boolean changed;

  @InjectionDeep
  private List<InfoTransformDefinition> infoTransformDefinitions;
  @InjectionDeep
  private List<TargetTransformDefinition> targetTransformDefinitions;
  @InjectionDeep
  private List<UsageParameter> usageParameters;

  static {
    iVariables vs = new Variables();
    vs.initializeVariablesFrom( null ); // sets up the default variables
    String maxSizeStr = vs.getVariable( UserDefinedJavaClass.HOP_DEFAULT_CLASS_CACHE_SIZE, "100" );
    int maxCacheSize = -1;
    try {
      maxCacheSize = Integer.parseInt( maxSizeStr );
    } catch ( Exception ignored ) {
      maxCacheSize = 100; // default to 100 if property not set
    }
    // Initialize Class ICache
    classCache = CacheBuilder.newBuilder().maximumSize( maxCacheSize ).build();
  }

  public static class FieldInfo implements Cloneable {
    @Injection( name = "FIELD_NAME", group = "FIELD_INFO" )
    public final String name;
    @Injection( name = "FIELD_TYPE", group = "FIELD_INFO", convertEmpty = true, converter = DataTypeConverter.class )
    public final int type;
    @Injection( name = "FIELD_LENGTH", group = "FIELD_INFO", convertEmpty = true, converter = NullNumberConverter.class )
    public final int length;
    @Injection( name = "FIELD_PRECISION", group = "FIELD_INFO", convertEmpty = true, converter = NullNumberConverter.class )
    public final int precision;

    public FieldInfo() {
      this( null, IValueMeta.TYPE_STRING, -1, -1 );
    }

    public FieldInfo( String name, int type, int length, int precision ) {
      super();
      this.name = name;
      this.type = type;
      this.length = length;
      this.precision = precision;
    }

    public Object clone() throws CloneNotSupportedException {
      return super.clone();
    }
  }

  public UserDefinedJavaClassMeta() {
    super();
    changed = true;
    infoTransformDefinitions = new ArrayList<>();
    targetTransformDefinitions = new ArrayList<>();
    usageParameters = new ArrayList<>();
  }

  @VisibleForTesting
  Class<?> cookClass( UserDefinedJavaClassDef def, ClassLoader clsloader ) throws CompileException, IOException, RuntimeException, HopTransformException {

    String checksum = def.getChecksum();
    Class<?> rtn = UserDefinedJavaClassMeta.classCache.getIfPresent( checksum );
    if ( rtn != null ) {
      return rtn;
    }

    if ( Thread.currentThread().getContextClassLoader() == null ) {
      Thread.currentThread().setContextClassLoader( this.getClass().getClassLoader() );
    }

    ClassBodyEvaluator cbe = new ClassBodyEvaluator();
    if ( clsloader == null ) {
      cbe.setParentClassLoader( Thread.currentThread().getContextClassLoader() );
    } else {
      cbe.setParentClassLoader( clsloader );
    }

    cbe.setClassName( def.getClassName() );

    StringReader sr;
    if ( def.isTransformClass() ) {
      cbe.setExtendedType( TransformClassBase.class );
      sr = new StringReader( def.getTransformedSource() );
    } else {
      sr = new StringReader( def.getSource() );
    }

    cbe.setDefaultImports( new String[] {
      "org.apache.hop.pipeline.transforms.userdefinedjavaclass.*", "org.apache.hop.pipeline.transform.*",
      "org.apache.hop.core.row.*", "org.apache.hop.core.*", "org.apache.hop.core.exception.*" } );

    cbe.cook( new Scanner( null, sr ) );
    rtn = cbe.getClazz();
    UserDefinedJavaClassMeta.classCache.put( checksum, rtn );
    return rtn;
  }

  @SuppressWarnings( "unchecked" )
  public void cookClasses() {
    cookErrors.clear();
    ClassLoader clsloader = null;
    for ( UserDefinedJavaClassDef def : getDefinitions() ) {
      if ( def.isActive() ) {
        try {
          Class<?> cookedClass = cookClass( def, clsloader );
          clsloader = cookedClass.getClassLoader();
          if ( def.isTransformClass() ) {
            cookedTransformClass = (Class<TransformClassBase>) cookedClass;
          }

        } catch ( Throwable e ) {
          CompileException exception = new CompileException( e.getMessage(), null );
          exception.setStackTrace( new StackTraceElement[] {} );
          cookErrors.add( exception );
        }
      }
    }
    changed = false;
  }

  public TransformClassBase newChildInstance( UserDefinedJavaClass parent, UserDefinedJavaClassMeta meta, UserDefinedJavaClassData data ) {
    if ( !checkClassCookings( getLog() ) ) {
      return null;
    }

    try {
      return cookedTransformClass.getConstructor( UserDefinedJavaClass.class, UserDefinedJavaClassMeta.class, UserDefinedJavaClassData.class ).newInstance( parent, meta, data );
    } catch ( Exception e ) {
      if ( log.isDebug() ) {
        log.logError( "Full debugging stacktrace of UserDefinedJavaClass instanciation exception:", e.getCause() );
      }
      HopException kettleException = new HopException( e.getMessage() );
      kettleException.setStackTrace( new StackTraceElement[] {} );
      cookErrors.add( kettleException );
      return null;
    }
  }

  public List<FieldInfo> getFieldInfo() {
    return Collections.unmodifiableList( fields );
  }

  public void setFieldInfo( List<FieldInfo> fields ) {
    replaceFields( fields );
  }

  public void replaceFields( List<FieldInfo> fields ) {
    this.fields = fields;
    changed = true;
  }

  public List<UserDefinedJavaClassDef> getDefinitions() {
    return Collections.unmodifiableList( definitions );
  }

  /**
   * This method oders the classes by sorting all the normal classes by alphabetic order and then sorting
   * all the transaction classes by alphabetical order. This makes the resolution of classes deterministic by type and
   * then by class name.
   *
   * @param definitions - Unorder list of user defined classes
   * @return - Ordered list of user defined classes
   */
  @VisibleForTesting
  protected List<UserDefinedJavaClassDef> orderDefinitions( List<UserDefinedJavaClassDef> definitions ) {
    List<UserDefinedJavaClassDef> orderedDefinitions = new ArrayList<>( definitions.size() );
    List<UserDefinedJavaClassDef> transactions =
      definitions.stream()
        .filter( def -> def.isTransformClass() && def.isActive() )
        .sorted( ( p1, p2 ) -> p1.getClassName().compareTo( p2.getClassName() ) )
        .collect( Collectors.toList() );

    List<UserDefinedJavaClassDef> normalClasses =
      definitions.stream()
        .filter( def -> !def.isTransformClass() )
        .sorted( ( p1, p2 ) -> p1.getClassName().compareTo( p2.getClassName() ) )
        .collect( Collectors.toList() );

    orderedDefinitions.addAll( normalClasses );
    orderedDefinitions.addAll( transactions );
    return orderedDefinitions;
  }

  public void replaceDefinitions( List<UserDefinedJavaClassDef> definitions ) {
    this.definitions.clear();
    this.definitions = orderDefinitions( definitions );
    changed = true;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    try {

      UserDefinedJavaClassMeta retval = (UserDefinedJavaClassMeta) super.clone();

      if ( fields != null ) {
        List<FieldInfo> newFields = new ArrayList<FieldInfo>( fields.size() );
        for ( FieldInfo field : fields ) {
          newFields.add( (FieldInfo) field.clone() );
        }
        retval.fields = newFields;
      }

      if ( definitions != null ) {
        List<UserDefinedJavaClassDef> newDefinitions = new ArrayList<UserDefinedJavaClassDef>();
        for ( UserDefinedJavaClassDef def : definitions ) {
          newDefinitions.add( (UserDefinedJavaClassDef) def.clone() );
        }
        retval.definitions = newDefinitions;
      }

      retval.cookedTransformClass = null;
      retval.cookErrors = new ArrayList<Exception>( 0 );

      if ( infoTransformDefinitions != null ) {
        List<InfoTransformDefinition> newInfoTransformDefinitions = new ArrayList<>();
        for ( InfoTransformDefinition transform : infoTransformDefinitions ) {
          newInfoTransformDefinitions.add( (InfoTransformDefinition) transform.clone() );
        }
        retval.infoTransformDefinitions = newInfoTransformDefinitions;
      }

      if ( targetTransformDefinitions != null ) {
        List<TargetTransformDefinition> newTargetTransformDefinitions = new ArrayList<>();
        for ( TargetTransformDefinition transform : targetTransformDefinitions ) {
          newTargetTransformDefinitions.add( (TargetTransformDefinition) transform.clone() );
        }
        retval.targetTransformDefinitions = newTargetTransformDefinitions;
      }

      if ( usageParameters != null ) {
        List<UsageParameter> newUsageParameters = new ArrayList<UsageParameter>();
        for ( UsageParameter param : usageParameters ) {
          newUsageParameters.add( (UsageParameter) param.clone() );
        }
        retval.usageParameters = newUsageParameters;
      }

      return retval;

    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      Node definitionsNode = XMLHandler.getSubNode( transformNode, ElementNames.definitions.name() );
      int nrDefinitions = XMLHandler.countNodes( definitionsNode, ElementNames.definition.name() );

      for ( int i = 0; i < nrDefinitions; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( definitionsNode, ElementNames.definition.name(), i );
        definitions.add( new UserDefinedJavaClassDef(
          ClassType.valueOf( XMLHandler.getTagValue( fnode, ElementNames.class_type.name() ) ),
          XMLHandler.getTagValue( fnode, ElementNames.class_name.name() ),
          XMLHandler.getTagValue( fnode, ElementNames.class_source.name() ) ) );
      }
      definitions = orderDefinitions( definitions );

      Node fieldsNode = XMLHandler.getSubNode( transformNode, ElementNames.fields.name() );
      int nrFields = XMLHandler.countNodes( fieldsNode, ElementNames.field.name() );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fieldsNode, ElementNames.field.name(), i );
        fields.add( new FieldInfo(
          XMLHandler.getTagValue( fnode, ElementNames.field_name.name() ),
          ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, ElementNames.field_type.name() ) ),
          Const.toInt( XMLHandler.getTagValue( fnode, ElementNames.field_length.name() ), -1 ),
          Const.toInt( XMLHandler.getTagValue( fnode, ElementNames.field_precision.name() ), -1 ) ) );
      }

      setClearingResultFields( !"N".equals( XMLHandler.getTagValue( transformNode, ElementNames.clear_result_fields
        .name() ) ) );

      infoTransformDefinitions.clear();
      Node infosNode = XMLHandler.getSubNode( transformNode, ElementNames.infoTransforms.name() );
      int nrInfos = XMLHandler.countNodes( infosNode, ElementNames.infoTransform.name() );
      for ( int i = 0; i < nrInfos; i++ ) {
        Node infoNode = XMLHandler.getSubNodeByNr( infosNode, ElementNames.infoTransform.name(), i );
        InfoTransformDefinition transformDefinition = new InfoTransformDefinition();
        transformDefinition.tag = XMLHandler.getTagValue( infoNode, ElementNames.transform_tag.name() );
        transformDefinition.transformName = XMLHandler.getTagValue( infoNode, ElementNames.transform_name.name() );
        transformDefinition.description = XMLHandler.getTagValue( infoNode, ElementNames.transform_description.name() );
        infoTransformDefinitions.add( transformDefinition );
      }

      targetTransformDefinitions.clear();
      Node targetsNode = XMLHandler.getSubNode( transformNode, ElementNames.target_transforms.name() );
      int nrTargets = XMLHandler.countNodes( targetsNode, ElementNames.target_transform.name() );
      for ( int i = 0; i < nrTargets; i++ ) {
        Node targetNode = XMLHandler.getSubNodeByNr( targetsNode, ElementNames.target_transform.name(), i );
        TargetTransformDefinition transformDefinition = new TargetTransformDefinition();
        transformDefinition.tag = XMLHandler.getTagValue( targetNode, ElementNames.transform_tag.name() );
        transformDefinition.transformName = XMLHandler.getTagValue( targetNode, ElementNames.transform_name.name() );
        transformDefinition.description = XMLHandler.getTagValue( targetNode, ElementNames.transform_description.name() );
        targetTransformDefinitions.add( transformDefinition );
      }

      usageParameters.clear();
      Node parametersNode = XMLHandler.getSubNode( transformNode, ElementNames.usage_parameters.name() );
      int nrParameters = XMLHandler.countNodes( parametersNode, ElementNames.usage_parameter.name() );
      for ( int i = 0; i < nrParameters; i++ ) {
        Node parameterNode = XMLHandler.getSubNodeByNr( parametersNode, ElementNames.usage_parameter.name(), i );
        UsageParameter usageParameter = new UsageParameter();
        usageParameter.tag = XMLHandler.getTagValue( parameterNode, ElementNames.parameter_tag.name() );
        usageParameter.value = XMLHandler.getTagValue( parameterNode, ElementNames.parameter_value.name() );
        usageParameter.description =
          XMLHandler.getTagValue( parameterNode, ElementNames.parameter_description.name() );
        usageParameters.add( usageParameter );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "UserDefinedJavaClassMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    // Moved the default code generation out of Meta since the Snippits class is in the UI package which isn't in the
    // classpath.
  }

  private boolean checkClassCookings( LogChannelInterface logChannel ) {
    boolean ok = cookedTransformClass != null && cookErrors.size() == 0;
    if ( changed ) {
      cookClasses();
      if ( cookedTransformClass == null ) {
        if ( cookErrors.size() > 0 ) {
          logChannel.logDebug( BaseMessages.getString(
            PKG, "UserDefinedJavaClass.Exception.CookingError", cookErrors.get( 0 ) ) );
        }
        ok = false;
      } else {
        ok = true;
      }
    }
    return ok;
  }

  @Override
  public TransformIOMetaInterface getTransformIOMeta() {
    if ( !checkClassCookings( getLog() ) ) {
      return super.getTransformIOMeta();
    }

    try {
      Method getTransformIOMeta = cookedTransformClass.getMethod( "getTransformIOMeta", UserDefinedJavaClassMeta.class );
      if ( getTransformIOMeta != null ) {
        TransformIOMetaInterface transformIoMeta = (TransformIOMetaInterface) getTransformIOMeta.invoke( null, this );
        if ( transformIoMeta == null ) {
          return super.getTransformIOMeta();
        } else {
          return transformIoMeta;
        }
      } else {
        return super.getTransformIOMeta();
      }
    } catch ( Exception e ) {
      e.printStackTrace();
      return super.getTransformIOMeta();
    }
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    for ( InfoTransformDefinition transformDefinition : infoTransformDefinitions ) {
      transformDefinition.transformMeta = TransformMeta.findTransform( transforms, transformDefinition.transformName );
    }
    for ( TargetTransformDefinition transformDefinition : targetTransformDefinitions ) {
      transformDefinition.transformMeta = TransformMeta.findTransform( transforms, transformDefinition.transformName );
    }
  }

  public void getFields( IRowMeta row, String originTransformName, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    if ( !checkClassCookings( getLog() ) ) {
      if ( cookErrors.size() > 0 ) {
        throw new HopTransformException( "Error initializing UserDefinedJavaClass to get fields: ", cookErrors
          .get( 0 ) );
      } else {
        return;
      }
    }

    try {
      Method getFieldsMethod =
        cookedTransformClass.getMethod(
          "getFields", boolean.class, IRowMeta.class, String.class, IRowMeta[].class,
          TransformMeta.class, iVariables.class, List.class );
      getFieldsMethod.invoke(
        null, isClearingResultFields(), row, originTransformName, info, nextTransform, variables, getFieldInfo() );
    } catch ( Exception e ) {
      throw new HopTransformException( "Error executing UserDefinedJavaClass.getFields(): ", e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( String.format( "\n    <%s>", ElementNames.definitions.name() ) );
    for ( UserDefinedJavaClassDef def : definitions ) {
      retval.append( String.format( "\n        <%s>", ElementNames.definition.name() ) );
      retval.append( "\n        " ).append(
        XMLHandler.addTagValue( ElementNames.class_type.name(), def.getClassType().name() ) );
      retval.append( "\n        " ).append(
        XMLHandler.addTagValue( ElementNames.class_name.name(), def.getClassName() ) );
      retval.append( "\n        " );
      retval.append( XMLHandler.addTagValue( ElementNames.class_source.name(), def.getSource() ) );
      retval.append( String.format( "\n        </%s>", ElementNames.definition.name() ) );
    }
    retval.append( String.format( "\n    </%s>", ElementNames.definitions.name() ) );

    retval.append( String.format( "\n    <%s>", ElementNames.fields.name() ) );
    for ( FieldInfo fi : fields ) {
      retval.append( String.format( "\n        <%s>", ElementNames.field.name() ) );
      retval.append( "\n        " ).append( XMLHandler.addTagValue( ElementNames.field_name.name(), fi.name ) );
      retval.append( "\n        " ).append(
        XMLHandler.addTagValue( ElementNames.field_type.name(), ValueMetaFactory.getValueMetaName( fi.type ) ) );
      retval.append( "\n        " ).append( XMLHandler.addTagValue( ElementNames.field_length.name(), fi.length ) );
      retval.append( "\n        " ).append(
        XMLHandler.addTagValue( ElementNames.field_precision.name(), fi.precision ) );
      retval.append( String.format( "\n        </%s>", ElementNames.field.name() ) );
    }
    retval.append( String.format( "\n    </%s>", ElementNames.fields.name() ) );
    retval.append( XMLHandler.addTagValue( ElementNames.clear_result_fields.name(), clearingResultFields ) );

    // Add the XML for the info transform definitions...
    //
    retval.append( XMLHandler.openTag( ElementNames.infoTransforms.name() ) );
    for ( InfoTransformDefinition transformDefinition : infoTransformDefinitions ) {
      retval.append( XMLHandler.openTag( ElementNames.infoTransform.name() ) );
      retval.append( XMLHandler.addTagValue( ElementNames.transform_tag.name(), transformDefinition.tag ) );
      retval.append( XMLHandler.addTagValue( ElementNames.transform_name.name(), transformDefinition.transformMeta != null
        ? transformDefinition.transformMeta.getName() : null ) );
      retval.append( XMLHandler.addTagValue( ElementNames.transform_description.name(), transformDefinition.description ) );
      retval.append( XMLHandler.closeTag( ElementNames.infoTransform.name() ) );
    }
    retval.append( XMLHandler.closeTag( ElementNames.infoTransforms.name() ) );

    // Add the XML for the target transform definitions...
    //
    retval.append( XMLHandler.openTag( ElementNames.target_transforms.name() ) );
    for ( TargetTransformDefinition transformDefinition : targetTransformDefinitions ) {
      retval.append( XMLHandler.openTag( ElementNames.target_transform.name() ) );
      retval.append( XMLHandler.addTagValue( ElementNames.transform_tag.name(), transformDefinition.tag ) );
      retval.append( XMLHandler.addTagValue( ElementNames.transform_name.name(), transformDefinition.transformMeta != null
        ? transformDefinition.transformMeta.getName() : null ) );
      retval.append( XMLHandler.addTagValue( ElementNames.transform_description.name(), transformDefinition.description ) );
      retval.append( XMLHandler.closeTag( ElementNames.target_transform.name() ) );
    }
    retval.append( XMLHandler.closeTag( ElementNames.target_transforms.name() ) );

    retval.append( XMLHandler.openTag( ElementNames.usage_parameters.name() ) );
    for ( UsageParameter usageParameter : usageParameters ) {
      retval.append( XMLHandler.openTag( ElementNames.usage_parameter.name() ) );
      retval.append( XMLHandler.addTagValue( ElementNames.parameter_tag.name(), usageParameter.tag ) );
      retval.append( XMLHandler.addTagValue( ElementNames.parameter_value.name(), usageParameter.value ) );
      retval.append( XMLHandler
        .addTagValue( ElementNames.parameter_description.name(), usageParameter.description ) );
      retval.append( XMLHandler.closeTag( ElementNames.usage_parameter.name() ) );
    }
    retval.append( XMLHandler.closeTag( ElementNames.usage_parameters.name() ) );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transforminfo,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "UserDefinedJavaClassMeta.CheckResult.ConnectedTransformOK2" ), transforminfo );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "UserDefinedJavaClassMeta.CheckResult.NoInputReceived" ), transforminfo );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    UserDefinedJavaClass userDefinedJavaClass =
      new UserDefinedJavaClass( transformMeta, this, data, cnr, pipelineMeta, pipeline );
    if ( pipeline.hasHaltedTransforms() ) {
      return null;
    }

    return userDefinedJavaClass;
  }

  public ITransformData getTransformData() {
    return new UserDefinedJavaClassData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * @return the clearingResultFields
   */
  public boolean isClearingResultFields() {
    return clearingResultFields;
  }

  /**
   * @param clearingResultFields the clearingResultFields to set
   */
  public void setClearingResultFields( boolean clearingResultFields ) {
    this.clearingResultFields = clearingResultFields;
  }

  /**
   * @return the infoTransformDefinitions
   */
  public List<InfoTransformDefinition> getInfoTransformDefinitions() {
    return infoTransformDefinitions;
  }

  /**
   * @param infoTransformDefinitions the infoTransformDefinitions to set
   */
  public void setInfoTransformDefinitions( List<InfoTransformDefinition> infoTransformDefinitions ) {
    this.infoTransformDefinitions = infoTransformDefinitions;
  }

  /**
   * @return the targetTransformDefinitions
   */
  public List<TargetTransformDefinition> getTargetTransformDefinitions() {
    return targetTransformDefinitions;
  }

  /**
   * @param targetTransformDefinitions the targetTransformDefinitions to set
   */
  public void setTargetTransformDefinitions( List<TargetTransformDefinition> targetTransformDefinitions ) {
    this.targetTransformDefinitions = targetTransformDefinitions;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  /**
   * @return the usageParameters
   */
  public List<UsageParameter> getUsageParameters() {
    return usageParameters;
  }

  /**
   * @param usageParameters the usageParameters to set
   */
  public void setUsageParameters( List<UsageParameter> usageParameters ) {
    this.usageParameters = usageParameters;
  }
}
