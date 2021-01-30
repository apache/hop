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

package org.apache.hop.core.plugins;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.util.Map;

/**
 * This class represents the transform plugin type.
 *
 * @author matt
 */
@PluginMainClassType( ITransformMeta.class )
@PluginAnnotationType( Transform.class )
public class TransformPluginType extends BasePluginType<Transform> {

  private static TransformPluginType transformPluginType;

  protected TransformPluginType() {
    super( Transform.class, "TRANSFORM", "Transform" );
  }

  public static TransformPluginType getInstance() {
    if ( transformPluginType == null ) {
      transformPluginType = new TransformPluginType();
    }
    return transformPluginType;
  }

  @Override
  protected String extractCategory( Transform annotation ) {
    return annotation.categoryDescription();
  }

  @Override
  protected String extractDesc( Transform annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( Transform annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( Transform annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( Transform annotation ) {
    return annotation.image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Transform annotation ) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Transform annotation ) {
    return annotation.i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Transform annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Transform annotation ) {
    return Const.getDocUrl( annotation.documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Transform annotation ) {
    return annotation.casesUrl();
  }

  @Override
  protected String extractForumUrl( Transform annotation ) {
    return annotation.forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup( Transform annotation ) {
    return annotation.classLoaderGroup();
  }

  @Override
  protected String extractSuggestion( Transform annotation ) {
    return annotation.suggestion();
  }

  @Override protected String[] extractKeywords( Transform annotation ) {
    return annotation.keywords();
  }
}
