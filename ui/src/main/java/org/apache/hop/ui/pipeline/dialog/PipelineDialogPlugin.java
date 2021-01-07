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

package org.apache.hop.ui.pipeline.dialog;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The class painted with this annotation is providing an additional user-interface in the pipeline settings dialog
 * in the form of a new tab.
 *
 * @author matt
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface PipelineDialogPlugin {
  String id();

  String name() default "";

  String description() default "";

  /**
   * Please use the i18n:package:key format in name, description and categoryDescription
   * @return
   */
  @Deprecated
  String i18nPackageName() default "";

  String classLoaderGroup() default "";
}
