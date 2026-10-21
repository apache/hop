/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.metadata.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method that is invoked by the metadata serializers when a property is encountered during
 * deserialization that does not match any {@link HopMetadataProperty}-annotated field.
 *
 * <p>This is the counterpart of {@link HopMetadataProperty} and is mainly used to keep reading
 * legacy or renamed keys: the annotated method receives an {@link UnknownProperty} describing the
 * key and its raw textual value, and can translate that value into a current field. It is honored
 * by both the XML and the JSON metadata serializers.
 *
 * <h2>Contract</h2>
 *
 * <ul>
 *   <li>The annotated method must be {@code public}. Discovery is based on {@link
 *       Class#getMethods()}, so private and protected methods are ignored.
 *   <li>The method must declare exactly one parameter of type {@link UnknownProperty}. Any other
 *       signature raises an {@link IllegalArgumentException}.
 *   <li>Several {@code @OnUnknownDeserializationProperty} methods may be declared; they are all
 *       invoked for each unknown property.
 *   <li>Because discovery goes through {@link Class#getMethods()}, a public override of an
 *       inherited annotated method replaces it: only the most-derived declaration is invoked, and
 *       the overridden parent method is not called.
 * </ul>
 *
 * <p>Example - a legacy boolean property replaced by an enum is read into the new field:
 *
 * <pre>{@code
 * @OnUnknownDeserializationProperty
 * public void onUnknownDeserializationProperty(UnknownProperty property) {
 *   if ("legacy_flag".equals(property.getName())) {
 *     this.value = Boolean.parseBoolean(property.getValue());
 *   }
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OnUnknownDeserializationProperty {}
