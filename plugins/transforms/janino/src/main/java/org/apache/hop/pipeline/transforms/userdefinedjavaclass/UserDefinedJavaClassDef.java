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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import java.security.MessageDigest;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class UserDefinedJavaClassDef implements Cloneable {
  public enum ClassType {
    NORMAL_CLASS,
    TRANSFORM_CLASS
  }

  @HopMetadataProperty(
      key = "class_type",
      injectionKey = "CLASS_TYPE",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.CLASS_TYPE")
  private ClassType classType;

  @HopMetadataProperty(
      key = "class_name",
      injectionKey = "CLASS_NAME",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.CLASS_NAME")
  private String className;

  @HopMetadataProperty(
      key = "class_source",
      injectionKey = "CLASS_SOURCE",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.CLASS_SOURCE")
  private String source;

  public UserDefinedJavaClassDef() {}

  public UserDefinedJavaClassDef(UserDefinedJavaClassDef d) {
    this();
    this.classType = d.classType;
    this.className = d.className;
    this.source = d.source;
  }

  public UserDefinedJavaClassDef(ClassType classType, String className, String source) {
    this();
    this.classType = classType;
    this.className = className;
    this.source = source;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return new UserDefinedJavaClassDef(this);
  }

  public String getChecksum() throws HopTransformException {
    String ck = this.className + this.source;
    try {
      byte[] b = MessageDigest.getInstance("MD5").digest(ck.getBytes());
      return Hex.encodeHexString(b);
    } catch (Exception ex) {
      // Can't get MD5 hashcode ?
      throw new HopTransformException("Unable to obtain checksum of UDJC - " + this.className);
    }
  }

  public String getTransformedSource() {
    StringBuilder sb = new StringBuilder(getSource());
    appendConstructor(sb);
    return sb.toString();
  }

  private static final String CONSTRUCTOR =
      "\n\npublic %s(UserDefinedJavaClass parent, UserDefinedJavaClassMeta meta, UserDefinedJavaClassData data) "
          + "throws HopTransformException { super(parent,meta,data);}";

  private void appendConstructor(StringBuilder sb) {
    sb.append(String.format(CONSTRUCTOR, className));
  }

  public boolean isTransformClass() {
    return (this.classType == ClassType.TRANSFORM_CLASS);
  }
}
