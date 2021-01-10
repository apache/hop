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
package org.apache.hop.pipeline.transforms.ldapoutput;

import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.ldapinput.LdapConnection;

/**
 * @author Samatar Hassan
 * @since 21-09-2007
 */
public class LdapOutputData extends BaseTransformData implements ITransformData {
  LdapConnection connection;
  int indexOfDNField;
  int[] fieldStream;
  String[] fieldsAttribute;
  int nrFields;
  int nrFieldsToUpdate;
  String separator;
  String[] attributes;
  String[] attributesToUpdate;

  int[] fieldStreamToUpdate;
  String[] fieldsAttributeToUpdate;

  int indexOfOldDNField;
  int indexOfNewDNField;

  public LdapOutputData() {
    super();
    this.indexOfDNField = -1;
    this.nrFields = 0;
    this.separator = null;
    this.fieldStreamToUpdate = null;
    this.fieldsAttributeToUpdate = null;
    this.attributesToUpdate = null;
    this.nrFieldsToUpdate = 0;
    this.indexOfOldDNField = -1;
    this.indexOfNewDNField = -1;
  }
}
