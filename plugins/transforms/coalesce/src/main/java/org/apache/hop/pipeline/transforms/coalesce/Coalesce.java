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

package org.apache.hop.pipeline.transforms.coalesce;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;

/**
 * Contains the properties of the inputs fields, target field name, target value
 * type and options.
 *
 * @author Nicolas ADMENT
 */
public class Coalesce implements Cloneable {

	private List<String> fields = new ArrayList<>();

	private int type = IValueMeta.TYPE_NONE;

	/** The target field name */
	@Injection(name = "NAME", group = "FIELDS")
	private String name;

	@Injection(name = "TYPE", group = "FIELDS")
	public void setType(final String name) {
		this.type = ValueMetaFactory.getIdForValueMeta(name);
	}

	@Injection(name = "INPUT_FIELDS", group = "FIELDS")
	public void setInputFields(final String fields) {

		this.fields = new ArrayList<>();

		if (fields != null) {
			for (String field : fields.split("\\s*,\\s*")) {
				this.addInputField(field);
			}
		}
	}

	@Injection(name = "REMOVE_INPUT_FIELDS", group = "FIELDS")
	private boolean removeFields;

	public Coalesce() {
		super();
	}

    public Coalesce(Coalesce cloned) {
      super();
      this.name = cloned.name;
      this.type = cloned.type;
      this.removeFields = cloned.removeFields;
      
      Iterator<String> iterator = cloned.fields.iterator();   
      while(iterator.hasNext())
      {
        fields.add(iterator.next());  
      }
    }
		
	@Override
	public Object clone() {
		return new Coalesce(this);
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = StringUtils.stripToNull(name);
	}

	public List<String> getInputFields() {
		return this.fields;
	}

	public String getInputField(int index) {
		return this.fields.get(index);
	}

	public void addInputField(final String field) {
		// Ignore empty field
		if (Utils.isEmpty(field))
			return;

		this.fields.add(field);
	}

	public void removeInputField(final String field) {
		this.fields.remove(field);
	}

	public void setInputFields(final List<String> fields) {

		if (fields == null)
			this.fields = new ArrayList<>();
		else
			this.fields = fields;
	}

	public int getType() {
		return this.type;
	}

	public void setType(final int type) {
		this.type = type;
	}

	private String getTypeDesc() {
		return ValueMetaFactory.getValueMetaName(this.type);
	}

	/**
	 * Remove input fields
	 * 
	 * @return
	 */
	public boolean isRemoveFields() {
		return this.removeFields;
	}

	public void setRemoveFields(boolean remove) {
		this.removeFields = remove;
	}

	@Override
	public String toString() {
		return name + ":" + getTypeDesc();
	}
}
