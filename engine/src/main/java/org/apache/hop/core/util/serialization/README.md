<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Classes in this package support serializing/deserializing TransformMetaInterface objects by leveraging 
MetadataInjection annotations, eliminating the need to write tedious metadata serialization code.

The simplest way for a step to use this strategy is to extend 
the BaseSerializingMeta class, which implements the 
  ````
  .loadXml() / .getXml() 
````

The MetaXmlSerializer implementation uses JAXB.  The output XML will have a form like:
```
    <step-props>
      <group name="some_group">
        <property name="Prop1">
          <value xsi:type="xs:string">value</value>
        </property>
        <property name="isLarge">
          <value xsi:type="xs:boolean">false</value>
        </property>
      </group>
    </step-props>
```

To serialize XML within a TransformMetaInterface implementation, the call
is:
  ` MetaXmlSerializer.serialize( TransformMetaProps.from( myMeta ) )`

For deserialization:
  ` MetaXmlSerializer.deserialize( xml ).to( myMeta )`

Repo writes/reads use

  ```
RepoSerializer
         .builder()
         .repo( repo )
         .transformMeta( transformMeta )
         .stepId( stepId )
         .transId( transId )
         .serialize()   /  .deserialize()
```

If any properties need to be encrypted, mark them with the @Sensitive annotation.


TODO:
* Support for database lists / metastore
