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
 *
 */

package org.apache.hop.neo4j.transforms.cypherbuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.CreateOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.EdgeCreateOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.EdgeMatchOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.EdgeMergeOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.IOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.MatchOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.MergeOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.ReturnOperation;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CypherBuilderMetaTest {

  private IVariables variables;

  @BeforeEach
  void before() {
    variables = Variables.getADefaultVariableSpace();
  }

  @Test
  void testMergeNode1() throws Exception {
    CypherBuilderMeta meta = createCypherBuilderMeta();

    MergeOperation merge = new MergeOperation();
    merge.setLabels(List.of("Customer"));
    merge.setAlias("n");
    merge.setKeys(List.of(new Property("id", "pId")));
    merge.setProperties(
        List.of(new Property("lastName", "pLastName"), new Property("firstName", "pFirstName")));

    meta.getOperations().add(merge);

    assertEquals(
        "MERGE(n:Customer {id:{pId}} ) SET n.lastName={pLastName}, n.firstName={pFirstName} "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMergeNodeUnwind1() throws Exception {
    CypherBuilderMeta meta = createCypherBuilderMeta(true);

    MergeOperation merge = new MergeOperation();
    merge.setLabels(List.of("Customer"));
    merge.setAlias("n");
    merge.setKeys(List.of(new Property("id", "pId")));
    merge.setProperties(
        List.of(new Property("lastName", "pLastName"), new Property("firstName", "pFirstName")));
    meta.getOperations().add(merge);

    assertEquals(
        "UNWIND $rows AS row "
            + Const.CR
            + "MERGE(n:Customer {id:row.pId} ) SET n.lastName=row.pLastName, n.firstName=row.pFirstName "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testCreateNode1() throws Exception {
    CypherBuilderMeta meta = createCypherBuilderMeta();

    CreateOperation create = new CreateOperation();
    create.setLabels(List.of("Customer"));
    create.setAlias("n");
    create.setKeys(List.of(new Property("id", "pId")));
    create.setProperties(List.of(new Property("lastName", "pLastName")));
    create.setProperties(List.of(new Property("firstName", "pFirstName")));
    meta.getOperations().add(create);

    assertEquals(
        "CREATE(n:Customer {id:{pId}} ) SET n.firstName={pFirstName} " + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testCreateNodeUnwind1() throws Exception {
    CypherBuilderMeta meta = createCypherBuilderMeta(true);

    CreateOperation create = new CreateOperation();
    create.setLabels(List.of("Customer"));
    create.setAlias("n");
    create.setKeys(List.of(new Property("id", "pId")));
    create.setProperties(List.of(new Property("lastName", "pLastName")));
    create.setProperties(List.of(new Property("firstName", "pFirstName")));
    meta.getOperations().add(create);

    assertEquals(
        "UNWIND $rows AS row "
            + Const.CR
            + "CREATE(n:Customer {id:row.pId} ) SET n.firstName=row.pFirstName "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMatchNode1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());

    MatchOperation match = new MatchOperation();
    match.setLabels(List.of("Customer"));
    match.setAlias("c");
    match.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(match);

    assertEquals("MATCH(c:Customer {id:{pId}} ) " + Const.CR, meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMatchNodeUnwind1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());
    meta.setUnwindAlias("row");

    MatchOperation match = new MatchOperation();
    match.setLabels(List.of("Customer"));
    match.setAlias("c");
    match.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(match);

    assertEquals(
        "UNWIND $rows AS row " + Const.CR + "MATCH(c:Customer {id:row.pId} ) " + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMatchReturnNode1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());

    MatchOperation match = new MatchOperation();
    match.setLabels(List.of("Customer"));
    match.setAlias("c");
    match.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(match);

    ReturnOperation ret = new ReturnOperation();
    ret.setReturnValues(
        List.of(
            new ReturnValue("c", "firstName", null, null, "first_name", "String", "String"),
            new ReturnValue("c", "lastName", null, null, "last_name", "String", "String")));
    meta.getOperations().add(ret);

    assertEquals(
        "MATCH(c:Customer {id:{pId}} ) "
            + Const.CR
            + "RETURN c.firstName AS first_name, c.lastName AS last_name "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMatchEdge1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());

    MatchOperation matchCustomer = new MatchOperation();
    matchCustomer.setLabels(List.of("Customer"));
    matchCustomer.setAlias("c");
    matchCustomer.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(matchCustomer);

    MatchOperation matchLocation = new MatchOperation();
    matchLocation.setLabels(List.of("Location"));
    matchLocation.setAlias("o");
    matchLocation.setKeys(List.of(new Property("zipCode", "pZipCode")));
    meta.getOperations().add(matchLocation);

    EdgeMatchOperation edgeCreate = new EdgeMatchOperation("c", "r", "HAS_LOCATION", "o");
    meta.getOperations().add(edgeCreate);

    assertEquals(
        "MATCH(c:Customer {id:{pId}} ) "
            + Const.CR
            + "MATCH(o:Location {zipCode:{pZipCode}} ) "
            + Const.CR
            + "MATCH(c)-[r:HAS_LOCATION]->(o) "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testCreateEdge1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());

    MatchOperation matchCustomer = new MatchOperation();
    matchCustomer.setLabels(List.of("Customer"));
    matchCustomer.setAlias("c");
    matchCustomer.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(matchCustomer);

    MatchOperation matchLocation = new MatchOperation();
    matchLocation.setLabels(List.of("Location"));
    matchLocation.setAlias("o");
    matchLocation.setKeys(List.of(new Property("zipCode", "pZipCode")));
    meta.getOperations().add(matchLocation);

    EdgeCreateOperation edgeCreate = new EdgeCreateOperation("c", "r", "HAS_LOCATION", "o");
    meta.getOperations().add(edgeCreate);

    assertEquals(
        "MATCH(c:Customer {id:{pId}} ) "
            + Const.CR
            + "MATCH(o:Location {zipCode:{pZipCode}} ) "
            + Const.CR
            + "CREATE(c)-[r:HAS_LOCATION]->(o) "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMergeEdge1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());

    MatchOperation matchCustomer = new MatchOperation();
    matchCustomer.setLabels(List.of("Customer"));
    matchCustomer.setAlias("c");
    matchCustomer.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(matchCustomer);

    MatchOperation matchLocation = new MatchOperation();
    matchLocation.setLabels(List.of("Location"));
    matchLocation.setAlias("o");
    matchLocation.setKeys(List.of(new Property("zipCode", "pZipCode")));
    meta.getOperations().add(matchLocation);

    EdgeMergeOperation edgeCreate = new EdgeMergeOperation("c", "r", "HAS_LOCATION", "o");
    meta.getOperations().add(edgeCreate);

    assertEquals(
        "MATCH(c:Customer {id:{pId}} ) "
            + Const.CR
            + "MATCH(o:Location {zipCode:{pZipCode}} ) "
            + Const.CR
            + "MERGE(c)-[r:HAS_LOCATION]->(o) "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  @Test
  void testMatchEdgeReturn1() throws Exception {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setParameters(createCustomerParameters());

    MatchOperation matchCustomer = new MatchOperation();
    matchCustomer.setLabels(List.of("Customer"));
    matchCustomer.setAlias("c");
    matchCustomer.setKeys(List.of(new Property("id", "pId")));
    meta.getOperations().add(matchCustomer);

    MatchOperation matchLocation = new MatchOperation();
    matchLocation.setLabels(List.of("Location"));
    matchLocation.setAlias("o");
    matchLocation.setKeys(List.of(new Property("zipCode", "pZipCode")));
    meta.getOperations().add(matchLocation);

    EdgeMatchOperation edgeCreate = new EdgeMatchOperation("c", "r", "HAS_LOCATION", "o");
    meta.getOperations().add(edgeCreate);

    ReturnOperation ret = new ReturnOperation();
    ret.setReturnValues(
        List.of(
            new ReturnValue("c", "firstName", null, null, "first_name", "String", "String"),
            new ReturnValue("c", "lastName", null, null, "last_name", "String", "String"),
            new ReturnValue("o", "city", null, null, null, "String", "String")));
    meta.getOperations().add(ret);

    assertEquals(
        "MATCH(c:Customer {id:{pId}} ) "
            + Const.CR
            + "MATCH(o:Location {zipCode:{pZipCode}} ) "
            + Const.CR
            + "MATCH(c)-[r:HAS_LOCATION]->(o) "
            + Const.CR
            + "RETURN c.firstName AS first_name, c.lastName AS last_name, o.city "
            + Const.CR,
        meta.getCypher(variables));
    testSerialization(meta);
  }

  private CypherBuilderMeta createCypherBuilderMeta() {
    return createCypherBuilderMeta(false);
  }

  private CypherBuilderMeta createCypherBuilderMeta(boolean unwind) {
    CypherBuilderMeta meta = new CypherBuilderMeta();
    meta.setConnectionName("neo4j");
    meta.setBatchSize("1000");
    if (unwind) {
      meta.setUnwindAlias("row");
    }
    meta.setParameters(createCustomerParameters());
    return meta;
  }

  private List<Parameter> createCustomerParameters() {
    return List.of(
        new Parameter("pId", "customer_id", "String"),
        new Parameter("pLastName", "last_name", "String"),
        new Parameter("pFirstName", "first_name", "String"),
        new Parameter("pZipCode", "zip_code", "String"),
        new Parameter("pCity", "city", "String"),
        new Parameter("pState", "state", "String"));
  }

  /**
   * Serialize this metadata to XML and back and then compare all attributes.
   *
   * @param meta The metadata to test serialization for.
   * @throws Exception
   */
  private void testSerialization(CypherBuilderMeta meta) throws Exception {
    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + meta.getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    CypherBuilderMeta copy =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), TransformMeta.XML_TAG),
            CypherBuilderMeta.class,
            new MemoryMetadataProvider());

    assertEquals(meta.getXml(), copy.getXml());

    // Verify that the copy is the same...
    //
    assertEquals(meta.getConnectionName(), copy.getConnectionName());
    assertEquals(meta.getBatchSize(), copy.getBatchSize());
    assertEquals(meta.getUnwindAlias(), copy.getUnwindAlias());
    assertEquals(meta.getOperations().size(), copy.getOperations().size());
    for (int i = 0; i < meta.getOperations().size(); i++) {
      IOperation metaOperation = meta.getOperations().get(i);
      IOperation copyOperation = copy.getOperations().get(i);
      assertEquals(metaOperation, copyOperation);
    }
    assertEquals(meta.getParameters().size(), copy.getParameters().size());
    for (int i = 0; i < meta.getParameters().size(); i++) {
      Parameter metaParameter = meta.getParameters().get(i);
      Parameter copyParameter = copy.getParameters().get(i);
      assertEquals(metaParameter, copyParameter);
    }
  }
}
