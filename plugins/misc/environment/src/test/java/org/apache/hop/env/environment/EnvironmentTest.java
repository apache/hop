package org.apache.hop.env.environment;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class EnvironmentTest {

  private static final String JSON_CASE1 = "{\n  \"name\" : \"name1\",\n  \"description\" : \"description1\"\n}";
  private static final String JSON_CASE2 = "{\n  \"name\" : \"name2\",\n  \"description\" : \"description2\",\n  "
    + "\"variables\" : [ {\n    \"name\" : \"var1\",\n    \"value\" : \"val1\",\n    \"description\" : \"desc1\"\n  }, "
    + "{\n    \"name\" : \"var2\",\n    \"value\" : \"val2\",\n    \"description\" : \"desc2\"\n  }, "
    + "{\n    \"name\" : \"var3\",\n    \"value\" : \"val3\",\n    \"description\" : \"desc3\"\n  } ]\n}";

  private static final List<EnvironmentVariable> case2Variables = Arrays.asList(
    new EnvironmentVariable( "var1", "val1", "desc1" ),
    new EnvironmentVariable( "var2", "val2", "desc2" ),
    new EnvironmentVariable( "var3", "val3", "desc3" )
  );

    @Test
  public void toJsonStringCase1() throws Exception {
    Environment environment = new Environment();
    environment.setName( "name1" );
    environment.setDescription( "description1" );

    String jsonString = environment.toJsonString();
    assertEquals( JSON_CASE1, jsonString );
  }

  @Test
  public void fromJsonStringCase1() throws Exception {
    Environment environment = Environment.fromJsonString(JSON_CASE1);
    assertEquals( "name1", environment.getName() );
    assertEquals( "description1", environment.getDescription( ) );
  }

  @Test
  public void toJsonStringCase2() throws Exception {
    Environment environment = new Environment();
    environment.setName( "name2" );
    environment.setDescription( "description2" );
    environment.getVariables().addAll( case2Variables );

    String jsonString = environment.toJsonString();
    assertEquals( JSON_CASE2, jsonString );
  }

  @Test
  public void fromJsonStringCase2() throws Exception {
    Environment environment = Environment.fromJsonString(JSON_CASE2);
    assertEquals( "name2", environment.getName() );
    assertEquals( "description2", environment.getDescription( ) );
    assertEquals( case2Variables.size(), environment.getVariables().size());
    for (int v=0;v<environment.getVariables().size();v++) {
      EnvironmentVariable varC = case2Variables.get(v);
      EnvironmentVariable varE = environment.getVariables().get(v);
      assertEquals( varC.getName(), varE.getName() );
      assertEquals( varC.getValue(), varE.getValue() );
      assertEquals( varC.getDescription(), varE.getDescription() );
    }
  }
}
