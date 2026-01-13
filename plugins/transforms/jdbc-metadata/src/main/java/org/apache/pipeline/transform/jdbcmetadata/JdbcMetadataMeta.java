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

package org.apache.pipeline.transform.jdbcmetadata;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "JdbcMetadata",
    image = "jdbcmetadata.svg",
    name = "i18n::JdbcMetadata.Name",
    description = "i18n::JdbcMetadata.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::JdbcMetaData.keyword",
    documentationUrl = "/pipeline/transforms/jdbcmetadata.html")
public class JdbcMetadataMeta extends BaseTransformMeta<JdbcMetadata, JdbcMetadataData> {

  public static Class<DatabaseMetaData> databaseMetaDataClass;

  private static final Map<Integer, String> OPTIONS_SCOPE = new HashMap<>();

  static {
    OPTIONS_SCOPE.put(DatabaseMetaData.bestRowSession, "bestRowSession");
    OPTIONS_SCOPE.put(DatabaseMetaData.bestRowTemporary, "bestRowTemporary");
    OPTIONS_SCOPE.put(DatabaseMetaData.bestRowTransaction, "bestRowTransaction");
  }

  public static final String CONST_TABLE_CAT = "TABLE_CAT";
  // following list of COL_ static members represent columns of metadata result sets
  private static final IValueMeta COL_TABLE_CAT = new ValueMetaString(CONST_TABLE_CAT);
  private static final IValueMeta COL_PKTABLE_CAT = new ValueMetaString("PKTABLE_CAT");
  private static final IValueMeta COL_FKTABLE_CAT = new ValueMetaString("FKTABLE_CAT");
  private static final IValueMeta COL_TABLE_CATALOG = new ValueMetaString("TABLE_CATALOG");
  private static final IValueMeta COL_TABLE_SCHEM = new ValueMetaString("TABLE_SCHEM");
  private static final IValueMeta COL_PKTABLE_SCHEM = new ValueMetaString("PKTABLE_SCHEM");
  private static final IValueMeta COL_FKTABLE_SCHEM = new ValueMetaString("FKTABLE_SCHEM");
  private static final IValueMeta COL_TABLE_NAME = new ValueMetaString("TABLE_NAME");
  private static final IValueMeta COL_PKTABLE_NAME = new ValueMetaString("PKTABLE_NAME");
  private static final IValueMeta COL_FKTABLE_NAME = new ValueMetaString("FKTABLE_NAME");
  private static final IValueMeta COL_TABLE_TYPE = new ValueMetaString("TABLE_TYPE");
  private static final IValueMeta COL_COLUMN_NAME = new ValueMetaString("COLUMN_NAME");
  private static final IValueMeta COL_PKCOLUMN_NAME = new ValueMetaString("PKCOLUMN_NAME");
  private static final IValueMeta COL_FKCOLUMN_NAME = new ValueMetaString("FKCOLUMN_NAME");
  private static final IValueMeta COL_PK_NAME = new ValueMetaString("PK_NAME");
  private static final IValueMeta COL_FK_NAME = new ValueMetaString("FK_NAME");
  private static final IValueMeta COL_KEY_SEQ = new ValueMetaInteger("KEY_SEQ");
  private static final IValueMeta COL_UPDATE_RULE = new ValueMetaInteger("UPDATE_RULE");
  private static final IValueMeta COL_DELETE_RULE = new ValueMetaInteger("DELETE_RULE");
  private static final IValueMeta COL_DEFERRABILITY = new ValueMetaInteger("DEFERRABILITY");
  private static final IValueMeta COL_TYPE_NAME = new ValueMetaString("TYPE_NAME");
  private static final IValueMeta COL_DATA_TYPE = new ValueMetaInteger("DATA_TYPE");
  private static final IValueMeta COL_PRECISION = new ValueMetaInteger("PRECISION");
  private static final IValueMeta COL_COLUMN_SIZE = new ValueMetaInteger("COLUMN_SIZE");
  private static final IValueMeta COL_BUFFER_LENGTH = new ValueMetaString("BUFFER_LENGTH");
  private static final IValueMeta COL_LITERAL_PREFIX = new ValueMetaString("LITERAL_PREFIX");
  private static final IValueMeta COL_LITERAL_SUFFIX = new ValueMetaString("LITERAL_SUFFIX");
  private static final IValueMeta COL_CREATE_PARAMS = new ValueMetaString("CREATE_PARAMS");
  private static final IValueMeta COL_NULLABLE = new ValueMetaInteger("NULLABLE");
  private static final IValueMeta COL_CASE_SENSITIVE = new ValueMetaBoolean("CASE_SENSITIVE");
  private static final IValueMeta COL_SEARCHABLE = new ValueMetaInteger("SEARCHABLE");
  private static final IValueMeta COL_UNSIGNED_ATTRIBUTE =
      new ValueMetaBoolean("UNSIGNED_ATTRIBUTE");
  private static final IValueMeta COL_FIXED_PREC_SCALE = new ValueMetaBoolean("FIXED_PREC_SCALE");
  private static final IValueMeta COL_AUTO_INCREMENT = new ValueMetaBoolean("AUTO_INCREMENT");
  private static final IValueMeta COL_LOCAL_TYPE_NAME = new ValueMetaString("LOCAL_TYPE_NAME");
  private static final IValueMeta COL_MINIMUM_SCALE = new ValueMetaInteger("MINIMUM_SCALE");
  private static final IValueMeta COL_MAXIMUM_SCALE = new ValueMetaInteger("MAXIMUM_SCALE");
  private static final IValueMeta COL_DECIMAL_DIGITS = new ValueMetaInteger("DECIMAL_DIGITS");
  private static final IValueMeta COL_SQL_DATA_TYPE = new ValueMetaInteger("SQL_DATA_TYPE");
  private static final IValueMeta COL_SQL_DATETIME_SUB = new ValueMetaInteger("SQL_DATETIME_SUB");
  private static final IValueMeta COL_SOURCE_DATA_TYPE = new ValueMetaInteger("SOURCE_DATA_TYPE");
  private static final IValueMeta COL_NUM_PREC_RADIX = new ValueMetaInteger("NUM_PREC_RADIX");
  private static final IValueMeta COL_REMARKS = new ValueMetaString("REMARKS");
  private static final IValueMeta COL_TYPE_CAT = new ValueMetaString("TYPE_CAT");
  private static final IValueMeta COL_TYPE_SCHEM = new ValueMetaString("TYPE_SCHEM");
  private static final IValueMeta COL_SELF_REFERENCING_COL_NAME =
      new ValueMetaString("SELF_REFERENCING_COL_NAME");
  private static final IValueMeta COL_REF_GENERATION = new ValueMetaString("REF_GENERATION");
  private static final IValueMeta COL_SCOPE = new ValueMetaInteger("SCOPE");
  private static final IValueMeta COL_PSEUDO_COLUMN = new ValueMetaInteger("COL_PSEUDO_COLUMN");
  private static final IValueMeta COL_GRANTOR = new ValueMetaString("GRANTOR");
  private static final IValueMeta COL_GRANTEE = new ValueMetaString("GRANTEE");
  private static final IValueMeta COL_PRIVILEGE = new ValueMetaString("PRIVILEGE");
  private static final IValueMeta COL_IS_GRANTABLE = new ValueMetaString("IS_GRANTABLE");
  private static final IValueMeta COL_COLUMN_DEF = new ValueMetaString("COLUMN_DEF");
  private static final IValueMeta COL_CHAR_OCTET_LENGTH = new ValueMetaInteger("CHAR_OCTET_LENGTH");
  private static final IValueMeta COL_ORDINAL_POSITION = new ValueMetaInteger("ORDINAL_POSITION");
  private static final IValueMeta COL_IS_NULLABLE = new ValueMetaString("IS_NULLABLE");
  private static final IValueMeta COL_SCOPE_CATALOG = new ValueMetaString("SCOPE_CATALOG");
  private static final IValueMeta COL_SCOPE_SCHEMA = new ValueMetaString("SCOPE_SCHEMA");
  private static final IValueMeta COL_SCOPE_TABLE = new ValueMetaString("SCOPE_TABLE");
  private static final IValueMeta COL_IS_AUTOINCREMENT = new ValueMetaString("IS_AUTOINCREMENT");
  private static final IValueMeta COL_IS_GENERATEDCOLUMN =
      new ValueMetaString("IS_GENERATEDCOLUMN");

  // following of argument descriptors describe arguments to metdata methods
  // 1) name of the argument
  // 2) java type of the argument.
  private static final Object[] ARG_CATALOG = new Object[] {"catalog", String.class};
  private static final Object[] ARG_SCHEMA = new Object[] {"schema", String.class};
  private static final Object[] ARG_TABLE = new Object[] {"table", String.class};
  private static final Object[] ARG_COLUMN_NAME_PATTERN =
      new Object[] {"columnNamePattern", String.class};
  private static final Object[] ARG_NULLABLE =
      new Object[] {"nullable", Boolean.class, new Object[] {}};
  private static final Object[] ARG_SCHEMA_PATTERN = new Object[] {"schemaPattern", String.class};
  private static final Object[] ARG_SCOPE = new Object[] {"scope", Integer.class, OPTIONS_SCOPE};
  private static final Object[] ARG_TABLE_TYPES = new Object[] {"tableTypes", String[].class};
  private static final Object[] ARG_TABLE_NAME_PATTERN =
      new Object[] {"tableNamePattern", String.class};
  private static final Object[] ARG_PARENT_CATALOG = new Object[] {"parentCatalog", String.class};
  private static final Object[] ARG_PARENT_SCHEMA = new Object[] {"parentSchema", String.class};
  private static final Object[] ARG_PARENT_TABLE = new Object[] {"parentTable", String.class};
  private static final Object[] ARG_FOREIGN_CATALOG = new Object[] {"foreignCatalog", String.class};
  private static final Object[] ARG_FOREIGN_SCHEMA = new Object[] {"foreignSchema", String.class};
  private static final Object[] ARG_FOREIGN_TABLE = new Object[] {"foreignTable", String.class};

  // this is a map of the methods we can get metadata from.
  // 1) name of the java.sql.DatabaseMetaData method
  // 2) array of argument descriptors
  // 3) array of return fields
  // 4) initially empty slot where the actual Method object is lazily stored.
  public static final Object[] methodDescriptors =
      new Object[] {
        new Object[] {"getCatalogs", new Object[] {}, new IValueMeta[] {COL_TABLE_CAT}, null},
        new Object[] {
          "getBestRowIdentifier",
          new Object[] {ARG_CATALOG, ARG_SCHEMA, ARG_TABLE, ARG_SCOPE, ARG_NULLABLE},
          new IValueMeta[] {
            COL_SCOPE, COL_COLUMN_NAME, COL_DATA_TYPE, COL_TYPE_NAME,
            COL_COLUMN_SIZE, COL_BUFFER_LENGTH, COL_DECIMAL_DIGITS, COL_PSEUDO_COLUMN
          },
          null
        },
        new Object[] {
          "getColumnPrivileges",
          new Object[] {ARG_CATALOG, ARG_SCHEMA, ARG_TABLE, ARG_COLUMN_NAME_PATTERN},
          new IValueMeta[] {
            COL_TABLE_CAT, COL_TABLE_SCHEM, COL_TABLE_NAME, COL_COLUMN_NAME,
            COL_GRANTOR, COL_GRANTEE, COL_PRIVILEGE, COL_IS_GRANTABLE
          },
          null
        },
        new Object[] {
          "getColumns",
          new Object[] {
            ARG_CATALOG, ARG_SCHEMA_PATTERN, ARG_TABLE_NAME_PATTERN, ARG_COLUMN_NAME_PATTERN
          },
          new IValueMeta[] {
            COL_TABLE_CAT,
            COL_TABLE_SCHEM,
            COL_TABLE_NAME,
            COL_COLUMN_NAME,
            COL_DATA_TYPE,
            COL_TYPE_NAME,
            COL_COLUMN_SIZE,
            COL_BUFFER_LENGTH,
            COL_DECIMAL_DIGITS,
            COL_NUM_PREC_RADIX,
            COL_NULLABLE,
            COL_REMARKS,
            COL_COLUMN_DEF,
            COL_SQL_DATA_TYPE,
            COL_SQL_DATETIME_SUB,
            COL_CHAR_OCTET_LENGTH,
            COL_ORDINAL_POSITION,
            COL_IS_NULLABLE,
            COL_SCOPE_CATALOG,
            COL_SCOPE_SCHEMA,
            COL_SCOPE_TABLE,
            COL_SOURCE_DATA_TYPE,
            COL_IS_AUTOINCREMENT,
            COL_IS_GENERATEDCOLUMN
          },
          null
        },
        new Object[] {
          "getCrossReference",
          new Object[] {
            ARG_PARENT_CATALOG, ARG_PARENT_SCHEMA, ARG_PARENT_TABLE,
            ARG_FOREIGN_CATALOG, ARG_FOREIGN_SCHEMA, ARG_FOREIGN_TABLE,
          },
          new IValueMeta[] {
            COL_PKTABLE_CAT,
            COL_PKTABLE_SCHEM,
            COL_PKTABLE_NAME,
            COL_PKCOLUMN_NAME,
            COL_FKTABLE_CAT,
            COL_FKTABLE_SCHEM,
            COL_FKTABLE_NAME,
            COL_FKCOLUMN_NAME,
            COL_KEY_SEQ,
            COL_UPDATE_RULE,
            COL_DELETE_RULE,
            COL_FK_NAME,
            COL_PK_NAME,
            COL_DEFERRABILITY
          },
          null
        },
        new Object[] {
          "getExportedKeys",
          new Object[] {ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
          new IValueMeta[] {
            COL_PKTABLE_CAT,
            COL_PKTABLE_SCHEM,
            COL_PKTABLE_NAME,
            COL_PKCOLUMN_NAME,
            COL_FKTABLE_CAT,
            COL_FKTABLE_SCHEM,
            COL_FKTABLE_NAME,
            COL_FKCOLUMN_NAME,
            COL_KEY_SEQ,
            COL_UPDATE_RULE,
            COL_DELETE_RULE,
            COL_FK_NAME,
            COL_PK_NAME,
            COL_DEFERRABILITY
          },
          null
        },
        new Object[] {
          "getImportedKeys",
          new Object[] {ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
          new IValueMeta[] {
            COL_PKTABLE_CAT,
            COL_PKTABLE_SCHEM,
            COL_PKTABLE_NAME,
            COL_PKCOLUMN_NAME,
            COL_FKTABLE_CAT,
            COL_FKTABLE_SCHEM,
            COL_FKTABLE_NAME,
            COL_FKCOLUMN_NAME,
            COL_KEY_SEQ,
            COL_UPDATE_RULE,
            COL_DELETE_RULE,
            COL_FK_NAME,
            COL_PK_NAME,
            COL_DEFERRABILITY
          },
          null
        },
        new Object[] {
          "getPrimaryKeys",
          new Object[] {ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
          new IValueMeta[] {
            COL_TABLE_CAT,
            COL_TABLE_SCHEM,
            COL_TABLE_NAME,
            COL_COLUMN_NAME,
            COL_KEY_SEQ,
            COL_PK_NAME
          },
          null
        },
        new Object[] {
          "getSchemas", new Object[] {}, new IValueMeta[] {COL_TABLE_SCHEM, COL_TABLE_CATALOG}, null
        },
        new Object[] {
          "getTablePrivileges",
          new Object[] {ARG_CATALOG, ARG_SCHEMA_PATTERN, ARG_TABLE_NAME_PATTERN},
          new IValueMeta[] {
            COL_TABLE_CAT,
            COL_TABLE_SCHEM,
            COL_TABLE_NAME,
            COL_GRANTOR,
            COL_GRANTEE,
            COL_PRIVILEGE,
            COL_IS_GRANTABLE
          },
          null
        },
        new Object[] {"getTableTypes", new Object[] {}, new IValueMeta[] {COL_TABLE_TYPE}, null},
        new Object[] {
          "getTables",
          new Object[] {ARG_CATALOG, ARG_SCHEMA_PATTERN, ARG_TABLE_NAME_PATTERN, ARG_TABLE_TYPES},
          new IValueMeta[] {
            COL_TABLE_CAT,
            COL_TABLE_SCHEM,
            COL_TABLE_NAME,
            COL_TABLE_TYPE,
            COL_REMARKS,
            COL_TYPE_CAT,
            COL_TYPE_SCHEM,
            COL_TYPE_NAME,
            COL_SELF_REFERENCING_COL_NAME,
            COL_REF_GENERATION
          },
          null
        },
        new Object[] {
          "getTypeInfo",
          new Object[] {},
          new IValueMeta[] {
            COL_TYPE_NAME,
            COL_DATA_TYPE,
            COL_PRECISION,
            COL_LITERAL_PREFIX,
            COL_LITERAL_SUFFIX,
            COL_CREATE_PARAMS,
            COL_NULLABLE,
            COL_CASE_SENSITIVE,
            COL_SEARCHABLE,
            COL_UNSIGNED_ATTRIBUTE,
            COL_FIXED_PREC_SCALE,
            COL_AUTO_INCREMENT,
            COL_LOCAL_TYPE_NAME,
            COL_MINIMUM_SCALE,
            COL_MAXIMUM_SCALE,
            COL_SQL_DATA_TYPE,
            COL_SQL_DATETIME_SUB,
            COL_NUM_PREC_RADIX
          },
          null
        },
        new Object[] {
          "getVersionColumns",
          new Object[] {ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
          new IValueMeta[] {
            COL_SCOPE, COL_COLUMN_NAME, COL_DATA_TYPE, COL_TYPE_NAME,
            COL_COLUMN_SIZE, COL_BUFFER_LENGTH, COL_DECIMAL_DIGITS, COL_PSEUDO_COLUMN
          },
          null
        }
      };

  /**
   * Constructor should call super() to make sure the base class has a chance to initialize
   * properly.
   */
  public JdbcMetadataMeta() {
    super();
    if (JdbcMetadataMeta.databaseMetaDataClass == null) {
      try {
        JdbcMetadataMeta.databaseMetaDataClass =
            (Class<DatabaseMetaData>) Class.forName("java.sql.DatabaseMetaData");
      } catch (Exception exception) {
        throw new IllegalArgumentException(exception);
      }
    }
  }

  @Override
  public void setDefault() {
    methodName = "getCatalogs";
    argumentSourceFields = false;
    outputFields = new ArrayList<>();
    outputFields.add(new OutputField(CONST_TABLE_CAT, CONST_TABLE_CAT));
  }

  @HopMetadataProperty private String connection;

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /** Stores the whether the input row should be returned even if no metadata was found */
  @HopMetadataProperty private boolean alwaysPassInputRow;

  /**
   * @return whether fields are to be used as arguments
   */
  public boolean isAlwaysPassInputRow() {
    return alwaysPassInputRow;
  }

  /**
   * @param alwaysPassInputRow whether fields should be used as arguments
   */
  public void setAlwaysPassInputRow(boolean alwaysPassInputRow) {
    this.alwaysPassInputRow = alwaysPassInputRow;
  }

  /** Stores the name of the method used to get the metadata */
  @HopMetadataProperty private String methodName;

  /**
   * Getter for the name method used to get metadata
   *
   * @return the name of the method used to get metadata
   */
  public String getMethodName() {
    return methodName;
  }

  /**
   * Setter for the name of the method used to get metadata
   *
   * @param methodName the name of the method used to get metadata
   */
  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  private static Object[] getMethodDescriptor(String methodName) {
    for (Object o : JdbcMetadataMeta.methodDescriptors) {
      Object[] oo = (Object[]) o;
      if (!oo[0].toString().equals(methodName)) continue;
      return oo;
    }
    return null;
  }

  public static Object[] getMethodDescriptor(int index) {
    return (Object[]) JdbcMetadataMeta.methodDescriptors[index];
  }

  public Object[] getMethodDescriptor() {
    return JdbcMetadataMeta.getMethodDescriptor(getMethodName());
  }

  public IValueMeta[] getMethodResultSetDescriptor() {
    Object[] methodDescriptor = getMethodDescriptor();
    return (IValueMeta[]) methodDescriptor[2];
  }

  public static int getMethodDescriptorIndex(String methodName) {
    Object[] methods = JdbcMetadataMeta.methodDescriptors;
    int n = methods.length;
    Object[] methodDescriptor;
    String name;
    for (int i = 0; i < n; i++) {
      methodDescriptor = (Object[]) methods[i];
      name = (String) methodDescriptor[0];
      if (name.equals(methodName)) return i;
    }
    return -1;
  }

  public static String getMethodName(int methodDescriptorIndex) {
    Object[] methodDescriptor =
        (Object[]) JdbcMetadataMeta.methodDescriptors[methodDescriptorIndex];
    return (String) methodDescriptor[0];
  }

  public static Class<?>[] getMethodParameterTypes(Object[] methodDescriptor) {
    Object[] parameters = (Object[]) methodDescriptor[1];
    Object[] parameter;
    int n = parameters.length;
    Class<?>[] parameterTypes = new Class<?>[n];
    for (int i = 0; i < n; i++) {
      parameter = (Object[]) parameters[i];
      parameterTypes[i] = (Class<?>) parameter[1];
    }
    return parameterTypes;
  }

  public static Method getMethod(String methodName) throws Exception {
    Method method;
    Object[] methodDescriptor = getMethodDescriptor(methodName);
    method = (Method) methodDescriptor[3];
    if (method != null) return method;
    Class<?> dbmd = Class.forName("java.sql.DatabaseMetaData");
    Class<?>[] parameterTypes = getMethodParameterTypes(methodDescriptor);
    method = dbmd.getDeclaredMethod(methodName, parameterTypes);
    methodDescriptor[3] = method;
    return method;
  }

  public Method getMethod() throws Exception {
    return JdbcMetadataMeta.getMethod(methodName);
  }

  /** Stores the whether fields are to be used for method arguments */
  @HopMetadataProperty private boolean argumentSourceFields;

  /**
   * @return whether fields are to be used as arguments
   */
  public boolean isArgumentSourceFields() {
    return argumentSourceFields;
  }

  /**
   * @param argumentSourceFields whether fields should be used as arguments
   */
  public void setArgumentSourceFields(boolean argumentSourceFields) {
    this.argumentSourceFields = argumentSourceFields;
  }

  /** Stores the whether to remove the fields used as arguments from the output row */
  @HopMetadataProperty private boolean removeArgumentFields;

  /**
   * @return whether to remove the fields used as arguments from the output row
   */
  public boolean isRemoveArgumentFields() {
    return removeArgumentFields;
  }

  /**
   * @param removeArgumentFields whether fields used as arguments should be removed from the output
   *     row
   */
  public void setRemoveArgumentFields(boolean removeArgumentFields) {
    this.removeArgumentFields = removeArgumentFields;
  }

  /** Stores method arguments */
  @HopMetadataProperty(groupKey = "arguments", key = "argument")
  private List<String> arguments;

  /**
   * @return get method arguments
   */
  public List<String> getArguments() {
    return arguments;
  }

  /**
   * @param arguments whether fields should be used as arguments
   */
  public void setArguments(List<String> arguments) {
    this.arguments = arguments;
  }

  /** Stores the selection of fields that are added to the stream */
  @HopMetadataProperty(groupKey = "outputFields", key = "outputField")
  private List<OutputField> outputFields;

  /**
   * @return the selection of fields added to the stream
   */
  public List<OutputField> getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields whether fields should be used as arguments
   */
  public void setOutputFields(List<OutputField> outputFields) {
    this.outputFields = outputFields;
  }

  /**
   * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
   * step meta object. Be sure to create proper deep copies if the step configuration is stored in
   * modifiable objects.
   *
   * <p>See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on
   * creating a deep copy.
   *
   * @return a deep copy of this
   */
  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // remove argument source fields coming from the input
    if (argumentSourceFields && removeArgumentFields) {
      for (String argument : arguments) {
        try {
          rowMeta.removeValueMeta(argument);
        } catch (HopValueException ex) {
          // this probably means the requested field could not be found.
          // we can't really handle this here; however, it's not a problem
          // because a missing field will be detected before writing rows.
        }
      }
    }

    // add the outputfields added by this step.
    List<OutputField> outputFields = getOutputFields();
    OutputField outputField;
    int n = outputFields.size();

    Object[] methodDescriptor = getMethodDescriptor();
    IValueMeta[] fields = (IValueMeta[]) methodDescriptor[2];
    int m = fields.length;
    IValueMeta field;
    String fieldName;

    for (int i = 0; i < n; i++) {
      outputField = outputFields.get(i);
      for (IValueMeta iValueMeta : fields) {
        field = iValueMeta;
        fieldName = field.getName();
        if (!fieldName.equals(outputField.getName())) {
          continue;
        }
        field =
            new ValueMetaBase(
                outputField.getRename() == null ? fieldName : outputField.getRename(),
                field.getType());
        field.setOrigin(origin);
        rowMeta.addValueMeta(field);
        break;
      }
    }
  }
}
