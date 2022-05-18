package org.apache.hop.beam.core.transform;

import org.apache.hop.core.exception.HopException;
import org.junit.Test;

import static org.apache.hop.beam.core.transform.BeamBQOutputTransform.validateBQFieldName;
import static org.junit.Assert.*;

public class BeamBQOutputTransformTest {

    @Test
    public void testValidateBQFieldName() throws Exception {
        validateBQFieldName("_name1");
        validateBQFieldName("name2");
        validateBQFieldName("name_3");

        needsException(null, "no Exception thrown: BQ field name can not be null");
        needsException("", "no Exception thrown: BQ field name can not be empty");
        needsException("1name", "no Exception thrown: BQ field name starting with a digit");
        needsException(" name", "no Exception thrown: BQ field name starting with a space");
        needsException("_table_name", "no Exception thrown: BQ field name can't start with _table_");
        needsException("_file_name", "no Exception thrown: BQ field name can't start with _file_");
        needsException("_partition_", "no Exception thrown: BQ field name can't start with _partition_");
        needsException("$name", "no Exception thrown: BQ field name only start with letters or underscore");
        needsException("%name", "no Exception thrown: BQ field name only start with letters or underscore");
        needsException("first name", "no Exception thrown: BQ field name only contain letters, digits or underscore");
        needsException("some-name", "no Exception thrown: BQ field name only contain letters, digits or underscore");
        needsException("last*name", "no Exception thrown: BQ field name only contain letters, digits or underscore");
    }

    public void needsException(String fieldName, String exceptionDescription) throws HopException {
        try {
            validateBQFieldName(fieldName);
            throw new HopException(exceptionDescription);
        } catch(Exception e) {
            // OK!
        }
    }
}