package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings( "WeakerAccess" )
public class SnowflakeBulkLoaderData extends BaseTransformData implements ITransformData {


    // When the meta.splitSize is exceeded the file being written is closed and a new file is created.  These new files
    // are called splits.  Every time a new file is created this is incremented so it will contain the latest split number
    public int splitnr;

    // Maps table fields to the location of the corresponding field on the input stream.
    public Map<String, Integer> fieldnrs;

    // The database being used
    public Database db;
    public DatabaseMeta databaseMeta;

    // A list of table fields mapped to their data type.  String[0] is the field name, String[1] is the Snowflake
    // data type
    public ArrayList<String[]> dbFields;

    // The number of rows output to temp files.  Incremented every time a new row is written.
    public int outputCount;


    // The output stream being used to write files
    public CompressionOutputStream out;

    public OutputStream writer;

    public OutputStream fos;

    // The metadata about the output row
    public IRowMeta outputRowMeta;

    // Byte arrays for constant characters put into output files.
    public byte[] binarySeparator;
    public byte[] binaryEnclosure;
    public byte[] escapeCharacters;
    public byte[] binaryNewline;

    public byte[] binaryNullValue;

    // Indicates that at least one file has been opened by the step
    public boolean oneFileOpened;

    // A list of files that have been previous created by the step
    public List<String> previouslyOpenedFiles;

    /**
     * Sets the default values
     */
    public SnowflakeBulkLoaderData() {
        super();

        previouslyOpenedFiles = new ArrayList<>();

        oneFileOpened = false;
        outputCount = 0;

        dbFields = null;
        db = null;
    }

    List<String> getPreviouslyOpenedFiles() {
        return previouslyOpenedFiles;
    }
}
