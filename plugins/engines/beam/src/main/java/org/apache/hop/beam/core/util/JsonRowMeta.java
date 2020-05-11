package org.apache.hop.beam.core.util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

public class JsonRowMeta {

  /**
   * Convert only the basic row metadata properties to JSON
   * Only what we need in Beam
   *
   * @param rowMeta The row to convert to JSON
   * @return
   */
  public static String toJson( IRowMeta rowMeta) {

    JSONObject jRowMeta = new JSONObject();

    JSONArray jValues = new JSONArray();
    jRowMeta.put("values", jValues);

    for (int v=0;v<rowMeta.size();v++) {
      IValueMeta valueMeta = rowMeta.getValueMeta( v );

      JSONObject jValue = new JSONObject();
      jValues.add( jValue );

      jValue.put("name", valueMeta.getName());
      jValue.put("type", valueMeta.getType());
      jValue.put("length", valueMeta.getLength());
      jValue.put("precision", valueMeta.getPrecision());
      jValue.put("conversionMask", valueMeta.getConversionMask());
    }

    return jRowMeta.toJSONString();
  }

  public static IRowMeta fromJson(String rowMetaJson) throws ParseException, HopPluginException {
    JSONParser parser = new JSONParser();
    JSONObject jRowMeta = (JSONObject) parser.parse( rowMetaJson );

    IRowMeta rowMeta = new RowMeta(  );

    JSONArray jValues = (JSONArray) jRowMeta.get("values");
    for (int v=0;v<jValues.size();v++) {
      JSONObject jValue = (JSONObject) jValues.get( v );
      String name = (String) jValue.get("name");
      long type = (long)jValue.get("type");
      long length = (long)jValue.get("length");
      long precision = (long)jValue.get("precision");
      String conversionMask = (String) jValue.get("conversionMask");
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta( name, (int)type, (int)length, (int)precision );
      valueMeta.setConversionMask( conversionMask );
      rowMeta.addValueMeta( valueMeta );
    }

    return rowMeta;

  }

}
