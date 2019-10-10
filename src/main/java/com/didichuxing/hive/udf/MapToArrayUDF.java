package com.didichuxing.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

@Description(name = "map_to_array",
    value = "_FUNC_(map) - Returns a Array of key=value pairs contained in a Map"
)
public class MapToArrayUDF extends GenericUDF
{
  private MapObjectInspector moi;

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
  {
    if (arguments.length != 1) {
      throw new UDFArgumentException("Usage : map_to_array(map) ");
    }
    if (!arguments[0].getCategory().equals(ObjectInspector.Category.MAP)) {
      throw new UDFArgumentException("Usage : map_to_array(map) ");
    }

    moi = (MapObjectInspector) arguments[0];
    ObjectInspector returnOi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    return ObjectInspectorFactory.getStandardListObjectInspector(returnOi);
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException
  {
    Map<?, ?> map = moi.getMap(arguments[0].get());
    String[] res = new String[map.size()];
    int i = 0;
    for (Map.Entry e : map.entrySet()) {
      res[i++] = e.getKey() + "=" + e.getValue();
    }
    return res;
  }

  public String getDisplayString(String[] strings)
  {
    return "map_to_array(" + strings[0] + ")";
  }
}
