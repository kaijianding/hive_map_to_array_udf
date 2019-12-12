package com.didichuxing.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Description(name = "map_to_array",
    value = "_FUNC_(map) - Returns a Array of key=value pairs contained in a Map"
)
public class MapToArrayUDF extends GenericUDF
{
  private static final String USAGE = "Usage : map_to_array(map[, list of excluded keys]) ";
  private MapObjectInspector moi;
  private ListObjectInspector loi;
  private Set<?> excludedKeySet = null;

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
  {
    if (arguments.length != 1 && arguments.length != 2) {
      throw new UDFArgumentException(USAGE);
    }
    if (!(arguments[0] instanceof MapObjectInspector)) {
      throw new UDFArgumentException(USAGE);
    }

    moi = (MapObjectInspector) arguments[0];
    if (arguments.length == 2) {
      if (!(arguments[1] instanceof ListObjectInspector)) {
        throw new UDFArgumentException(USAGE);
      }
      loi = (ListObjectInspector) arguments[1];
      if (!(loi.getListElementObjectInspector() instanceof StringObjectInspector)) {
        throw new UDFArgumentException(USAGE);
      }
      if (loi instanceof StandardConstantListObjectInspector) {
        excludedKeySet = new HashSet<>(((StandardConstantListObjectInspector) loi).getWritableConstantValue());
      }
    }

    ObjectInspector returnOi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
        PrimitiveObjectInspector.PrimitiveCategory.STRING);
    return ObjectInspectorFactory.getStandardListObjectInspector(returnOi);
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException
  {
    Map<?, ?> map = moi.getMap(arguments[0].get());
    // no excluded keys
    if (this.loi == null) {
      // array is more memory efficient
      String[] res = new String[map.size()];
      int i = 0;
      for (Map.Entry<?, ?> e : map.entrySet()) {
        res[i++] = e.getKey() + "=" + e.getValue();
      }
      return res;
    }
    Set<?> excludedKeySet = this.excludedKeySet;
    // the excluded keys are non-constant
    if (this.excludedKeySet == null) {
      excludedKeySet = new HashSet<>(loi.getList(arguments[1].get()));
    }
    List<String> res = new ArrayList<>(map.size());
    for (Map.Entry<?, ?> e : map.entrySet()) {
      if (excludedKeySet.contains(e.getKey())) {
        continue;
      }
      res.add(e.getKey() + "=" + e.getValue());
    }
    return res;
  }

  public String getDisplayString(String[] strings)
  {
    if (strings.length == 2) {
      return "map_to_array(" + strings[0] + ", " + strings[1] + ")";
    }
    return "map_to_array(" + strings[0] + ")";
  }
}
