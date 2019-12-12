package com.didichuxing.hive.udf;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapToArrayUDFTest
{
  @Test
  public void testNoExclude() throws HiveException
  {
    MapToArrayUDF example = new MapToArrayUDF();
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(stringOI, stringOI);
    ListObjectInspector resultInspector = (ListObjectInspector) example.initialize(new ObjectInspector[]{mapOI});

    // create the actual UDF arguments
    Map<String, String> map = new HashMap<>();
    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");
    map.put("d", "4");

    Object result = example.evaluate(new DeferredObject[]{new DeferredJavaObject(map)});
    assertEquals(Lists.newArrayList("a=1", "b=2", "c=3", "d=4"), resultInspector.getList(result));
  }

  @Test
  public void testNonConstantValues() throws HiveException
  {
    MapToArrayUDF example = new MapToArrayUDF();
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(stringOI, stringOI);
    ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
    ListObjectInspector resultInspector = (ListObjectInspector) example.initialize(new ObjectInspector[]{
        mapOI, listOI
    });

    // create the actual UDF arguments
    Map<String, String> map = new HashMap<>();
    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");
    map.put("d", "4");
    List<String> excludedKeys = new ArrayList<>();
    excludedKeys.add("a");
    excludedKeys.add("b");

    Object result = example.evaluate(new DeferredObject[]{
        new DeferredJavaObject(map),
        new DeferredJavaObject(excludedKeys)
    });
    assertEquals(Lists.newArrayList("c=3", "d=4"), resultInspector.getList(result));
  }

  @Test
  public void testConstantValues() throws HiveException
  {
    MapToArrayUDF example = new MapToArrayUDF();
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(stringOI, stringOI);
    List<String> excludedKeys = new ArrayList<>();
    excludedKeys.add("a");
    excludedKeys.add("c");
    ObjectInspector listOI = ObjectInspectorFactory.getStandardConstantListObjectInspector(stringOI, excludedKeys);
    ListObjectInspector resultInspector = (ListObjectInspector) example.initialize(new ObjectInspector[]{
        mapOI, listOI
    });

    // create the actual UDF arguments
    Map<String, String> map = new HashMap<>();
    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");
    map.put("d", "4");

    Object result = example.evaluate(new DeferredObject[]{
        new DeferredJavaObject(map),
        null
    });
    assertEquals(Lists.newArrayList("b=2", "d=4"), resultInspector.getList(result));
  }
}
