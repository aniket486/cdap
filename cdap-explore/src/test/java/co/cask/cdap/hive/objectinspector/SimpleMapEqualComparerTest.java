/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.cask.cdap.hive.objectinspector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SimpleMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class SimpleMapEqualComparerTest {

  public static class IntegerStringMapHolder {
    Map<Integer, String> mMap;

    public IntegerStringMapHolder() {
      mMap = new TreeMap<>();
    }
  }

  @Test
  public void testSameType() {
    // empty maps
    IntegerStringMapHolder o1 = new IntegerStringMapHolder();
    IntegerStringMapHolder o2 = new IntegerStringMapHolder();
    ObjectInspector oi1 = ObjectInspectorFactory.getReflectionObjectInspector(IntegerStringMapHolder.class);
    int rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi1, new SimpleMapEqualComparer());
    Assert.assertEquals(0, rc);

    // equal maps
    o1.mMap.put(42, "The answer to Life, Universe And Everything");
    o2.mMap.put(42, "The answer to Life, Universe And Everything");

    o1.mMap.put(1729, "A taxi cab number");
    o2.mMap.put(1729, "A taxi cab number");
    rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi1, new SimpleMapEqualComparer());
    Assert.assertEquals(0, rc);

    // unequal maps
    o2.mMap.put(1729, "Hardy-Ramanujan Number");
    rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi1, new SimpleMapEqualComparer());
    Assert.assertFalse(0 == rc);
  }

  public static class TextStringMapHolder {
    Map<Text, String> mMap;

    public TextStringMapHolder() {
      mMap = new TreeMap<>();
    }
  }

  Object serializeAndDeserialize(TextStringMapHolder o1, StructObjectInspector oi1,
      LazySimpleSerDe serde,
      LazySerDeParameters serdeParams) throws IOException, SerDeException {
    ByteStream.Output serializeStream = new ByteStream.Output();
    LazySimpleSerDe.serialize(serializeStream, o1, oi1, serdeParams
        .getSeparators(), 0, serdeParams.getNullSequence(), serdeParams
        .isEscaped(), serdeParams.getEscapeChar(), serdeParams
        .getNeedsEscape());
    Text t = new Text(serializeStream.toByteArray());
    return serde.deserialize(t);
  }

  @Test
  public void testCompatibleType() throws SerDeException, IOException {
    // empty maps
    TextStringMapHolder o1 = new TextStringMapHolder();
    StructObjectInspector oi1 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(TextStringMapHolder.class);

    LazySimpleSerDe serde = new LazySimpleSerDe();
    Configuration conf = new Configuration();
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.LIST_COLUMNS, ObjectInspectorUtils.getFieldNames(oi1));
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi1));
    LazySerDeParameters serdeParams = new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
    serde.initialize(conf, tbl);
    ObjectInspector oi2 = serde.getObjectInspector();

    Object o2 = serializeAndDeserialize(o1, oi1, serde, serdeParams);

    int rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi2, new SimpleMapEqualComparer());
    Assert.assertEquals(0, rc);

    // equal maps
    o1.mMap.put(new Text("42"), "The answer to Life, Universe And Everything");
    o1.mMap.put(new Text("1729"), "A taxi cab number");
    o2 = serializeAndDeserialize(o1, oi1, serde, serdeParams);
    rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi2, new SimpleMapEqualComparer());
    Assert.assertEquals(0, rc);

    // unequal maps
    o1.mMap.put(new Text("1729"), "Hardy-Ramanujan Number");
    rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi2, new SimpleMapEqualComparer());
    Assert.assertFalse(0 == rc);
  }

  public static class StringTextMapHolder {
    Map<String, Text> mMap;

    public StringTextMapHolder() {
      mMap = new TreeMap<>();
    }
  }

  Object serializeAndDeserialize(StringTextMapHolder o1, StructObjectInspector oi1,
      LazySimpleSerDe serde,
      LazySerDeParameters serdeParams) throws IOException, SerDeException {
    ByteStream.Output serializeStream = new ByteStream.Output();
    LazySimpleSerDe.serialize(serializeStream, o1, oi1, serdeParams
        .getSeparators(), 0, serdeParams.getNullSequence(), serdeParams
        .isEscaped(), serdeParams.getEscapeChar(), serdeParams
        .getNeedsEscape());
    Text t = new Text(serializeStream.toByteArray());
    return serde.deserialize(t);
  }

  @Test
  public void testIncompatibleType() throws SerDeException, IOException {
    // empty maps
    StringTextMapHolder o1 = new StringTextMapHolder();
    StructObjectInspector oi1 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(StringTextMapHolder.class);

    LazySimpleSerDe serde = new LazySimpleSerDe();
    Configuration conf = new Configuration();
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.LIST_COLUMNS, ObjectInspectorUtils.getFieldNames(oi1));
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi1));
    LazySerDeParameters serdeParams = new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
    serde.initialize(conf, tbl);
    ObjectInspector oi2 = serde.getObjectInspector();

    Object o2 = serializeAndDeserialize(o1, oi1, serde, serdeParams);

    int rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi2, new SimpleMapEqualComparer());
    Assert.assertEquals(0, rc);

    // equal maps
    o1.mMap.put("42", new Text("The answer to Life, Universe And Everything"));
    o1.mMap.put("1729", new Text("A taxi cab number"));
    o2 = serializeAndDeserialize(o1, oi1, serde, serdeParams);
    rc = ObjectInspectorUtils.compare(o1, oi1, o2, oi2, new SimpleMapEqualComparer());
    Assert.assertFalse(0 == rc);
  }

}
