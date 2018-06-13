/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.lineage.field.codec.OperationTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test for {@link FieldLineageInfo}
 */
public class FieldLineageInfoTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  @Test
  public void testInvalidGraph() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "body")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(write);

    try {
      // Create graph without read operation
      FieldLineageInfo graph = new FieldLineageInfo(operations);
      Assert.fail("Graph creation should fail since no read operation is specified.");
    } catch (IllegalArgumentException e) {
      String msg = "Field level lineage requires at least one operation of type 'READ'.";
      Assert.assertEquals(e.getMessage(), msg);
    }

    operations.clear();

    operations.add(read);
    operations.add(parse);

    try {
      // Create graph without write operation
      FieldLineageInfo graph = new FieldLineageInfo(operations);
      Assert.fail("Graph creation should fail since no write operation is specified.");
    } catch (IllegalArgumentException e) {
      String msg = "Field level lineage requires at least one operation of type 'WRITE'.";
      Assert.assertEquals(e.getMessage(), msg);
    }


    WriteOperation duplicateWrite = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint3"),
                                                       Arrays.asList(InputField.of("read", "offset"),
                                                                    InputField.of("parse", "name"),
                                                                    InputField.of("parse", "body")));

    operations.add(write);
    operations.add(duplicateWrite);

    try {
      // Create graph with non-unique operation names
      FieldLineageInfo graph = new FieldLineageInfo(operations);
      Assert.fail("Graph creation should fail since operation name 'write' is repeated.");
    } catch (IllegalArgumentException e) {
      String msg = "Operation name 'write' is repeated";
      Assert.assertTrue(e.getMessage().contains(msg));
    }

    operations.clear();

    TransformOperation invalidOrigin = new TransformOperation("anotherparse", "parse body",
                                                              Arrays.asList(InputField.of("invalid", "body"),
                                                                            InputField.of("anotherinvalid", "body")),
                                                              "name", "address");

    operations.add(read);
    operations.add(parse);
    operations.add(write);
    operations.add(invalidOrigin);

    try {
      // Create graph without invalid origins
      FieldLineageInfo graph = new FieldLineageInfo(operations);
      Assert.fail("Graph creation should fail since operation with name 'invalid' and 'anotherinvalid' do not exist.");
    } catch (IllegalArgumentException e) {
      String msg = "No operation is associated with the origins '[invalid, anotherinvalid]'.";
      Assert.assertEquals(e.getMessage(), msg);
    }
  }

  @Test
  public void testValidGraph() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Arrays.asList(InputField.of("read", "body")), "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "body")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(write);
    operations.add(parse);
    FieldLineageInfo graph1 = new FieldLineageInfo(operations);

    // Serializing and deserializing the graph should result in the same checksum.
    String operationsJson = GSON.toJson(graph1.getOperations());
    Type myType = new TypeToken<HashSet<Operation>>() { }.getType();
    Set<Operation> deserializedOperations = GSON.fromJson(operationsJson, myType);
    FieldLineageInfo graph2 = new FieldLineageInfo(deserializedOperations);
    Assert.assertEquals(graph1.getChecksum(), graph2.getChecksum());

    // Create graph with different ordering of same operations. Checksum should still be same.
    operations.clear();
    operations.add(write);
    operations.add(parse);
    operations.add(read);

    FieldLineageInfo graph3 = new FieldLineageInfo(operations);
    Assert.assertEquals(graph1.getChecksum(), graph3.getChecksum());

    // Change the namespace name of the write operation from ns to myns. The checksum should change now.
    operations.clear();

    WriteOperation anotherWrite = new WriteOperation("write", "write data", EndPoint.of("myns", "endpoint2"),
                                                     Arrays.asList(InputField.of("read", "offset"),
                                                                   InputField.of("parse", "name"),
                                                                   InputField.of("parse", "body")));
    operations.add(anotherWrite);
    operations.add(parse);
    operations.add(read);
    FieldLineageInfo graph4 = new FieldLineageInfo(operations);
    Assert.assertNotEquals(graph1.getChecksum(), graph4.getChecksum());
  }

  @Test
  public void testSimpleFieldLineageSummary() {
    // read: file -> (offset, body)
    // parse: (body) -> (first_name, last_name)
    // concat: (first_name, last_name) -> (name)
    // write: (offset, name) -> another_file

    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "first_name", "last_name");

    TransformOperation concat = new TransformOperation("concat", "concatinating the fields",
                                                       Arrays.asList(InputField.of("parse", "first_name"),
                                                                     InputField.of("parse", "last_name")), "name");

    WriteOperation write = new WriteOperation("write_op", "writing data to file",
                                              EndPoint.of("myns", "another_file"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("concat", "name")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(concat);
    operations.add(read);
    operations.add(write);

    FieldLineageInfo info = new FieldLineageInfo(operations);

    // EndPoint(myns, another_file) should have two fields: offset and name
    Map<EndPoint, Set<String>> destinationFields = info.getDestinationFields();
    EndPoint destination = EndPoint.of("myns", "another_file");
    Assert.assertEquals(1, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("offset", "name")), destinationFields.get(destination));

    Map<EndPointField, Set<EndPointField>> incomingSummary = info.getIncomingSummary();
    Map<EndPointField, Set<EndPointField>> outgoingSummary = info.getOutgoingSummary();

    // test incoming summaries

    // offset in the destination is generated from offset field read from source
    EndPointField endPointField = new EndPointField(destination, "offset");
    Set<EndPointField> sourceEndPointFields = incomingSummary.get(endPointField);
    Assert.assertEquals(1, sourceEndPointFields.size());
    EndPointField expectedEndPointField = new EndPointField(EndPoint.of("endpoint1"), "offset");
    Assert.assertEquals(expectedEndPointField, sourceEndPointFields.iterator().next());

    // name in the destination is generated from body field read from source
    endPointField = new EndPointField(destination, "name");
    sourceEndPointFields = incomingSummary.get(endPointField);
    Assert.assertEquals(1, sourceEndPointFields.size());
    expectedEndPointField = new EndPointField(EndPoint.of("endpoint1"), "body");
    Assert.assertEquals(expectedEndPointField, sourceEndPointFields.iterator().next());

    // test outgoing summaries

    // offset in the source should only affect the field offset in the destination
    EndPoint source = EndPoint.of("endpoint1");
    endPointField = new EndPointField(source, "offset");
    Set<EndPointField> destinationEndPointFields = outgoingSummary.get(endPointField);
    Assert.assertEquals(1, destinationEndPointFields.size());
    expectedEndPointField = new EndPointField(EndPoint.of("myns", "another_file"), "offset");
    Assert.assertEquals(expectedEndPointField, destinationEndPointFields.iterator().next());

    // body in the source should only affect the field name in the destination
    endPointField = new EndPointField(source, "body");
    destinationEndPointFields = outgoingSummary.get(endPointField);
    Assert.assertEquals(1, destinationEndPointFields.size());
    expectedEndPointField = new EndPointField(EndPoint.of("myns", "another_file"), "name");
    Assert.assertEquals(expectedEndPointField, destinationEndPointFields.iterator().next());
  }

  @Test
  public void testMultiPathFieldLineage() {
    // read1: file1 -> (offset, body)
    // read2: file2 -> (offset, body)
    // merge: (read1.offset, read1.body, read2.offset, read2.body) -> (offset, body)
    // parse: (merge.body) -> (name,address)
    // write: (parse.name, parse.address, merge.offset) -> file

    ReadOperation read1 = new ReadOperation("read1", "Reading from file1", EndPoint.of("ns1", "file1"),
                                            "offset", "body");

    ReadOperation read2 = new ReadOperation("read2", "Reading from file2", EndPoint.of("ns2", "file2"),
                                            "offset", "body");

    TransformOperation merge = new TransformOperation("merge", "merging fields",
                                                      Arrays.asList(InputField.of("read1", "offset"),
                                                                    InputField.of("read2", "offset"),
                                                                    InputField.of("read1", "body"),
                                                                    InputField.of("read2", "body")), "offset", "body");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("merge", "body")),
                                                      "name", "address");

    WriteOperation write = new WriteOperation("write", "writing to another file", EndPoint.of("ns3", "file1"),
                                              Arrays.asList(InputField.of("merge", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(merge);
    operations.add(read1);
    operations.add(read2);
    operations.add(write);

    FieldLineageInfo info = new FieldLineageInfo(operations);
  }

  @Test
  public void testMultiPathOutputFieldLineage() {
    // read1: file1 -> (offset, body)
    // read2: file2 -> (offset, body)
    // merge1: (read1.offset, read1.body, read2.offset, read2.body) -> (offset, body)
    // parse1: (merge.body) -> (name, address)
    // read3: file3 -> (offset, body)
    // parse2: (read3.body) -> (name, address)
    // merge2: (parse1.name, parse1.address, parse2.name, parse2.address) -> (name, address)
    // write1: (merge2.name, merge2.address, merge.offset) -> file
    // write2: (parse2.name, parse2.address) -> another_file

    ReadOperation read1 = new ReadOperation("read1", "Reading from file1", EndPoint.of("ns1", "file1"),
                                            "offset", "body");

    ReadOperation read2 = new ReadOperation("read2", "Reading from file2", EndPoint.of("ns2", "file2"),
                                            "offset", "body");

    TransformOperation merge1 = new TransformOperation("merge1", "merging fields",
                                                       Arrays.asList(InputField.of("read1", "offset"),
                                                                     InputField.of("read2", "offset"),
                                                                     InputField.of("read1", "body"),
                                                                     InputField.of("read2", "body")), "offset", "body");

    TransformOperation parse1 = new TransformOperation("parse1", "parsing merged fields between file1 and file2",
                                                       Collections.singletonList(InputField.of("merge1", "body")),
                                                       "name", "address");

    ReadOperation read3 = new ReadOperation("read3", "Reading from file3", EndPoint.of("ns3", "file3"),
                                            "offset", "body");

    TransformOperation parse2 = new TransformOperation("parse2", "parsing body",
                                                       Collections.singletonList(InputField.of("read3", "body")),
                                                       "name", "address");

    TransformOperation merge2 = new TransformOperation("merge2", "merging parsed fields",
                                                       Arrays.asList(InputField.of("parse1", "name"),
                                                                     InputField.of("parse2", "name"),
                                                                     InputField.of("parse1", "address"),
                                                                     InputField.of("parse2", "address")),
                                                       "offset", "body");

    WriteOperation write1 = new WriteOperation("write1", "writing to file", EndPoint.of("ns1", "another_file"),
                                               Arrays.asList(InputField.of("merge2", "name"),
                                                             InputField.of("merge2", "address"),
                                                             InputField.of("merge1", "offset")));

    WriteOperation write2 = new WriteOperation("write2", "writing to file", EndPoint.of("ns2", "another_file"),
                                               Arrays.asList(InputField.of("parse2", "name"),
                                                             InputField.of("parse2", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read1);
    operations.add(read2);
    operations.add(read3);
    operations.add(merge1);
    operations.add(merge2);
    operations.add(parse1);
    operations.add(parse2);
    operations.add(write1);
    operations.add(write2);

    FieldLineageInfo info = new FieldLineageInfo(operations);
  }
}
