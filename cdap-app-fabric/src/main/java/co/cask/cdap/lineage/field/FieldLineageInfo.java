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
import co.cask.cdap.api.lineage.field.OperationType;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.lineage.field.codec.OperationTypeAdapter;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class representing the information about field lineage for a single program run.
 * Currently we store the operations associated with the field lineage and corresponding
 * checksum. Algorithm to compute checksum is same as how Avro computes the Schema fingerprint.
 * (https://issues.apache.org/jira/browse/AVRO-1006). The implementation of fingerprint
 * algorithm is taken from {@code org.apache.avro.SchemaNormalization} class. Since the checksum
 * is persisted in store, any change to the canonicalize form or fingerprint algorithm would
 * require upgrade step to update the stored checksums.
 */
public class FieldLineageInfo {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  private final Set<Operation> operations;

  // Map of EndPoint representing destination to the set of fields belonging to it.
  private transient Map<EndPoint, Set<String>> destinationFields;

  // For each (EndPoint,Field) combination from the source, we maintain the outgoing summary i.e.
  // set of (EndPoint, Field)s which were generated by it.
  private transient Map<EndPointField, Set<EndPointField>> outgoingSummary;

  // For each (EndPoint,Field) for the destination we maintain the incoming summary i.e.
  // combination of (EndPoint,Field) which were responsible for generating it.
  private transient Map<EndPointField, Set<EndPointField>> incomingSummary;

  private long checksum;

  /**
   * Create an instance of a class from supplied collection of operations.
   * Validations are performed on the collection before creating instance. All of the operations
   * must have unique names. Collection must have at least one operation of type READ and one
   * operation of type WRITE. The origins specified for the {@link InputField} are also validated
   * to make sure the operation with the corresponding name exists in the collection. However, we do not
   * validate the existence of path to the fields in the destination from sources. If no such path exists
   * then the lineage will be incomplete.
   *
   * @param operations the collection of field lineage operations
   * @throws IllegalArgumentException if validation fails
   */
  public FieldLineageInfo(Collection<? extends Operation> operations) {
    Set<String> operationNames = new HashSet<>();
    Set<String> allOrigins = new HashSet<>();
    boolean readExists = false;
    boolean writeExists = false;
    for (Operation operation : operations) {
      if (!operationNames.add(operation.getName())) {
        throw new IllegalArgumentException(String.format("All operations provided for creating field " +
                                                           "level lineage info must have unique names. " +
                                                           "Operation name '%s' is repeated.", operation.getName()));
      }
      switch (operation.getType()) {
        case READ:
          ReadOperation read = (ReadOperation) operation;
          EndPoint source = read.getSource();
          if (source == null) {
            throw new IllegalArgumentException(String.format("Source endpoint cannot be null for the read " +
                                                               "operation '%s'.", read.getName()));
          }
          readExists = true;
          break;
        case TRANSFORM:
          TransformOperation transform = (TransformOperation) operation;
          allOrigins.addAll(transform.getInputs().stream().map(InputField::getOrigin).collect(Collectors.toList()));
          break;
        case WRITE:
          WriteOperation write = (WriteOperation) operation;
          EndPoint destination = write.getDestination();
          if (destination == null) {
            throw new IllegalArgumentException(String.format("Destination endpoint cannot be null for the write " +
                                                               "operation '%s'.", write.getName()));
          }
          allOrigins.addAll(write.getInputs().stream().map(InputField::getOrigin).collect(Collectors.toList()));
          writeExists = true;
          break;
        default:
          // no-op
      }
    }

    if (!readExists) {
      throw new IllegalArgumentException("Field level lineage requires at least one operation of type 'READ'.");
    }

    if (!writeExists) {
      throw new IllegalArgumentException("Field level lineage requires at least one operation of type 'WRITE'.");
    }

    Sets.SetView<String> invalidOrigins = Sets.difference(allOrigins, operationNames);
    if (!invalidOrigins.isEmpty()) {
      throw new IllegalArgumentException(String.format("No operation is associated with the origins '%s'.",
                                                       invalidOrigins));
    }

    this.operations = new HashSet<>(operations);
    this.checksum = computeChecksum();
  }

  /**
   * @return the checksum for the operations
   */
  public long getChecksum() {
    return checksum;
  }

  /**
   * @return the operations
   */
  public Set<Operation> getOperations() {
    return operations;
  }

  public Map<EndPoint, Set<String>> getDestinationFields() {
    if (destinationFields == null) {
      destinationFields = computeDestinationFields();
    }
    return destinationFields;
  }

  public Map<EndPointField, Set<EndPointField>> getOutgoingSummary() {
    if (outgoingSummary == null) {
      outgoingSummary = computeOutgoingSummary();
    }
    return outgoingSummary;
  }

  public Map<EndPointField, Set<EndPointField>> getIncomingSummary() {
    if (incomingSummary == null) {
      incomingSummary = computeIncomingSummary();
    }
    return incomingSummary;
  }

  private long computeChecksum() {
    return fingerprint64(canonicalize().getBytes(Charsets.UTF_8));
  }

  private Map<EndPoint, Set<String>> computeDestinationFields() {
    Map<EndPoint, Set<String>> destinationFields = new HashMap<>();
    for (Operation operation : this.operations) {
      if (operation.getType() != OperationType.WRITE) {
        continue;
      }

      WriteOperation write = (WriteOperation) operation;
      Set<String> endPointFields = destinationFields.computeIfAbsent(write.getDestination(), k -> new HashSet<>());
      for (InputField field : write.getInputs()) {
        endPointFields.add(field.getName());
      }
    }
    return destinationFields;
  }

  private Map<EndPointField, Set<EndPointField>> computeOutgoingSummary() {
    Map<EndPointField, Set<EndPointField>> outgoingSummary = new HashMap<>();
    FieldLineageGraph graph = new FieldLineageGraph(this.operations);
    List<List<Node>> allPaths = graph.getAllPaths();

    for (List<Node> path : allPaths) {

      EndPointField sourceEndPointField = getSourceEndPointFieldFromPath(path);
      if (sourceEndPointField == null) {
        continue;
      }

      EndPointField destinationEndPointField = getDestinationEndPointFieldFromPath(path);
      if (destinationEndPointField == null) {
        continue;
      }

      Set<EndPointField> outgoingEndPointFields = outgoingSummary.computeIfAbsent(sourceEndPointField,
                                                                                  k -> new HashSet<>());
      outgoingEndPointFields.add(destinationEndPointField);
    }
    return outgoingSummary;
  }

  /**
   * Extract and return the source {@link EndPointField} from a given path.
   * For any complete path, first three nodes are required to create the source EndPointField.
   * First node represents the source EndPoint, second node represents the operation of type READ, and
   * third node represent the field. If source EndPointField cannot be constructed from the path,
   * possibly because the complete lineage information is not available, {@code null} is returned.
   */
  @Nullable
  private EndPointField getSourceEndPointFieldFromPath(List<Node> path) {
    if (path.size() < 3) {
      return null;
    }

    Node node = path.get(0);
    if (node.getType() != Node.Type.ENDPOINT) {
      return null;
    }

    EndPointNode endPointNode = (EndPointNode) node;
    EndPoint source = endPointNode.getEndPoint();

    node = path.get(1);
    if (node.getType() != Node.Type.OPERATION) {
      return null;
    }

    OperationNode operationNode = (OperationNode) node;
    if (operationNode.getOperation().getType() != OperationType.READ) {
      return null;
    }

    node = path.get(2);
    if (node.getType() != Node.Type.FIELD) {
      return null;
    }

    FieldNode fieldNode = (FieldNode) node;
    return new EndPointField(source, fieldNode.getName());
  }

  /**
   * Extract and return the destination {@link EndPointField} from a given path.
   * For any complete path, last three nodes are required to create the destination EndPointField.
   * Second node from last represents the field being written, first node from the last
   * represents the operation of type WRITE, and last node represents the destination EndPoint.
   * If destination EndPointField cannot be constructed from the path, possibly because the complete
   * lineage information is not available, {@code null} is returned.
   */
  @Nullable
  private EndPointField getDestinationEndPointFieldFromPath(List<Node> path) {
    if (path.size() < 3) {
      return null;
    }

    int index = path.size() - 1;
    Node node = path.get(index);
    if (node.getType() != Node.Type.ENDPOINT) {
      return null;
    }

    EndPointNode endPointNode = (EndPointNode) node;
    EndPoint destination = endPointNode.getEndPoint();

    node = path.get(index - 1);
    if (node.getType() != Node.Type.OPERATION) {
      return null;
    }

    OperationNode operationNode = (OperationNode) node;
    if (operationNode.getOperation().getType() != OperationType.WRITE) {
      return null;
    }

    node = path.get(index - 2);
    if (node.getType() != Node.Type.FIELD) {
      return null;
    }

    FieldNode fieldNode = (FieldNode) node;
    return new EndPointField(destination, fieldNode.getName());
  }

  private Map<EndPointField, Set<EndPointField>> computeIncomingSummary() {
    Map<EndPointField, Set<EndPointField>> incomingSummary = new HashMap<>();
    if (outgoingSummary == null) {
      outgoingSummary = computeOutgoingSummary();
    }

    for (Map.Entry<EndPointField, Set<EndPointField>> entry : outgoingSummary.entrySet()) {
      Set<EndPointField> values = entry.getValue();
      for (EndPointField value : values) {
        Set<EndPointField> incomingEndPointFields = incomingSummary.computeIfAbsent(value, k -> new HashSet<>());
        incomingEndPointFields.add(entry.getKey());
      }
    }
    return incomingSummary;
  }

  /**
   * Creates the canonicalize representation of the collection of operations. Canonicalize representation is
   * simply the JSON format of operations. Before creating the JSON, collection of operations is sorted based
   * on the operation name so that irrespective of the order of insertion, same set of operations always generate
   * same canonicalize form. This representation is then used for computing the checksum. So if there are any changes
   * to this representation, upgrade step would be required to update all the checksums stored in store.
   */
  private String canonicalize() {
    List<Operation> ops = new ArrayList<>(operations);
    Collections.sort(ops, new Comparator<Operation>() {
      @Override
      public int compare(Operation o1, Operation o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    return GSON.toJson(ops);
  }

  private static final long EMPTY64 = 0xc15d213aa4d7a795L;

  /**
   * The implementation of fingerprint algorithm is copied from {@code org.apache.avro.SchemaNormalization} class.
   *
   * @param data byte string for which fingerprint is to be computed
   * @return the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a byte string
   */
   private long fingerprint64(byte[] data) {
    long result = EMPTY64;
    for (byte b: data) {
      int index = (int) (result ^ b) & 0xff;
      result = (result >>> 8) ^ FP64.FP_TABLE[index];
    }
    return result;
  }

  /* An inner class ensures that FP_TABLE initialized only when needed. */
  private static class FP64 {
    private static final long[] FP_TABLE = new long[256];
    static {
      for (int i = 0; i < 256; i++) {
        long fp = i;
        for (int j = 0; j < 8; j++) {
          long mask = -(fp & 1L);
          fp = (fp >>> 1) ^ (EMPTY64 & mask);
        }
        FP_TABLE[i] = fp;
      }
    }
  }
}
