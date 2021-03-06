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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.spark.SparkCompatReader;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.runtime.spi.SparkCompat;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.ssh.SSHContext;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Service for provisioning related operations
 */
public class ProvisioningService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningService.class);
  private final AtomicReference<ProvisionerInfo> provisionerInfo;
  private final ProvisionerProvider provisionerProvider;
  private final ProvisionerConfigProvider provisionerConfigProvider;
  private final ProvisionerNotifier provisionerNotifier;
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final SparkCompat sparkCompat;
  private ExecutorService executorService;

  @Inject
  ProvisioningService(CConfiguration cConf, ProvisionerProvider provisionerProvider,
                      ProvisionerConfigProvider provisionerConfigProvider,
                      ProvisionerNotifier provisionerNotifier, LocationFactory locationFactory,
                      DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.provisionerProvider = provisionerProvider;
    this.provisionerConfigProvider = provisionerConfigProvider;
    this.provisionerNotifier = provisionerNotifier;
    this.provisionerInfo = new AtomicReference<>(new ProvisionerInfo(new HashMap<>(), new HashMap<>()));
    this.locationFactory = locationFactory;
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   new TransactionSystemClientAdapter(txClient),
                                                                   NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.sparkCompat = SparkCompatReader.get(cConf);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());
    reloadProvisioners();
    // TODO: CDAP-13246 check ProvisionerDataset to find any operations that are supposed to be in progress and
    // pick up where they left off
    this.executorService = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("provisioning-service-%d"));
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}", getClass().getSimpleName());
    try {
      // Shutdown the executor, which will issue an interrupt to the running thread.
      // Wait for a moment for threads to complete. Even if they don't, however, it also ok since we have
      // the state persisted and the threads are daemon threads.
      executorService.shutdownNow();
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // Ignore it.
    }
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Cancel any provisioning operation currently taking place for the program run
   *
   * @param programRunId the program run
   */
  public void cancel(ProgramRunId programRunId) {
    // TODO: CDAP-13297 implement
  }

  /**
   * Record that a cluster will be provisioned for a program run, returning a Runnable that will actually perform
   * the cluster provisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   *
   * @param provisionRequest the provision request
   * @param datasetContext dataset context for the transaction
   * @return runnable that will actually execute the cluster provisioning
   */
  public ProvisioningTask provision(ProvisionRequest provisionRequest, DatasetContext datasetContext) {
    ProgramRunId programRunId = provisionRequest.getProgramRunId();
    ProgramOptions programOptions = provisionRequest.getProgramOptions();
    Map<String, String> args = programOptions.getArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(args);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);
    if (provisioner == null) {
      LOG.error("Could not provision cluster for program run {} because provisioner {} does not exist.",
                programRunId, name);
      provisionerNotifier.deprovisioned(programRunId);
      return new NoOpProvisioningTask(programRunId);
    }

    // Generate the SSH key pair if the provisioner is not YARN
    SSHKeyInfo sshKeyInfo = null;
    if (!YarnProvisioner.SPEC.equals(provisioner.getSpec())) {
      try {
        sshKeyInfo = generateSSHKey(programRunId);
      } catch (Exception e) {
        LOG.error("Failed to generate SSH key pair for program run {} with provisioner {}", programRunId, name, e);
        provisionerNotifier.deprovisioning(programRunId);
        return new NoOpProvisioningTask(programRunId);
      }
    }

    Map<String, String> properties = SystemArguments.getProfileProperties(args);
    ProvisionerContext context = new DefaultProvisionerContext(programRunId, properties, sparkCompat,
                                                               createSSHContext(sshKeyInfo));

    ClusterOp clusterOp = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.REQUESTING_CREATE);
    ClusterInfo clusterInfo =
      new ClusterInfo(programRunId, provisionRequest.getProgramDescriptor(),
                      properties, name, provisionRequest.getUser(), clusterOp, sshKeyInfo, null);
    ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);
    provisionerDataset.putClusterInfo(clusterInfo);

    ProvisionTask task = new ProvisionTask(provisionRequest, provisioner, context, provisionerNotifier,
                                           transactional, datasetFramework, sshKeyInfo);
    return new ProvisioningTask(programRunId) {
      @Override
      public void run() {
        executorService.execute(task);
      }
    };
  }

  /**
   * Record that a cluster will be deprovisioned for a program run, returning a task that will actually perform
   * the cluster deprovisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   *
   * @param programRunId the program run to deprovision
   * @param datasetContext dataset context for the transaction
   * @return runnable that will actually execute the cluster deprovisioning
   */
  public ProvisioningTask deprovision(ProgramRunId programRunId, DatasetContext datasetContext) {
    ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);
    ClusterInfo existing = provisionerDataset.getClusterInfo(programRunId);
    if (existing == null) {
      LOG.error("Received request to de-provision a cluster for program run {}, but could not find information " +
                  "about the cluster.", programRunId);
      // TODO: CDAP-13246 move to orphaned state
      return new NoOpProvisioningTask(programRunId);
    }
    Provisioner provisioner = provisionerInfo.get().provisioners.get(existing.getProvisionerName());
    if (provisioner == null) {
      LOG.error("Could not de-provision cluster for program run {} because provisioner {} does not exist.",
                programRunId, existing.getProvisionerName());
      // TODO: CDAP-13246 move to orphaned state
      return new NoOpProvisioningTask(programRunId);
    }

    Map<String, String> properties = existing.getProvisionerProperties();
    ProvisionerContext context = new DefaultProvisionerContext(programRunId, properties, sparkCompat,
                                                               createSSHContext(existing.getSshKeyInfo()));

    ClusterOp clusterOp = new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.REQUESTING_DELETE);
    ClusterInfo clusterInfo = new ClusterInfo(existing, clusterOp, existing.getCluster());
    provisionerDataset.putClusterInfo(clusterInfo);

    DeprovisionTask task = new DeprovisionTask(programRunId, provisioner, context, provisionerNotifier,
                                               locationFactory, transactional, datasetFramework);
    return new ProvisioningTask(programRunId) {
      @Override
      public void run() {
        executorService.execute(task);
      }
    };
  }

  /**
   * Reloads provisioners in the extension directory. Any new provisioners will be added and any deleted provisioners
   * will be removed.
   */
  public void reloadProvisioners() {
    Map<String, Provisioner> provisioners = provisionerProvider.loadProvisioners();
    Map<String, ProvisionerConfig> provisionerConfigs =
      provisionerConfigProvider.loadProvisionerConfigs(provisioners.keySet());
    LOG.debug("Provisioners = {}", provisioners);
    Map<String, ProvisionerDetail> details = new HashMap<>(provisioners.size());
    for (Map.Entry<String, Provisioner> provisionerEntry : provisioners.entrySet()) {
      ProvisionerSpecification spec = provisionerEntry.getValue().getSpec();
      String provisionerName = provisionerEntry.getKey();
      ProvisionerConfig config = provisionerConfigs.getOrDefault(provisionerName,
                                                                 new ProvisionerConfig(new ArrayList<>()));
      details.put(provisionerName, new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                         config.getConfigurationGroups()));
    }
    provisionerInfo.set(new ProvisionerInfo(provisioners, details));
  }

  /**
   * @return unmodifiable collection of all provisioner specs
   */
  public Collection<ProvisionerDetail> getProvisionerDetails() {
    return provisionerInfo.get().details.values();
  }

  /**
   * Get the spec for the specified provisioner.
   *
   * @param name the name of the provisioner
   * @return the spec for the provisioner, or null if the provisioner does not exist
   */
  @Nullable
  public ProvisionerDetail getProvisionerDetail(String name) {
    return provisionerInfo.get().details.get(name);
  }

  /**
   * Validate properties for the specified provisioner.
   *
   * @param provisionerName the name of the provisioner to validate
   * @param properties properties for the specified provisioner
   * @throws NotFoundException if the provisioner does not exist
   * @throws IllegalArgumentException if the properties are invalid
   */
  public void validateProperties(String provisionerName, Map<String, String> properties) throws NotFoundException {
    Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);
    if (provisioner == null) {
      throw new NotFoundException(String.format("Provisioner '%s' does not exist", provisionerName));
    }
    provisioner.validateProperties(properties);
  }

  /**
   * Generates a SSH key pair.
   */
  private SSHKeyInfo generateSSHKey(ProgramRunId programRunId) throws JSchException, IOException {
    JSch jsch = new JSch();
    KeyPair keyPair = KeyPair.genKeyPair(jsch, KeyPair.RSA, 2048);

    Location keysDir = locationFactory.create(String.format("provisioner/keys/%s.%s.%s.%s.%s",
                                                            programRunId.getNamespace(),
                                                            programRunId.getApplication(),
                                                            programRunId.getType().name().toLowerCase(),
                                                            programRunId.getProgram(),
                                                            programRunId.getRun()));
    keysDir.mkdirs();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyPair.writePublicKey(bos, "cdap@cask.co");
    byte[] publicKey = bos.toByteArray();

    bos.reset();
    keyPair.writePrivateKey(bos, null);
    byte[] privateKey = bos.toByteArray();

    Location publicKeyFile = keysDir.append("id_rsa.pub");
    try (OutputStream os = publicKeyFile.getOutputStream()) {
      os.write(publicKey);
    }

    Location privateKeyFile = keysDir.append("id_rsa");
    try (OutputStream os = privateKeyFile.getOutputStream("600")) {
      os.write(privateKey);
    }

    return new SSHKeyInfo(keysDir.toURI(), publicKeyFile.getName(), privateKeyFile.getName(), "cdap");
  }

  @Nullable
  private SSHContext createSSHContext(@Nullable SSHKeyInfo keyInfo) {
    return keyInfo == null ? null : new DefaultSSHContext(locationFactory, keyInfo);
  }

  /**
   * Just a container for provisioner instances and specs, so that they can be updated atomically.
   */
  private static class ProvisionerInfo {
    private final Map<String, Provisioner> provisioners;
    private final Map<String, ProvisionerDetail> details;

    private ProvisionerInfo(Map<String, Provisioner> provisioners, Map<String, ProvisionerDetail> details) {
      this.provisioners = Collections.unmodifiableMap(provisioners);
      this.details = Collections.unmodifiableMap(details);
    }
  }
}
