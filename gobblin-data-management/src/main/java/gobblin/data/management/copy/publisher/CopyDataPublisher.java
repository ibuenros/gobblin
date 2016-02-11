/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.recovery.RecoveryHelper;
import gobblin.data.management.copy.splitter.DistcpFileSplitter;
import gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.writer.FileAwareInputStreamDataWriterBuilder;
import gobblin.publisher.UnpublishedHandling;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.publisher.DataPublisher;
import gobblin.util.HadoopUtils;

/**
 * A {@link DataPublisher} to {@link CopyableFile}s from task output to final destination.
 */
@Slf4j
public class CopyDataPublisher extends DataPublisher implements UnpublishedHandling {

  private Path writerOutputDir;
  private FileSystem fs;
  protected EventSubmitter eventSubmitter;

  /**
   * Build a new {@link CopyDataPublisher} from {@link State}. The constructor expects the following to be set in the
   * {@link State},
   * <ul>
   * <li>{@link ConfigurationKeys#WRITER_OUTPUT_DIR}
   * <li>{@link ConfigurationKeys#WRITER_FILE_SYSTEM_URI}
   * </ul>
   *
   */
  public CopyDataPublisher(State state) throws IOException {
    super(state);
    Configuration conf = new Configuration();
    String uri = this.state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);

    this.fs = FileSystem.get(URI.create(uri), conf);

    FileAwareInputStreamDataWriterBuilder.setJobSpecificOutputPaths(state);

    this.writerOutputDir = new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));

    MetricContext metricContext =
        Instrumented.getMetricContext(state, CopyDataPublisher.class, GobblinMetrics.getCustomTagsFromState(state));

    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "gobblin.copy.CopyDataPublisher").build();
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    /*
     * This mapping is used to set WorkingState of all {@link WorkUnitState}s to {@link
     * WorkUnitState.WorkingState#COMMITTED} after a {@link CopyableDataset} is successfully published
     */
    Multimap<CopyableFile.DatasetAndPartition, WorkUnitState> datasets = groupByFileSet(states);

    boolean allDatasetsPublished = true;
    for (CopyableFile.DatasetAndPartition datasetAndPartition : datasets.keySet()) {
      try {
        this.publishFileSet(datasetAndPartition, datasets.get(datasetAndPartition));
      } catch (Throwable e) {
        CopyEventSubmitterHelper.submitFailedDatasetPublish(eventSubmitter, datasetAndPartition);
        log.error("Failed to publish " + datasetAndPartition.getDataset().getDatasetTargetRoot(), e);
        allDatasetsPublished = false;
      }
    }

    if (!allDatasetsPublished) {
      throw new IOException("Not all datasets published successfully");
    }
  }

  @Override public void handleUnpublishedWorkUnits(Collection<? extends WorkUnitState> states) throws IOException {
      int filesPersisted = persistFailedFileSet(states);
      log.info(String.format("Successfully persisted %d work units.", filesPersisted));
  }

  /**
   * Create a {@link Multimap} that maps a {@link CopyableDataset} to all {@link WorkUnitState}s that belong to this
   * {@link CopyableDataset}. This mapping is used to set WorkingState of all {@link WorkUnitState}s to
   * {@link WorkUnitState.WorkingState#COMMITTED} after a {@link CopyableDataset} is successfully published.
   */
  private Multimap<CopyableFile.DatasetAndPartition, WorkUnitState> groupByFileSet(
      Collection<? extends WorkUnitState> states)
      throws IOException {
    Multimap<CopyableFile.DatasetAndPartition, WorkUnitState> datasetRoots = ArrayListMultimap.create();

    for (WorkUnitState workUnitState : states) {
      CopyableFile file = CopySource.deserializeCopyableFile(workUnitState);
      CopyableFile.DatasetAndPartition datasetAndPartition = file.getDatasetAndPartition(
          CopyableDatasetMetadata.deserialize(workUnitState.getProp(CopySource.SERIALIZED_COPYABLE_DATASET)));

      datasetRoots.put(datasetAndPartition, workUnitState);
    }
    return datasetRoots;
  }

  /**
   * Publish data for a {@link CopyableDataset}.
   *
   */
  private void publishFileSet(CopyableFile.DatasetAndPartition datasetAndPartition,
      Collection<WorkUnitState> datasetWorkUnitStates)
      throws IOException {

    Preconditions.checkArgument(!datasetWorkUnitStates.isEmpty(),
        "publishFileSet received an empty collection work units. This is an error in code.");

    CopyableDatasetMetadata metadata = CopyableDatasetMetadata.deserialize(
        datasetWorkUnitStates.iterator().next().getProp(CopySource.SERIALIZED_COPYABLE_DATASET));
    Path datasetWriterOutputPath = new Path(this.writerOutputDir, datasetAndPartition.identifier());

    log.info("Merging all split work units.");
    DistcpFileSplitter.mergeAllSplitWorkUnits(this.fs, datasetWorkUnitStates);

    log.info(String
        .format("Publishing fileSet from %s to %s", datasetWriterOutputPath, metadata.getDatasetTargetRoot()));

    HadoopUtils.renameRecursively(fs, datasetWriterOutputPath, findPathRoot(metadata.getDatasetTargetRoot()));

    fs.delete(datasetWriterOutputPath, true);

    long datasetOriginTimestamp = Long.MAX_VALUE;
    long datasetUpstreamTimestamp = Long.MAX_VALUE;

    for (WorkUnitState wus : datasetWorkUnitStates) {
      if (wus.getWorkingState() == WorkingState.SUCCESSFUL) {
        wus.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        CopyEventSubmitterHelper.submitSuccessfulFilePublish(eventSubmitter, wus);
      }
      CopyableFile copyableFile = CopySource.deserializeCopyableFile(wus);
      if (datasetOriginTimestamp > copyableFile.getOriginTimestamp()) {
        datasetOriginTimestamp = copyableFile.getOriginTimestamp();
      }
      if (datasetUpstreamTimestamp > copyableFile.getUpstreamTimestamp()) {
        datasetUpstreamTimestamp = copyableFile.getUpstreamTimestamp();
      }
    }

    CopyEventSubmitterHelper.submitSuccessfulDatasetPublish(eventSubmitter, datasetAndPartition,
        Long.toString(datasetOriginTimestamp), Long.toString(datasetUpstreamTimestamp));
  }

  private Path findPathRoot(Path path) {
    while (path.getParent() != null) {
      path = path.getParent();
    }
    return path;
  }

  private int persistFailedFileSet(Collection<? extends WorkUnitState> workUnitStates) throws IOException {
    RecoveryHelper recoveryHelper = new RecoveryHelper(this.fs, this.state);
    int filesPersisted = 0;
    for (WorkUnitState wu : workUnitStates) {
      if (wu.getWorkingState() == WorkingState.SUCCESSFUL) {
        CopyableFile file = CopySource.deserializeCopyableFile(wu);
        Path outputPath = FileAwareInputStreamDataWriter.getOutputFilePath(wu);
        if (recoveryHelper.persistFile(wu, file, outputPath)) {
          filesPersisted++;
        }
      }
    }
    return filesPersisted;
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void initialize() throws IOException {
  }
}
