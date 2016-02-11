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

package gobblin.data.management.copy;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.extractor.FileAwareInputStreamExtractor;
import gobblin.data.management.copy.publisher.CopyEventSubmitterHelper;
import gobblin.data.management.copy.splitter.DistcpFileSplitter;
import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.partition.FileSet;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.util.ExecutorsUtils;
import gobblin.util.PathUtils;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;
import gobblin.util.RateControlledFileSystem;
import gobblin.util.WriterUtils;
import gobblin.util.executors.ScalingThreadPoolExecutor;
import gobblin.util.guid.Guid;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;


/**
 * {@link gobblin.source.Source} that generates work units from {@link gobblin.data.management.copy.CopyableDataset}s.
 *
 */
@Slf4j
public class CopySource extends AbstractSource<String, FileAwareInputStream> {

  public static final String DEFAULT_DATASET_PROFILE_CLASS_KEY = CopyableGlobDatasetFinder.class.getCanonicalName();
  public static final String SERIALIZED_COPYABLE_FILE = CopyConfiguration.COPY_PREFIX + ".serialized.copyable.file";
  public static final String SERIALIZED_COPYABLE_DATASET = CopyConfiguration.COPY_PREFIX + ".serialized.copyable.datasets";
  public static final String WORK_UNIT_GUID = CopyConfiguration.COPY_PREFIX + ".work.unit.guid";
  public static final String MAX_CONCURRENT_LISTING_SERVICES = CopyConfiguration.COPY_PREFIX + ".max.concurrent.listing.services";
  public static final int DEFAULT_MAX_CONCURRENT_LISTING_SERVICES = 20;
  public static final String MAX_FILESYSTEM_QPS = CopyConfiguration.COPY_PREFIX + ".max.filesystem.qps";
  public static final String MAX_FILES_COPIED_KEY = CopyConfiguration.COPY_PREFIX + ".max.files.copied";
  public static final int DEFAULT_MAX_FILES_COPIED = 100000;

  /**
   * <ul>
   * Does the following:
   * <li>Instantiate a {@link DatasetFinder}.
   * <li>Find all {@link Dataset} using {@link DatasetFinder}.
   * <li>For each {@link CopyableDataset} get all {@link CopyableFile}s.
   * <li>Create a {@link WorkUnit} per {@link CopyableFile}.
   * </ul>
   *
   * <p>
   * In this implementation, one workunit is created for every {@link CopyableFile} found. But the extractor/converters
   * and writers are built to support multiple {@link CopyableFile}s per workunit
   * </p>
   *
   * @param state see {@link gobblin.configuration.SourceState}
   * @return Work units for copying files.
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    CopyContext copyContext = new CopyContext();

    try {

      FileSystem sourceFs = getSourceFileSystem(state);
      FileSystem targetFs = getTargetFileSystem(state);

      DatasetFinder<CopyableDataset> datasetFinder =
          DatasetUtils.instantiateDatasetFinder(state.getProperties(), sourceFs, DEFAULT_DATASET_PROFILE_CLASS_KEY);
      List<CopyableDataset> copyableDatasets = datasetFinder.findDatasets();

      // TODO: The comparator sets the priority of file sets. Currently, all file sets have the same priority, this needs to
      // be pluggable.
      ConcurrentBoundedWorkUnitList workUnitList =
          new ConcurrentBoundedWorkUnitList(state.getPropAsInt(MAX_FILES_COPIED_KEY, DEFAULT_MAX_FILES_COPIED),
          new AllEqualComparator<FileSet<CopyableFile>>());

      ExecutorService executor =
          ScalingThreadPoolExecutor.newScalingThreadPool(0,
              state.getPropAsInt(MAX_CONCURRENT_LISTING_SERVICES, DEFAULT_MAX_CONCURRENT_LISTING_SERVICES),
              100, ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("Dataset-cleaner-pool-%d")));
      ListeningExecutorService service = MoreExecutors.listeningDecorator(executor);
      List<ListenableFuture<?>> futures = Lists.newArrayList();

      for (CopyableDataset copyableDataset : copyableDatasets) {
        Path targetRoot = getTargetRoot(state, datasetFinder, copyableDataset);
        futures.add(service.submit(
            new DatasetWorkUnitGenerator(copyableDataset, sourceFs, targetFs, state, targetRoot,
                copyContext, workUnitList)));
      }

      for (ListenableFuture<?> future : futures) {
        try {
          future.get();
        } catch (ExecutionException | InterruptedException exc) {
          throw new IOException("Failed to generate work units.", exc);
        }
      }

      log.info(String.format("Created %s workunits ", workUnitList.getWorkUnits().size()));

      return Lists.newArrayList(workUnitList.getWorkUnits());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private FileSystem getOptionallyThrottledFileSystem(FileSystem fs, State state) throws IOException {
    if (state.contains(MAX_FILESYSTEM_QPS)) {
      try {
        RateControlledFileSystem newFS = new RateControlledFileSystem(fs, state.getPropAsInt(MAX_FILESYSTEM_QPS));
        newFS.startRateControl();
        return newFS;
      } catch (ExecutionException ee) {
        throw new IOException("Could not create throttled FileSystem.", ee);
      }
    }
    return fs;
  }

  /**
   * {@link Runnable} to generate copy listing for one {@link CopyableDataset}.
   */
  @AllArgsConstructor
  private class DatasetWorkUnitGenerator implements Runnable {

    private final CopyableDataset copyableDataset;
    private final FileSystem originFs;
    private final FileSystem targetFs;
    private final State state;
    private final Path targetRoot;
    private final CopyContext copyContext;
    private final ConcurrentBoundedWorkUnitList workUnitList;

    @Override public void run() {

      if (workUnitList.hasRejectedFileSet()) {
        // Stop generating work units the first time the work unit container rejects a file set due to capacity issues.
        // TODO: more sophisticated stop algorithm.
        return;
      }

      try {
        CopyConfiguration copyConfiguration =
            CopyConfiguration.builder(this.state.getProperties()).targetRoot(this.targetRoot).
                copyContext(this.copyContext).build();

        Collection<CopyableFile> files = this.copyableDataset.getCopyableFiles(this.targetFs, copyConfiguration);
        List<FileSet<CopyableFile>> fileSets = partitionCopyableFiles(this.copyableDataset, files);

        // Sort to optimize the insertion to work units list
        Collections.sort(fileSets, this.workUnitList.getComparator());

        for (FileSet<CopyableFile> fileSet : fileSets) {
          Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, CopyConfiguration.COPY_PREFIX, fileSet.getName());
          List<WorkUnit> workUnitsForPartition = Lists.newArrayList();
          for (CopyableFile copyableFile : fileSet.getFiles()) {

            CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(this.copyableDataset, this.targetRoot);
            CopyableFile.DatasetAndPartition datasetAndPartition = copyableFile.getDatasetAndPartition(metadata);

            WorkUnit workUnit = new WorkUnit(extract);
            workUnit.addAll(this.state);
            serializeCopyableFile(workUnit, copyableFile);
            serializeCopyableDataset(workUnit, metadata);
            GobblinMetrics.addCustomTagToState(workUnit, new Tag<>(CopyEventSubmitterHelper.DATASET_ROOT_METADATA_NAME,
                this.copyableDataset.datasetRoot().toString()));
            workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetAndPartition.toString());
            workUnit.setProp(SlaEventKeys.DATASET_URN_KEY, this.copyableDataset.datasetRoot());
            workUnit.setProp(SlaEventKeys.PARTITION_KEY, copyableFile.getFileSet());
            computeAndSetWorkUnitGuid(workUnit);
            workUnitsForPartition.addAll(DistcpFileSplitter.splitFile(copyableFile, workUnit, targetFs));
          }
          this.workUnitList.addFileSet(fileSet, workUnitsForPartition);
        }
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to generate work units for dataset " + this.copyableDataset.datasetRoot(), ioe);
      }
    }
  }

  /**
   * @param state a {@link gobblin.configuration.WorkUnitState} carrying properties needed by the returned
   *          {@link Extractor}
   * @return a {@link FileAwareInputStreamExtractor}.
   * @throws IOException
   */
  @Override
  public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {

    CopyableFile copyableFile = deserializeCopyableFile(state);

    return new FileAwareInputStreamExtractor(getSourceFileSystem(state), copyableFile, state);
  }

  @Override
  public void shutdown(SourceState state) {
  }

  protected FileSystem getSourceFileSystem(State state) throws IOException {

    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

  private FileSystem getTargetFileSystem(State state) throws IOException {
    return getOptionallyThrottledFileSystem(WriterUtils.getWriterFS(state, 1, 0), state);
  }

  private Path getTargetRoot(State state, DatasetFinder<?> datasetFinder, CopyableDataset dataset) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
        "Missing property " + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);
    Path basePath = new Path(state.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
    Path datasetRelativeToCommonRoot = PathUtils.relativizePath(
        PathUtils.getPathWithoutSchemeAndAuthority(dataset.datasetRoot()),
        PathUtils.getPathWithoutSchemeAndAuthority(datasetFinder.commonDatasetRoot()));
    return new Path(basePath, datasetRelativeToCommonRoot);
  }

  private void computeAndSetWorkUnitGuid(WorkUnit workUnit) throws IOException {
    Guid guid = Guid.fromStrings(workUnit.contains(ConfigurationKeys.CONVERTER_CLASSES_KEY) ?
        workUnit.getProp(ConfigurationKeys.CONVERTER_CLASSES_KEY) :
        "");
    setWorkUnitGuid(workUnit, guid.append(deserializeCopyableFile(workUnit)));
  }

  /**
   * Set a unique, replicable guid for this work unit. Used for recovering partially successful work units.
   * @param state {@link State} where guid should be written.
   * @param guid A byte array guid.
   */
  public static void setWorkUnitGuid(State state, Guid guid) throws IOException {
    state.setProp(WORK_UNIT_GUID, guid.toString());
  }

  /**
   * Get guid in this state if available. This is the reverse operation of {@link #setWorkUnitGuid}.
   * @param state State from which guid should be extracted.
   * @return A byte array guid.
   * @throws IOException
   */
  public static Optional<Guid> getWorkUnitGuid(State state) throws IOException {
    if (state.contains(WORK_UNIT_GUID)) {
      return Optional.of(Guid.deserialize(state.getProp(WORK_UNIT_GUID)));
    } else {
      return Optional.absent();
    }
  }

  /**
   * Serialize a {@link List} of {@link CopyableFile}s into a {@link State} at {@link #SERIALIZED_COPYABLE_FILE}
   */
  public static void serializeCopyableFile(State state, CopyableFile copyableFile) throws IOException {
    state.setProp(SERIALIZED_COPYABLE_FILE, CopyableFile.serialize(copyableFile));
  }

  /**
   * Deserialize a {@link List} of {@link CopyableFile}s from a {@link State} at {@link #SERIALIZED_COPYABLE_FILE}
   */
  public static CopyableFile deserializeCopyableFile(State state) throws IOException {
    return CopyableFile.deserialize(state.getProp(SERIALIZED_COPYABLE_FILE));
  }

  /**
   * Serialize a {@link CopyableDataset} into a {@link State} at {@link #SERIALIZED_COPYABLE_DATASET}
   */
  public static void serializeCopyableDataset(State state, CopyableDatasetMetadata copyableDataset) throws IOException {
    state.setProp(SERIALIZED_COPYABLE_DATASET, copyableDataset.serialize());
  }

  /**
   * Deserialize a {@link CopyableDataset} from a {@link State} at {@link #SERIALIZED_COPYABLE_DATASET}
   */
  public static CopyableDatasetMetadata deserializeCopyableDataset(State state) throws IOException {
    return CopyableDatasetMetadata.deserialize(state.getProp(SERIALIZED_COPYABLE_DATASET));
  }

  private List<FileSet<CopyableFile>> partitionCopyableFiles(Dataset dataset, Collection<CopyableFile> files) {
    Map<String, FileSet.Builder<CopyableFile>> partitionBuildersMaps = Maps.newHashMap();
    for (CopyableFile file : files) {
      if (!partitionBuildersMaps.containsKey(file.getFileSet())) {
        partitionBuildersMaps.put(file.getFileSet(), new FileSet.Builder<CopyableFile>(file.getFileSet(), dataset));
      }
      partitionBuildersMaps.get(file.getFileSet()).add(file);
    }
    return Lists.newArrayList(Iterables.transform(partitionBuildersMaps.values(),
        new Function<FileSet.Builder<CopyableFile>, FileSet<CopyableFile>>() {
          @Nullable @Override public FileSet<CopyableFile> apply(FileSet.Builder<CopyableFile> input) {
            return input.build();
          }
        }));
  }

}
