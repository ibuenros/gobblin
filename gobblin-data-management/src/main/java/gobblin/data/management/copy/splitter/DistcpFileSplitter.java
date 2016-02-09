/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.splitter;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.guid.Guid;


/**
 * Helper class for splitting files for distcp.
 */
@Slf4j
public class DistcpFileSplitter {

  public static final String MAX_SPLIT_SIZE_KEY = CopyConfiguration.COPY_PREFIX + ".file.max.split.size";
  public static final long DEFAULT_MAX_SPLIT_SIZE = Long.MAX_VALUE;

  public static final Set<String> KNOWN_SCHEMES_SUPPORTING_CONCAT = Sets.newHashSet("hdfs");

  /**
   * A split for a distcp file. Represents a section of a file, aligned to block boundaries.
   */
  @Data
  public static class Split {
    private final long lowPosition;
    private final long highPosition;
    private final int splitNumber;
    private final int totalSplits;
    private final String partName;

    public final boolean isLastSplit() {
      return this.splitNumber == this.totalSplits - 1;
    }
  }

  private static final String SPLIT_KEY = CopyConfiguration.COPY_PREFIX + ".file.splitter.split";
  private static final Gson GSON = new Gson();

  /**
   * Split an input {@link CopyableFile} into multiple splits aligned with block boundaries.
   *
   * @param file {@link CopyableFile} to split.
   * @param workUnit {@link WorkUnit} generated for this file.
   * @param targetFs destination {@link FileSystem} where file is to be copied.
   * @return a list of {@link WorkUnit}, each for a split of this file.
   * @throws IOException
   */
  public static Collection<WorkUnit> splitFile(CopyableFile file, WorkUnit workUnit, FileSystem targetFs)
      throws IOException {

    long len = file.getFileStatus().getLen();
    long blockSize = file.getBlockSize(targetFs);

    long maxSplitSize = workUnit.getPropAsLong(MAX_SPLIT_SIZE_KEY, DEFAULT_MAX_SPLIT_SIZE);

    if (maxSplitSize < blockSize) {
      log.warn(String.format("Max split size must be at least block size. Adjusting to %d.", blockSize));
      maxSplitSize = blockSize;
    }

    if (len < maxSplitSize) {
      return Lists.newArrayList(workUnit);
    }

    if (!KNOWN_SCHEMES_SUPPORTING_CONCAT.contains(targetFs.getUri().getScheme())) {
      log.warn(String.format("File %s with size %d should be split, however file system with scheme %s does "
          + "not appear to support concat. Will not split.", file.getDestination(), len, targetFs.getUri().getScheme()));
      return Lists.newArrayList(workUnit);
    }

    Collection<WorkUnit> newWorkUnits = Lists.newArrayList();

    long lengthPerSplit = (maxSplitSize / blockSize) * blockSize;
    int splits = (int) (len / lengthPerSplit + 1);
    for (int i =0; i < splits; i++) {
      WorkUnit newWorkUnit = WorkUnit.copyOf(workUnit);

      long lowPos = lengthPerSplit * i;
      long highPos = lengthPerSplit * (i + 1);

      Split split = new Split(lowPos, highPos, i, splits,
          String.format("%s.__PART%d__", file.getDestination().getName(), i));
      String serializedSplit = GSON.toJson(split);

      newWorkUnit.setProp(SPLIT_KEY, serializedSplit);

      Guid oldGuid = CopySource.getWorkUnitGuid(newWorkUnit).get();
      Guid newGuid = oldGuid.append(Guid.fromStrings(serializedSplit));

      CopySource.setWorkUnitGuid(workUnit, newGuid);
      newWorkUnits.add(newWorkUnit);
    }

    return newWorkUnits;

  }

  /**
   * Finds all split work units in the input collection and merges the file parts into the expected output files.
   * @param fileSystem {@link FileSystem} where file parts exist.
   * @param workUnits Collection of {@link WorkUnitState}s possibly containing split work units.
   * @return The collection of {@link WorkUnitState}s where split work units for each file have been merged.
   * @throws IOException
   */
  public static Collection<WorkUnitState> mergeAllSplitWorkUnits(FileSystem fileSystem, Collection<WorkUnitState> workUnits)
      throws IOException {
    ListMultimap<CopyableFile, WorkUnitState> splitWorkUnitsMap = ArrayListMultimap.create();
    for (WorkUnitState workUnit : workUnits) {
      if (isSplitWorkUnit(workUnit)) {
        CopyableFile copyableFile = CopySource.deserializeCopyableFile(workUnit);
        splitWorkUnitsMap.put(copyableFile, workUnit);
      }
    }
    for (CopyableFile file : splitWorkUnitsMap.keySet()) {
      log.info(String.format("Merging split file %s.", file.getDestination()));
      Path parentPath = FileAwareInputStreamDataWriter.getOutputFilePath(splitWorkUnitsMap.get(file).get(0)).getParent();

      WorkUnitState newWorkUnit = mergeSplits(fileSystem, file, splitWorkUnitsMap.get(file), parentPath);

      for (WorkUnitState wu : splitWorkUnitsMap.get(file)) {
        workUnits.remove(wu);
      }
      workUnits.add(newWorkUnit);

    }
    return workUnits;
  }

  /**
   * Merges all the splits for a given file.
   * @param fileSystem {@link FileSystem} where file parts exist.
   * @param file {@link CopyableFile} to merge.
   * @param workUnits {@link WorkUnitState}s for all parts of this file.
   * @param parentPath {@link Path} where the parts of the file are located.
   * @return a {@link WorkUnit} equivalent to the distcp work unit if the file had not been split.
   * @throws IOException
   */
  private static WorkUnitState mergeSplits(FileSystem fileSystem, CopyableFile file, Collection<WorkUnitState> workUnits,
      Path parentPath) throws IOException {

    log.info(String.format("File %s was written in %d parts. Merging.", file.getDestination(), workUnits.size()));
    Path[] parts = new Path[workUnits.size()];
    for (WorkUnitState workUnit : workUnits) {
      if (!isSplitWorkUnit(workUnit)) {
        throw new IOException("Not a split work unit.");
      }
      Split split = getSplit(workUnit).get();
      parts[split.getSplitNumber()] = new Path(parentPath, split.getPartName());
    }

    Path target = new Path(parentPath, file.getDestination().getName());

    fileSystem.rename(parts[0], target);
    fileSystem.concat(target, Arrays.copyOfRange(parts, 1, parts.length));

    WorkUnitState finalWorkUnit = workUnits.iterator().next();
    finalWorkUnit.removeProp(SPLIT_KEY);
    return finalWorkUnit;
  }

  /**
   * @return whether the {@link WorkUnit} is a split work unit.
   */
  public static boolean isSplitWorkUnit(State workUnit) {
    return workUnit.contains(SPLIT_KEY);
  }

  /**
   * @return the {@link Split} object contained in the {@link WorkUnit}.
   */
  public static Optional<Split> getSplit(State workUnit) {
    return workUnit.contains(SPLIT_KEY) ? Optional.of(GSON.fromJson(workUnit.getProp(SPLIT_KEY), Split.class))
        : Optional.<Split>absent();
  }

}
