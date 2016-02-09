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

package gobblin.data.management.copy.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.copy.PreserveAttributes;
import gobblin.data.management.copy.recovery.RecoveryHelper;
import gobblin.data.management.copy.splitter.DistcpFileSplitter;
import gobblin.state.ConstructState;
import gobblin.util.FinalState;
import gobblin.util.PathUtils;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;
import gobblin.util.io.StreamUtils;
import gobblin.writer.DataWriter;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;


/**
 * A {@link DataWriter} to write {@link FileAwareInputStream}
 */
@Slf4j
public class FileAwareInputStreamDataWriter implements DataWriter<FileAwareInputStream>, FinalState {

  protected final AtomicLong bytesWritten = new AtomicLong();
  protected final AtomicLong filesWritten = new AtomicLong();
  protected final State state;
  protected final FileSystem fs;
  protected final Path stagingDir;
  protected final Path outputDir;
  protected final Closer closer = Closer.create();
  protected CopyableDatasetMetadata copyableDatasetMetadata;
  protected final RecoveryHelper recoveryHelper;
  /**
   * The copyable file in the WorkUnit might be modified by converters (e.g. output extensions added / removed).
   * This field is set when {@link #write} is called, and points to the actual, possibly modified {@link CopyableFile}
   * that was written by this writer.
   */
  protected Optional<CopyableFile> actualProcessedCopyableFile;

  public FileAwareInputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {

    if (numBranches > 1) {
      throw new IOException("Distcp can only operate with one branch.");
    }

    this.state = state;

    String uri =
        this.state
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches,
                branchId), ConfigurationKeys.LOCAL_FS_URI);

    this.fs = FileSystem.get(URI.create(uri), new Configuration());
    this.stagingDir = WriterUtils.getWriterStagingDir(state, numBranches, branchId);
    this.outputDir = getOutputDir(state);
    this.copyableDatasetMetadata =
        CopyableDatasetMetadata.deserialize(state.getProp(CopySource.SERIALIZED_COPYABLE_DATASET));
    this.recoveryHelper = new RecoveryHelper(this.fs, state);
    this.actualProcessedCopyableFile = Optional.absent();
  }

  @Override
  public final void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    CopyableFile copyableFile = fileAwareInputStream.getFile();
    Path stagingFile = getStagingFilePath(copyableFile);
    this.actualProcessedCopyableFile = Optional.of(copyableFile);
    this.fs.mkdirs(stagingFile.getParent());
    writeImpl(fileAwareInputStream.getInputStream(), stagingFile, copyableFile, fileAwareInputStream);
    this.filesWritten.incrementAndGet();
  }

  /**
   * Write the contents of input stream into staging path.
   *
   * <p>
   *   WriteAt indicates the path where the contents of the input stream should be written. When this method is called,
   *   the path writeAt.getParent() will exist already, but the path writeAt will not exist. When this method is returned,
   *   the path writeAt must exist. Any data written to any location other than writeAt or a descendant of writeAt
   *   will be ignored.
   * </p>
   *
   * @param inputStream {@link FSDataInputStream} whose contents should be written to staging path.
   * @param writeAt {@link Path} at which contents should be written.
   * @param copyableFile {@link CopyableFile} that generated this copy operation.
   * @param record The actual {@link FileAwareInputStream} passed to the write method.
   * @throws IOException
   */
  protected void writeImpl(FSDataInputStream inputStream, Path writeAt, CopyableFile copyableFile,
      FileAwareInputStream record) throws IOException {

    final short replication = copyableFile.getReplication(this.fs);
    final long blockSize = copyableFile.getBlockSize(this.fs);
    long maxBytes = Long.MAX_VALUE;
    // Whether writer must write EXACTLY maxBytes.
    boolean mustMatchMaxBytes = false;

    if (record.getSplit().isPresent()) {
      inputStream.seek(record.getSplit().get().getLowPosition());
      maxBytes = record.getSplit().get().getHighPosition() - record.getSplit().get().getLowPosition();
      mustMatchMaxBytes = !record.getSplit().get().isLastSplit();
    }

    Predicate<FileStatus> fileStatusAttributesFilter = new Predicate<FileStatus>() {
      @Override public boolean apply(FileStatus input) {
        return input.getReplication() == replication && input.getBlockSize() == blockSize;
      }
    };
    Optional<FileStatus> persistedFile = this.recoveryHelper.findPersistedFile(this.state,
        copyableFile, fileStatusAttributesFilter);

    if (persistedFile.isPresent()) {
      log.info(String.format("Recovering persisted file %s to %s.", persistedFile.get().getPath(), writeAt));
      this.fs.rename(persistedFile.get().getPath(), writeAt);
    } else {

      FSDataOutputStream os =
          this.fs.create(writeAt, true, fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize);
      try {
        long bytesWrittenToFile = StreamUtils.copy(inputStream, os, maxBytes);
        if (mustMatchMaxBytes && bytesWrittenToFile != maxBytes) {
          throw new IOException(String.format("Incomplete write: expected %d, wrote %d bytes.", maxBytes, bytesWrittenToFile));
        }
        this.bytesWritten.addAndGet(bytesWrittenToFile);
        log.info("bytes written: " + this.bytesWritten.get() + " for file " + copyableFile);
      } finally {
        os.close();
        inputStream.close();
      }
    }
  }

  /**
   * Sets the owner/group and permission for the file in the task staging directory
   */
  protected void setFilePermissions(CopyableFile file) throws IOException {
    setRecursivePermission(getStagingFilePath(file), file.getDestinationOwnerAndPermission());
  }

  protected Path getStagingFilePath(CopyableFile file) {
    if (DistcpFileSplitter.isSplitWorkUnit(this.state)) {
      return new Path(this.stagingDir, DistcpFileSplitter.getSplit(this.state).get().getPartName());
    } else {
      return new Path(this.stagingDir, file.getDestination().getName());
    }

  }

  protected static Path getPartitionOutputRoot(Path outputDir,
      CopyableFile.DatasetAndPartition datasetAndPartition) {
    return new Path(outputDir, datasetAndPartition.identifier());
  }

  public static Path getOutputFilePath(CopyableFile file, Path outputDir,
      CopyableFile.DatasetAndPartition datasetAndPartition, State workUnit) {
    return new Path(getPartitionOutputRoot(outputDir, datasetAndPartition),
        PathUtils.withoutLeadingSeparator(file.getDestination()));
  }

  public static Path getOutputFilePath(State wu) throws IOException {
    CopyableFile file = CopySource.deserializeCopyableFile(wu);
    Path outputDir = getOutputDir(wu);
    CopyableDatasetMetadata metadata = CopySource.deserializeCopyableDataset(wu);
    return getOutputFilePath(file, outputDir,
        file.getDatasetAndPartition(metadata), wu);
  }

  public static Path getSplitOutputFilePath(State wu) throws IOException {
    if (DistcpFileSplitter.isSplitWorkUnit(wu)) {
      return new Path(getOutputFilePath(wu).getParent(), DistcpFileSplitter.getSplit(wu).get().getPartName());
    } else {
      return getOutputFilePath(wu);
    }
  }

  public static Path getSplitOutputFilePath(CopyableFile file, Path outputDir,
      CopyableFile.DatasetAndPartition datasetAndPartition, State workUnit) throws IOException {
    if (DistcpFileSplitter.isSplitWorkUnit(workUnit)) {
      return new Path(getOutputFilePath(file, outputDir, datasetAndPartition, workUnit).getParent(),
          DistcpFileSplitter.getSplit(workUnit).get().getPartName());
    } else {
      return getOutputFilePath(file, outputDir, datasetAndPartition, workUnit);
    }
  }

  public static Path getOutputDir(State state) {
    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR,
        1, 0)));
  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. It will not throw exceptions, if operations
   * cannot be executed, will warn and continue.
   */
  private void safeSetPathPermission(Path path, OwnerAndPermission ownerAndPermission) {

    try {
      if (ownerAndPermission.getFsPermission() != null) {
        fs.setPermission(path, ownerAndPermission.getFsPermission());
      }
    } catch (IOException ioe) {
      log.warn("Failed to set permission for directory " + path, ioe);
    }

    String owner = Strings.isNullOrEmpty(ownerAndPermission.getOwner()) ? null : ownerAndPermission.getOwner();
    String group = Strings.isNullOrEmpty(ownerAndPermission.getGroup()) ? null : ownerAndPermission.getGroup();

    try {
      if (owner != null || group != null) {
        this.fs.setOwner(path, owner, group);
      }
    } catch (IOException ioe) {
      log.warn("Failed to set owner and/or group for path " + path, ioe);
    }

  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. And recursively to all directories and files under
   * it.
   */
  private void setRecursivePermission(Path path, OwnerAndPermission ownerAndPermission) throws IOException {
    List<FileStatus> files = FileListUtils.listPathsRecursively(fs, path, FileListUtils.NO_OP_PATH_FILTER);

    // Set permissions bottom up. Permissions are set to files first and then directories
    Collections.reverse(files);

    for (FileStatus file : files) {
      safeSetPathPermission(file.getPath(), addExecutePermissionsIfRequired(file, ownerAndPermission));
    }
  }

  /**
   * The method makes sure it always grants execute permissions for an owner if the <code>file</code> passed is a
   * directory. The publisher needs it to publish it to the final directory and list files under this directory.
   */
  private OwnerAndPermission addExecutePermissionsIfRequired(FileStatus file, OwnerAndPermission ownerAndPermission) {

    if (ownerAndPermission.getFsPermission() == null) {
      return ownerAndPermission;
    }

    if (!file.isDir()) {
      return ownerAndPermission;
    }

    return new OwnerAndPermission(ownerAndPermission.getOwner(), ownerAndPermission.getGroup(),
        addExecutePermissionToOwner(ownerAndPermission.getFsPermission()));

  }

  static FsPermission addExecutePermissionToOwner(FsPermission fsPermission) {
    FsAction newOwnerAction = fsPermission.getUserAction().or(FsAction.EXECUTE);
    return
        new FsPermission(newOwnerAction, fsPermission.getGroupAction(), fsPermission.getOtherAction());
  }

  @Override
  public long recordsWritten() {
    return this.filesWritten.get();
  }

  @Override
  public long bytesWritten() throws IOException {
    return this.bytesWritten.get();
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  /**
   * Moves the file from task staging to task output. Each task has its own staging directory but all the tasks share
   * the same task output directory.
   *
   * {@inheritDoc}
   *
   * @see gobblin.writer.DataWriter#commit()
   */
  @Override
  public void commit() throws IOException {

    CopyableFile copyableFile = this.actualProcessedCopyableFile.get();
    Path stagingFilePath = getStagingFilePath(copyableFile);
    Path outputFilePath = getSplitOutputFilePath(copyableFile, this.outputDir,
        copyableFile.getDatasetAndPartition(this.copyableDatasetMetadata), this.state);

    log.info(String.format("Committing data from %s to %s", stagingFilePath, outputFilePath));
    try {
      setFilePermissions(copyableFile);

      Iterator<OwnerAndPermission> ancestorOwnerAndPermissionIt = copyableFile.getAncestorsOwnerAndPermission() == null ?
          Iterators.<OwnerAndPermission>emptyIterator() : copyableFile.getAncestorsOwnerAndPermission().iterator();

      ensureDirectoryExists(this.fs, outputFilePath.getParent(), ancestorOwnerAndPermissionIt);

      if (!this.fs.rename(stagingFilePath, outputFilePath)) {
          // target exists
          throw new IOException(String.format("Could not commit file %s.", outputFilePath));
      }
    } catch (IOException ioe) {
      // persist file
      this.recoveryHelper.persistFile(this.state, copyableFile, stagingFilePath);
      throw ioe;
    } finally {
      try {
        this.fs.delete(this.stagingDir, true);
      } catch (IOException ioe) {
        log.warn("Failed to delete staging path at " + this.stagingDir);
      }
    }
  }

  private void ensureDirectoryExists(FileSystem fs, Path path,
      Iterator<OwnerAndPermission> ownerAndPermissionIterator) throws IOException {

    if (fs.exists(path)) {
      return;
    }

    if (ownerAndPermissionIterator.hasNext()) {
      OwnerAndPermission ownerAndPermission = ownerAndPermissionIterator.next();

      if (path.getParent() != null) {
        ensureDirectoryExists(fs, path.getParent(), ownerAndPermissionIterator);
      }

      if (!fs.mkdirs(path)) {
        // fs.mkdirs returns false if path already existed. Do not overwrite permissions
        return;
      }

      if (ownerAndPermission.getFsPermission() != null) {
        log.debug("Applying permissions %s to path %s.", ownerAndPermission.getFsPermission(), path);
        fs.setPermission(path, addExecutePermissionToOwner(ownerAndPermission.getFsPermission()));
      }

      String group = ownerAndPermission.getGroup();
      String owner = ownerAndPermission.getOwner();
      if (group != null || owner != null) {
        log.debug("Applying owner %s and group %s to path %s.", owner, group, path);
        fs.setOwner(path, owner, group);
      }
    } else {
      fs.mkdirs(path);
    }
  }

  @Override
  public void cleanup() throws IOException {
    // Do nothing
  }

  @Override public State getFinalState() {
    try {
      State state = new State();
      CopySource.serializeCopyableFile(state, this.actualProcessedCopyableFile.get());
      state.setProp("did.i.actually.write.props", "yes");
      ConstructState constructState = new ConstructState();
      constructState.addOverwriteProperties(state);
      return constructState;
    } catch (IOException ioe) {
      throw new RuntimeException("Could not serialize actual processed copyable file.", ioe);
    }
  }
}
