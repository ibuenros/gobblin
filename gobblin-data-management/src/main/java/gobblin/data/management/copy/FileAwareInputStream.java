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

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import org.apache.hadoop.fs.FSDataInputStream;

import com.google.common.base.Optional;

import gobblin.data.management.copy.splitter.DistcpFileSplitter;


/**
 * A wrapper to {@link FSDataInputStream} that represents an entity to be copied. The enclosed {@link CopyableFile} instance
 * contains file Metadata like permission, destination path etc. required by the writers and converters.
 */
@Getter
public class FileAwareInputStream {

  private CopyableFile file;
  private FSDataInputStream inputStream;
  private Optional<DistcpFileSplitter.Split> split = Optional.absent();

  @Builder(toBuilder = true)
  public FileAwareInputStream(@NonNull CopyableFile file, @NonNull FSDataInputStream inputStream,
      Optional<DistcpFileSplitter.Split> split) {
    this.file = file;
    this.inputStream = inputStream;
    this.split = split == null ? Optional.<DistcpFileSplitter.Split>absent() : split;
  }

  @Override
  public String toString() {
    return this.file.toString();
  }
}
