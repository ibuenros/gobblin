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

import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Matchers;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyContext;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.PreserveAttributes;
import gobblin.data.management.dataset.DummyDataset;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.guid.Guid;

import static org.mockito.Mockito.*;

public class DistcpFileSplitterTest {

  @Test public void testSplitFile() throws Exception {

    FileSystem fs = FileSystem.getLocal(new Configuration());

    FileSystem fsMock = spy(fs);
    doReturn(new URI("hdfs://mycluster/")).when(fsMock).getUri();
    doNothing().when(fsMock).concat(any(Path.class), Matchers.<Path[]>any());

    long maxSplitSize = fs.getDefaultBlockSize(new Path("/"));
    Path destination = new Path("/destination/file");

    FileStatus status = new FileStatus(4 * maxSplitSize + 1, false, 0, 0, 0, new Path("/file"));

    CopyableFile cf = CopyableFile.builder(fs, status, new Path("/"), new CopyConfiguration(new Path("/"),
        PreserveAttributes.fromMnemonicString(""), new CopyContext(), Optional.<String>absent())).
        destination(destination).build();

    WorkUnit workUnit = new WorkUnit(new Extract(Extract.TableType.APPEND_ONLY, "", ""));
    CopySource.serializeCopyableFile(workUnit, cf);
    CopySource.serializeCopyableDataset(workUnit, new CopyableDatasetMetadata(new DummyDataset(new Path("/")), new Path("/")));
    workUnit.setProp(DistcpFileSplitter.MAX_SPLIT_SIZE_KEY, maxSplitSize);
    workUnit.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, "/writer/output");
    CopySource.setWorkUnitGuid(workUnit, Guid.fromStrings("guid"));

    Collection<WorkUnit> workUnits = DistcpFileSplitter.splitFile(cf, workUnit, fsMock);

    Assert.assertEquals(workUnits.size(), 5);

    boolean[] splitsExist = new boolean[5];
    for (WorkUnit wu : workUnits) {
      Assert.assertTrue(DistcpFileSplitter.isSplitWorkUnit(wu));
      DistcpFileSplitter.Split split = DistcpFileSplitter.getSplit(wu).get();
      Assert.assertEquals(split.getTotalSplits(), 5);
      Assert.assertFalse(splitsExist[split.getSplitNumber()]);
      splitsExist[split.getSplitNumber()] = true;
      int splitNumber = split.getSplitNumber();
      Assert.assertEquals(split.getLowPosition(), maxSplitSize * splitNumber);
      Assert.assertEquals(split.getHighPosition(), maxSplitSize * (splitNumber + 1));
    }
    for (boolean bool : splitsExist) {
      Assert.assertTrue(bool);
    }

    List<WorkUnitState> workUnitStates = Lists.newArrayList();
    for (WorkUnit wu : workUnits) {
      workUnitStates.add(new WorkUnitState(wu));
    }

    Collection<WorkUnitState> outputStates = DistcpFileSplitter.mergeAllSplitWorkUnits(fsMock, workUnitStates);

    verify(fsMock).concat(any(Path.class), Matchers.<Path[]>any());
    Assert.assertEquals(outputStates.size(), 1);
    Assert.assertFalse(DistcpFileSplitter.isSplitWorkUnit(outputStates.iterator().next()));
    Assert.assertEquals(cf, CopySource.deserializeCopyableFile(outputStates.iterator().next()));

  }

}
