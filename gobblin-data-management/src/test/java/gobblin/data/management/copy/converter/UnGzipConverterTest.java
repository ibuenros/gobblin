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
package gobblin.data.management.copy.converter;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableFileUtils;
import gobblin.data.management.copy.FileAwareInputStream;

import java.io.InputStream;

import joptsimple.internal.Strings;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;


public class UnGzipConverterTest {

  @DataProvider(name = "fileDataProvider")
  public static Object[][] fileDataProvider() {
    // {filePath, expectedText}
    return new Object[][] { { "unGzipConverterTest/archived.tar.gz", "text" }, { "unGzipConverterTest/archived.tgz", "text" } };
  }

  @Test(dataProvider = "fileDataProvider")
  public void testGz(final String filePath, final String expectedText) throws Exception {

    UnGzipConverter converter = new UnGzipConverter();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    String fullPath = getClass().getClassLoader().getResource(filePath).getFile();
    FileAwareInputStream fileAwareInputStream =
        FileAwareInputStream.builder().file(CopyableFileUtils.getTestCopyableFile(filePath)).
            inputStream(fs.open(new Path(fullPath))).build();

    Iterable<FileAwareInputStream> iterable =
        converter.convertRecord("outputSchema", fileAwareInputStream, new WorkUnitState());

    String actual = readGzipStreamAsString(Iterables.getFirst(iterable, null).getInputStream());
    Assert.assertEquals(actual.trim(), expectedText);

  }

  private static String readGzipStreamAsString(InputStream is) throws Exception {
    TarArchiveInputStream tarIn = new TarArchiveInputStream(is);
    try {
      TarArchiveEntry tarEntry;
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {
        if (tarEntry.isFile() && tarEntry.getName().endsWith(".txt")) {
          return IOUtils.toString(tarIn, "UTF-8");
        }
      }
    } finally {
      tarIn.close();
    }

    return Strings.EMPTY;
  }
}
