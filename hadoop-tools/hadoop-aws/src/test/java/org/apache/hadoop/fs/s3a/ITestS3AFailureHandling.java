/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getLandsatCSVPath;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test S3A Failure translation, including a functional test
 * generating errors during stream IO.
 */
public class ITestS3AFailureHandling extends AbstractS3ATestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFailureHandling.class);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setBoolean(Constants.ENABLE_MULTI_DELETE, true);
    return conf;
  }
  @Test
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int originalLength = 8192;
    final byte[] originalDataset = dataset(originalLength, 'a', 32);
    final int newLength = originalLength + 1;
    final byte[] newDataset = dataset(newLength, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, originalDataset, originalDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      // seek forward and read successfully
      instream.seek(1024);
      assertTrue("no data to read", instream.read() >= 0);

      // overwrite
      writeDataset(fs, testpath, newDataset, newDataset.length, 1024, true);
      // here the new file length is larger. Probe the file to see if this is true,
      // with a spin and wait
      eventually(30 * 1000, 1000,
          () -> {
            assertEquals(newLength, fs.getFileStatus(testpath).getLen());
          });

      // With the new file version in place, any subsequent S3 read by eTag will fail.
      // A new read by eTag will occur in reopen() on read after a seek() backwards.  We verify
      // seek backwards results in the expected IOException and seek() forward works without issue.

      // first check seek forward
      instream.seek(2048);
      assertTrue("no data to read", instream.read() >= 0);

      // now check seek backward
      instream.seek(instream.getPos() - 100);
      intercept(IOException.class, "", "read",
          () -> instream.read());

      byte[] buf = new byte[256];

      intercept(IOException.class, "eTag constraint not met", "read",
          () -> instream.read(buf));
      intercept(IOException.class, "eTag constraint not met", "read",
          () -> instream.read(instream.getPos(), buf, 0, buf.length));
      intercept(IOException.class, "eTag constraint not met", "readfully",
          () -> instream.readFully(instream.getPos() - 512, buf));

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      intercept(FileNotFoundException.class, "", "read()",
          () -> instream.read());
      intercept(FileNotFoundException.class, "", "readfully",
          () -> instream.readFully(2048, buf));

    }
  }

  /**
   * Assert that a read operation returned an EOF value.
   * @param operation specific operation
   * @param readResult result
   */
  private void assertIsEOF(String operation, int readResult) {
    assertEquals("Expected EOF from "+ operation
        + "; got char " + (char) readResult, -1, readResult);
  }

  @Test
  public void testMultiObjectDeleteNoFile() throws Throwable {
    describe("Deleting a missing object");
    removeKeys(getFileSystem(), "ITestS3AFailureHandling/missingFile");
  }

  private void removeKeys(S3AFileSystem fileSystem, String... keys)
      throws IOException {
    List<DeleteObjectsRequest.KeyVersion> request = new ArrayList<>(
        keys.length);
    for (String key : keys) {
      request.add(new DeleteObjectsRequest.KeyVersion(key));
    }
    fileSystem.removeKeys(request, false, false);
  }

  @Test
  public void testMultiObjectDeleteSomeFiles() throws Throwable {
    Path valid = path("ITestS3AFailureHandling/validFile");
    touch(getFileSystem(), valid);
    NanoTimer timer = new NanoTimer();
    removeKeys(getFileSystem(), getFileSystem().pathToKey(valid),
        "ITestS3AFailureHandling/missingFile");
    timer.end("removeKeys");
  }

  @Test
  public void testMultiObjectDeleteNoPermissions() throws Throwable {
    Path testFile = getLandsatCSVPath(getConfiguration());
    S3AFileSystem fs = (S3AFileSystem)testFile.getFileSystem(
        getConfiguration());
    intercept(MultiObjectDeleteException.class,
        () -> removeKeys(fs, fs.pathToKey(testFile)));
  }
}
