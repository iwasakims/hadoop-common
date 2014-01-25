/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tracing;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.cloudera.htrace.HTraceConfiguration;
import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.TraceScope;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestTracing {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @Test
  public void testSpanReceiverHost() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
        SetSpanReceiver.class.getName());
    SpanReceiverHost.init(conf);
  }

  @Test
  public void testWriteTraceHooks() throws Exception {
    TraceScope ts = Trace.startSpan("testWriteTraceHooks", Sampler.ALWAYS);

    Path file = new Path("traceWriteTest.dat");
    long startTime = System.currentTimeMillis();
    FSDataOutputStream stream = dfs.create(file);


    for (int i = 0; i < 10; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(102400).getBytes();
      stream.write(data);
    }
    stream.hflush();
    Thread.sleep(10);
    stream.close();
    Thread.sleep(10);
    ts.close();
    long endTime = System.currentTimeMillis();

    // There should be ~80 but leave some room so that this doesn't
    // have to be edited if anything is changed.
    // 0 is expected if tracing is off so 20 is a good indicator
    // that tracing is working.
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() >= 20);

    String[] expectedSpanNames = {
        "testWriteTraceHooks",
        "DFSOutputStream",
        "DFSOutputStream.write",
        "DFSOutputStream.flushOrSync",
        "DFSOutputStream.close",
    };

    assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
    Span s = map.get("DFSOutputStream").get(0);
    Assert.assertNotNull(s);

    long spanStart = s.getStartTimeMillis();
    long spanEnd = s.getStopTimeMillis();
    Assert.assertTrue(spanStart - 20 < startTime);
    Assert.assertTrue(spanEnd + 20 > endTime);


    // There should only be one trace id as it should all be homed in the
    // top trace.
    for (Span span : SetSpanReceiver.SetHolder.spans) {
      Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
    }
  }

  @Test
  public void testReadTraceHooks() throws Exception {
    String fileName = "traceRead.dat";
    Path filePath = new Path(fileName);

    // Create the file.
    FSDataOutputStream ostream = dfs.create(filePath);
    for (int i = 0; i < 50; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(10240).getBytes();
      ostream.write(data);
    }
    ostream.close();


    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testReadTraceHooks", Sampler.ALWAYS);
    FSDataInputStream istream = dfs.open(filePath, 10240);
    ByteBuffer buf = ByteBuffer.allocate(10240);

    int count = 0;
    try {
      while (istream.read(buf) > 0) {
        count += 1;
        buf.clear();
        istream.seek(istream.getPos() + 5);

      }
    } catch (IOException ioe) {
      // Ignore this it's probably a seek after eof.
    } finally {
      istream.close();
    }
    ts.getSpan().addTimelineAnnotation("count: " + count);
    ts.close();
    long endTime = System.currentTimeMillis();

    String[] expectedSpanNames = {
        "testReadTraceHooks",
        "DFSInputStream",
        "DFSInputStream.read",
        "DFSInputStream.close",
    };

    assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
    Span s = map.get("DFSInputStream").get(0);
    Assert.assertNotNull(s);

    long spanStart = s.getStartTimeMillis();
    long spanEnd = s.getStopTimeMillis();
    Assert.assertTrue(spanStart - 30 < startTime);
    Assert.assertTrue(spanEnd + 10 > endTime);

    Assert.assertTrue(SetSpanReceiver.SetHolder.size() > 10);

    // There should only be one trace id as it should all be homed in the
    // top trace.
    for (Span span : SetSpanReceiver.SetHolder.spans) {
      Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
    }
  }

  @Before
  public void cleanSet() {
    SetSpanReceiver.SetHolder.spans.clear();
  }

  @BeforeClass
  public static void setupCluster() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
        SetSpanReceiver.class.getName());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();

    dfs = cluster.getFileSystem();
  }

  @AfterClass
  public static void shutDown() throws IOException {
    cluster.shutdown();
  }

  private void assertSpanNamesFound(String[] expectedSpanNames) {
    Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
    for (String spanName : expectedSpanNames) {
      Assert.assertTrue("Should find a span with name " + spanName, map.get(spanName) != null);
    }
  }

  /**
   * Span receiver that puts all spans into a single set.
   * This is useful for testing.
   * <p/>
   * We're not using HTrace's POJOReceiver here so as that doesn't
   * push all the metrics to a static place, and would make testing
   * SpanReceiverHost harder.
   */
  public static class SetSpanReceiver implements SpanReceiver {

    public void configure(HTraceConfiguration conf) {
    }

    public void receiveSpan(Span span) {
      SetHolder.spans.add(span);
    }

    public void close() {
    }

    public static class SetHolder {
      public static Set<Span> spans = new HashSet<Span>();

      public static int size() {
        return spans.size();
      }

      public static Map<String, List<Span>> getMap() {
        Map<String, List<Span>> map = new HashMap<String, List<Span>>();

        for (Span s : spans) {
          List<Span> l = map.get(s.getDescription());
          if (l == null) {
            l = new LinkedList<Span>();
            map.put(s.getDescription(), l);
          }
          l.add(s);
        }
        return map;
      }
    }
  }
}