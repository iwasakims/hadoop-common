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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.toProto;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpRequestShortCircuitAccessProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.Message;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.TraceScope;

/** Sender */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Sender implements DataTransferProtocol {
  private final DataOutputStream out;
  private final Span parentSpan;

  public Sender(final DataOutputStream out, Span parentSpan) {
    this.out = out;
    this.parentSpan = parentSpan;
  }

  /** Create a sender for DataTransferProtocol with a output stream. */
  public Sender(final DataOutputStream out) {
    this(out, null);
  }

  /** Initialize a operation. */
  private static void op(final DataOutput out, final Op op
      ) throws IOException {
    out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    op.write(out);
  }

  private static void send(final DataOutputStream out, final Op opcode,
      final Message proto) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sending DataTransferOp " + proto.getClass().getSimpleName()
          + ": " + proto);
    }
    op(out, opcode);
    proto.writeDelimitedTo(out);
    out.flush();
  }

  static private CachingStrategyProto getCachingStrategy(CachingStrategy cachingStrategy) {
    CachingStrategyProto.Builder builder = CachingStrategyProto.newBuilder();
    if (cachingStrategy.getReadahead() != null) {
      builder.setReadahead(cachingStrategy.getReadahead().longValue());
    }
    if (cachingStrategy.getDropBehind() != null) {
      builder.setDropBehind(cachingStrategy.getDropBehind().booleanValue());
    }
    return builder.build();
  }

  @Override
  public void readBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException {
    TraceScope ts = null;
    if (parentSpan != null) {
      ts = Trace.continueSpan(parentSpan.child("Sender.readBlock"));
    }

    OpReadBlockProto proto = OpReadBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildClientHeader(blk, clientName, blockToken))
      .setOffset(blockOffset)
      .setLen(length)
      .setSendChecksums(sendChecksum)
      .setCachingStrategy(getCachingStrategy(cachingStrategy))
      .build();

    try {
      send(out, Op.READ_BLOCK, proto);
    } finally {
      if (ts != null) ts.close();
    }
  }
  

  @Override
  public void writeBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final DatanodeInfo source,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      DataChecksum requestedChecksum,
      final CachingStrategy cachingStrategy) throws IOException {
    TraceScope ts = null;
    if (parentSpan != null) {
      ts = Trace.continueSpan(parentSpan.child("Sender.writeBlock"));
    }

    ClientOperationHeaderProto header = DataTransferProtoUtil.buildClientHeader(
        blk, clientName, blockToken);

    ChecksumProto checksumProto =
      DataTransferProtoUtil.toProto(requestedChecksum);
    try {
      OpWriteBlockProto.Builder proto = OpWriteBlockProto.newBuilder()
        .setHeader(header)
        .addAllTargets(PBHelper.convert(targets, 1))
        .setStage(toProto(stage))
        .setPipelineSize(pipelineSize)
        .setMinBytesRcvd(minBytesRcvd)
        .setMaxBytesRcvd(maxBytesRcvd)
        .setLatestGenerationStamp(latestGenerationStamp)
        .setRequestedChecksum(checksumProto)
        .setCachingStrategy(getCachingStrategy(cachingStrategy));

      if (source != null) {
        proto.setSource(PBHelper.convertDatanodeInfo(source));
      }
      send(out, Op.WRITE_BLOCK, proto.build());
      } finally {
      if (ts != null) ts.close();
    }
  }

  @Override
  public void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets) throws IOException {
    TraceScope ts = null;
    if (parentSpan != null) {
      ts = Trace.continueSpan(parentSpan.child("Sender.transferBlock"));
    }

    OpTransferBlockProto proto = OpTransferBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildClientHeader(
          blk, clientName, blockToken))
      .addAllTargets(PBHelper.convert(targets))
      .build();

    try {
      send(out, Op.TRANSFER_BLOCK, proto);
    } finally {
      if (ts != null) ts.close();
    }
  }

  @Override
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      int maxVersion) throws IOException {
    TraceScope ts = null;
    if (parentSpan != null) {
      ts = Trace.continueSpan(parentSpan.child("Sender.requestShortCircuitFds"));
    }

    OpRequestShortCircuitAccessProto proto =
        OpRequestShortCircuitAccessProto.newBuilder()
          .setHeader(DataTransferProtoUtil.buildBaseHeader(
            blk, blockToken)).setMaxVersion(maxVersion).build();
    try {
      send(out, Op.REQUEST_SHORT_CIRCUIT_FDS, proto);
    } finally {
      if (ts != null) ts.close();
    }
  }
  
  @Override
  public void replaceBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo source) throws IOException {
    TraceScope ts = null;
    if (parentSpan != null) {
      ts = Trace.continueSpan(parentSpan.child("Sender.replaceBlock"));
    }

    OpReplaceBlockProto proto = OpReplaceBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
      .setDelHint(delHint)
      .setSource(PBHelper.convertDatanodeInfo(source))
      .build();

    try {
      send(out, Op.REPLACE_BLOCK, proto);
    } finally {
      if (ts != null) ts.close();
    }
  }

  @Override
  public void copyBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    TraceScope ts = null;
    if (Trace.isTracing()) {
      ts = Trace.startSpan("Sender.copyBlock");
    }

    OpCopyBlockProto proto = OpCopyBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
      .build();

    try {
      send(out, Op.COPY_BLOCK, proto);
    } finally {
      if (ts != null) ts.close();
    }
  }

  @Override
  public void blockChecksum(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    TraceScope ts = null;
    if (parentSpan != null) {
      ts = Trace.continueSpan(parentSpan.child("Sender.blockChecksum"));
    }

    OpBlockChecksumProto proto = OpBlockChecksumProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
      .build();
    try {
      send(out, Op.BLOCK_CHECKSUM, proto);
    } finally {
      if (ts != null) ts.close();
    }
  }
}
