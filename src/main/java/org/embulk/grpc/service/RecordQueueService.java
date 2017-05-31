package org.embulk.grpc.service;

import com.google.protobuf.Any;
import com.google.protobuf.Api;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.StructProto;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import org.embulk.grpc.ConsumeRequest;
import org.embulk.grpc.ConsumeResponse;
import org.embulk.grpc.ProduceRequest;
import org.embulk.grpc.ProduceResponse;
import org.embulk.grpc.RecordQueueDescribeRequest;
import org.embulk.grpc.RecordQueueDescribeResponse;
import org.embulk.grpc.RecordQueueGrpc;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class RecordQueueService
    extends RecordQueueGrpc.RecordQueueImplBase
{
    private static final Logger logger = Exec.getLogger(RecordQueueService.class);
    @Override
    public void produce(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver)
    {
        logger.warn("getRecord on RecordQueueService#produce: {}", request.getRecord());
        responseObserver.onNext(ProduceResponse.newBuilder()
                .setOffset(1)
                .setSuccess(true)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void consume(ConsumeRequest request, StreamObserver<ConsumeResponse> responseObserver)
    {
        responseObserver.onNext(ConsumeResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void describe(RecordQueueDescribeRequest request, StreamObserver<RecordQueueDescribeResponse> responseObserver)
    {
        RecordQueueDescribeResponse build = RecordQueueDescribeResponse.newBuilder().setApi(Api.getDefaultInstance()).build();
        responseObserver.onNext(build);
        responseObserver.onCompleted();
    }
}
