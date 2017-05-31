package org.embulk.grpc;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.3.0)",
    comments = "Source: record_queue.proto")
public final class RecordQueueGrpc {

  private RecordQueueGrpc() {}

  public static final String SERVICE_NAME = "record_queue.RecordQueue";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<org.embulk.grpc.ProduceRequest,
      org.embulk.grpc.ProduceResponse> METHOD_PRODUCE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "record_queue.RecordQueue", "Produce"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.embulk.grpc.ProduceRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.embulk.grpc.ProduceResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<org.embulk.grpc.ConsumeRequest,
      org.embulk.grpc.ConsumeResponse> METHOD_CONSUME =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "record_queue.RecordQueue", "Consume"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.embulk.grpc.ConsumeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.embulk.grpc.ConsumeResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<org.embulk.grpc.RecordQueueDescribeRequest,
      org.embulk.grpc.RecordQueueDescribeResponse> METHOD_DESCRIBE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "record_queue.RecordQueue", "Describe"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.embulk.grpc.RecordQueueDescribeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.embulk.grpc.RecordQueueDescribeResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RecordQueueStub newStub(io.grpc.Channel channel) {
    return new RecordQueueStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RecordQueueBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RecordQueueBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static RecordQueueFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RecordQueueFutureStub(channel);
  }

  /**
   */
  public static abstract class RecordQueueImplBase implements io.grpc.BindableService {

    /**
     */
    public void produce(org.embulk.grpc.ProduceRequest request,
        io.grpc.stub.StreamObserver<org.embulk.grpc.ProduceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PRODUCE, responseObserver);
    }

    /**
     */
    public void consume(org.embulk.grpc.ConsumeRequest request,
        io.grpc.stub.StreamObserver<org.embulk.grpc.ConsumeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CONSUME, responseObserver);
    }

    /**
     */
    public void describe(org.embulk.grpc.RecordQueueDescribeRequest request,
        io.grpc.stub.StreamObserver<org.embulk.grpc.RecordQueueDescribeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DESCRIBE, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_PRODUCE,
            asyncUnaryCall(
              new MethodHandlers<
                org.embulk.grpc.ProduceRequest,
                org.embulk.grpc.ProduceResponse>(
                  this, METHODID_PRODUCE)))
          .addMethod(
            METHOD_CONSUME,
            asyncUnaryCall(
              new MethodHandlers<
                org.embulk.grpc.ConsumeRequest,
                org.embulk.grpc.ConsumeResponse>(
                  this, METHODID_CONSUME)))
          .addMethod(
            METHOD_DESCRIBE,
            asyncUnaryCall(
              new MethodHandlers<
                org.embulk.grpc.RecordQueueDescribeRequest,
                org.embulk.grpc.RecordQueueDescribeResponse>(
                  this, METHODID_DESCRIBE)))
          .build();
    }
  }

  /**
   */
  public static final class RecordQueueStub extends io.grpc.stub.AbstractStub<RecordQueueStub> {
    private RecordQueueStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RecordQueueStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordQueueStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RecordQueueStub(channel, callOptions);
    }

    /**
     */
    public void produce(org.embulk.grpc.ProduceRequest request,
        io.grpc.stub.StreamObserver<org.embulk.grpc.ProduceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PRODUCE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void consume(org.embulk.grpc.ConsumeRequest request,
        io.grpc.stub.StreamObserver<org.embulk.grpc.ConsumeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CONSUME, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void describe(org.embulk.grpc.RecordQueueDescribeRequest request,
        io.grpc.stub.StreamObserver<org.embulk.grpc.RecordQueueDescribeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DESCRIBE, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RecordQueueBlockingStub extends io.grpc.stub.AbstractStub<RecordQueueBlockingStub> {
    private RecordQueueBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RecordQueueBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordQueueBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RecordQueueBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.embulk.grpc.ProduceResponse produce(org.embulk.grpc.ProduceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PRODUCE, getCallOptions(), request);
    }

    /**
     */
    public org.embulk.grpc.ConsumeResponse consume(org.embulk.grpc.ConsumeRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CONSUME, getCallOptions(), request);
    }

    /**
     */
    public org.embulk.grpc.RecordQueueDescribeResponse describe(org.embulk.grpc.RecordQueueDescribeRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DESCRIBE, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RecordQueueFutureStub extends io.grpc.stub.AbstractStub<RecordQueueFutureStub> {
    private RecordQueueFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RecordQueueFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordQueueFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RecordQueueFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.embulk.grpc.ProduceResponse> produce(
        org.embulk.grpc.ProduceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PRODUCE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.embulk.grpc.ConsumeResponse> consume(
        org.embulk.grpc.ConsumeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CONSUME, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.embulk.grpc.RecordQueueDescribeResponse> describe(
        org.embulk.grpc.RecordQueueDescribeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DESCRIBE, getCallOptions()), request);
    }
  }

  private static final int METHODID_PRODUCE = 0;
  private static final int METHODID_CONSUME = 1;
  private static final int METHODID_DESCRIBE = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RecordQueueImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RecordQueueImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PRODUCE:
          serviceImpl.produce((org.embulk.grpc.ProduceRequest) request,
              (io.grpc.stub.StreamObserver<org.embulk.grpc.ProduceResponse>) responseObserver);
          break;
        case METHODID_CONSUME:
          serviceImpl.consume((org.embulk.grpc.ConsumeRequest) request,
              (io.grpc.stub.StreamObserver<org.embulk.grpc.ConsumeResponse>) responseObserver);
          break;
        case METHODID_DESCRIBE:
          serviceImpl.describe((org.embulk.grpc.RecordQueueDescribeRequest) request,
              (io.grpc.stub.StreamObserver<org.embulk.grpc.RecordQueueDescribeResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class RecordQueueDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.embulk.grpc.RecordQueueProto.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RecordQueueGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RecordQueueDescriptorSupplier())
              .addMethod(METHOD_PRODUCE)
              .addMethod(METHOD_CONSUME)
              .addMethod(METHOD_DESCRIBE)
              .build();
        }
      }
    }
    return result;
  }
}
