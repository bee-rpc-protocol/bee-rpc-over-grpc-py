# Bee-RPC: Enhancing Large Message Handling in gRPC

## Abstract

Bee-RPC is an extension of the gRPC protocol that allows efficient transfer of messages of any size while minimizing the impact on performance. This paper describes the structure of messages in Bee-RPC and how they are managed to optimize the transmission of large data.

## Introduction

The gRPC protocol is used for communication between distributed applications. However, its ability to efficiently transmit large messages can be limited. Bee-RPC addresses this limitation by enabling the transfer of large messages while maintaining optimal performance.

## Messages in Bee-RPC

A message in Bee-RPC is defined using a protobuf message, which includes several key attributes for efficient data transmission:

```protobuf
syntax = "proto3";

package buffer;

message Empty {}

message Buffer  {
    message Head {
        message Partition {
            map<int32, Partition> index = 1;
        }
        int32 index = 1;
        repeated Partition partitions = 2;
    }
    message Block {
        message Hash {
            bytes type = 1;
            bytes value = 2;
        }
        repeated Hash hashes = 1;
        repeated uint64 previous_lengths_position = 2;
    }
    optional bytes chunk = 1;
    optional bool separator = 2;
    optional bool signal = 3;
    optional Head head = 4;
    optional Block block = 5;
    optional Block skip = 6;
}

```

### Structure of a Buffer Message

- **chunk**: A message is divided into one or more fragments, each represented by a `chunk` attribute. Receivers must accumulate these fragments until they encounter a message with the `separator` attribute activated, indicating the end of the current message.
- **signal**: This attribute allows the receiver to inform the sender that it can temporarily stop sending Buffers. This prevents the receiver from storing the buffer in memory if it does not need it at that moment. When the sender receives a Buffer with the `signal` active, it can resume sending.
- **head**: The `head` attribute is used to specify the message's index and define the message's partition. The message index allows the same gRPC method to receive different objects identified by indices in its input and output. This facilitates interoperability between different objects within a single gRPC method.
- **block**: A block is a subset of the buffer associated with a hash identifier. It allows the receiver to request that the sender skip the transmission of certain parts of the buffer if it already has that data.
- **skip**: The request itself, travelling in the opposite direction to `block`: "I already hold this block, stop sending it". It is a separate field from `block` because on an inbound stream `block` already means a block boundary, and a receiver that read a skip request as one would splice the named block's content into whatever it was reconstructing.

### Block Skipping

Block skipping is opt-in per call, and both ends must opt in for it to save anything:

- A **receiver** passes a `StreamControl` to `parse_from_buffer`; it queues a skip request whenever a block that exists locally begins to arrive.
- A **sender** passes the same object to `serialize_to_buffer`; it drops the body of any block the peer has asked it to skip, and emits the block's end marker immediately.
- Over gRPC, a client opts in with `client_grpc(..., block_skip=True)`; a server handler calls `control.watch()` once it has finished parsing its request, so that requests arriving during the response are still seen.

A peer that does not honour skip requests parses them as an unknown field and ignores them, and the transfer completes exactly as it did before.

## Using Blocks (Buffer Containers)

Blocks are a fundamental feature of Bee-RPC for efficient management of large messages:

1. When the receiver receives a Buffer with the `block` attribute, a list of block identifiers and a list of indices of the Protobuf lengths affected by the block are defined.
2. The receiver checks if it already has the buffer on disk. If so, it can skip the transfer of that data.
3. The receiver returns a Buffer to the sender naming that block in the `skip` attribute.
4. The sender receives the request and stops sending the block's content, emitting the block's end marker immediately so that subsequent Buffers are no longer part of the block.
5. The receiver waits to receive that block's end marker to continue accumulating data and paying attention to the content of the following Buffers.

Steps 3 and 4 are best-effort: a sender that ignores the request transmits the block in full, and the receiver drains and discards it as it always has. Because the request cannot be sent before the block's start marker has arrived, some of the block is normally in flight already; the sender stops at the first chunk after the request lands.

### Nested Blocks

It is possible to incorporate blocks within blocks, allowing for finer granularity in data management and transmission optimization.

## Conclusion

Bee-RPC is an extension of the gRPC protocol that enables efficient transfer of messages of any size while maintaining optimal performance. By dividing messages into fragments, using signals, and allowing block management, this extension becomes a valuable tool for applications that require efficient transfer of large data. Its ability to adapt to different indices facilitates interoperability between objects within a single gRPC method, making it a versatile solution for distributed applications.

## References

- [gRPC Website](https://grpc.io/)
- [Protocol Buffers Documentation](https://developers.google.com/protocol-buffers)
- [Protocol Buffers Encoding Specification](https://developers.google.com/protocol-buffers/docs/encoding#simple)
- [Protocol Buffers Encoded Proto Size Limitations](https://protobuf.dev/programming-guides/encoding/#size-limit)
