# StreamFusion Connectors

## Introduction

In our [last blog post](streaming-for-the-ai-age.md), we discussed StreamFusion, a new vectorized Flink-compatible SQL engine
built on top of DataFusion! In this one, we’ll discuss extending StreamFusion to read from and
write to a variety of external systems and formats.

As of the time of authoring this blog post, StreamFusion currently supports source and sink
operators for Kafka (avro, json, protobuf), Iceberg, Paimon, Fluss, Delta, and even plain
parquet files! In order to build these, I’ve had to figure out a way to be able to
safely/quickly iterate while keeping our surface area for bugs minimized. I’ve been heavily
relying on AI to do much of the development of StreamFusion, which as we all know can get out of
hand if not given the proper guardrails.

## RowData In, RowData Out

The most simple approach for supporting native connectors in StreamFusion is to *not do anything*.
Each Flink source returns some RowData and each sink accepts RowData to persist. A simple
utility function to convert between RowData and arrow buffers, and vice versa, would be enough
to trivially support each Flink source and sink in StreamFusion. In fact, one could even go a
step further on the sink side, and have StreamFusion create a “ArrowBackedRowData” class which
implements the RowData interface to avoid paying the penalty of transposing columnar arrow data
to row oriented RowData. We do indeed support this as a fallback for any unimplemented
connectors.

As you might predict though, converting externally stored data into a Flink internal model,
which we then proceed to convert back to arrow buffers, can be extremely slow. Let’s imagine
reading from an Apache Fluss broker, which internally stores its write ahead log in arrow
format. We’d have to transpose our data from columns to rows back to columns to process it. For
connectors which expose/receive columnar data (e.g. parquet files, table formats, arrow/ADBC
endpoints), we see a major benefit from building something a bit more bespoke. Even for ones
that don’t, performance is still empirically better when we avoid RowData as an intermediate
conversion.

## Rust Is A Honeypot

When building out a new StreamFusion connector for a new external service, it can be (and was
initially for me) very tempting to immediately use its rust client library. As a concrete
example, let’s imagine building a kafka data source in StreamFusion. One might first look to
depend on rust-rdkafka. There is definitely overhead to copy data on the Java heap into an off
heap memory buffer and then call rust functions on it via JNI. So this seems like a good way to
achieve maximum performance, right?

The reality isn’t so great. Recall that StreamFusion aims to provide identical behavior to
Flink, not just in query results, but also in interactions with external systems. Digging a bit
deeper, you might realize that the consumer/producer defaults between kafka’s java and rust
client library highly differ (different read committed isolation levels and checksum handling by
default, for example) and you’re stuck having to dig through all of those and translate them
manually. Configuration keys can also have different names. More significantly, kafka’s java
client supports consumer side buffer configuration that the rust one does not, which could
impact the health of the broker.

When I was building the StreamFusion kafka connector via the rust library, I found myself
worrying more about these small details - distracting me from just trying to get the best
performance possible. To be clear, I know that in the ultra-long term, building a fully native
connector is the right thing to do. But in this project’s infancy, I care more about drawing new
users to StreamFusion (via diversified connector support) while allowing myself to sleep at
night over kafka configuration.

## A Hybrid Solution

As fun as it sounds to try rebuilding the entirety of Flink in Rust, besides just rearchitecting
its execution model, doing so actually has very little impact on job throughput. It doesn’t
benefit me much to have rust handling my HTTP connections to kafka instead of java, but it does
really benefit me to have parquet-rust converting parquet files to arrow buffers instead of
parquet-java (major SIMD differences, community support at this point).

To that end, I’ve tried to think critically about where native code fits into StreamFusion and
where it doesn’t. At this time, I’m trying to follow five main tenets:

1. **Minimize data transpose** - once data gets converted to arrow buffers, I want to keep it that
   way as long as possible
2. **Native code is for data operations** - native code belongs in the execution path, and the
   execution path *only*

    1. Writing data files in an iceberg table
    2. Joining two separate data streams, aggregating, projection, filtering (datafusion
       operations)
    3. Serializing and deserializing data to and from arrow buffers from their formats in
       external systems (e.g. avro to arrow from a kafka broker)

3. **Identical interactions with external systems** - switching to StreamFusion shouldn’t run a risk
   of messing with your existing infrastructure due to an unforeseen configuration change
4. **Minimize surface area for bugs** - while it seems great to use client libraries in rust as
   opposed to java, functionality is often not identical
5. **Make sure we pass existing connector tests**

How do we achieve these goals? Fortunately it’s not too hard! We can slightly modify existing
flink source and sink to do just about everything the same except for touching *data*. I’ll
continue to use kafka as an example.

For a kafka source, we can continue to reuse identical Flink client code for connecting to the
broker, enumerating topics and partitions to read from, polling the bytes from the brokers
themselves, and committing offsets. The one place where rust code jumps in is translating those
bytes on the java heap to arrow buffers. We’ll have to copy the data once (at most), but
relative to serialization costs, this is a very small price to pay for code simplicity and peace
of mind.

On the sink side, native code is once again responsible for serializing arrow buffers to avro,
JSON, or ProtoBuf, and passing those bytes back to the existing Java client library which
handles all transaction publishing machinery. This ensures that any existing user configuration
for a job continues to work as intended, and native code only touches the part of the job which
really consumes a lot of CPU time (the data transpose from columnar to row-wise and subsequent
serialization).

## Conclusion

In theory, StreamFusion *could* be faster, which is a hard truth for me to swallow. However, in
this early stage of the product what matters more to me than speed is trust. Not only do I need
early users of StreamFusion to trust that I’m competent enough to manage the project, but also I
also need to trust StreamFusion to do what it’s supposed to without meticulously reviewing every
line of almost 2000 commits.
