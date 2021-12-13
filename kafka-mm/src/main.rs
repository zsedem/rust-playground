use futures::prelude::stream::Map;
use futures::stream::select;
use futures::stream::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::error::KafkaError;
use futures::future::Either::{Right, Left};
use rdkafka::message::{OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::Offset;
use rdkafka::{Message, TopicPartitionList};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time;
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use clap::{Arg, App, AppSettings};

#[tokio::main]
async fn main() {
    let matches = App::new("Kafka Mirrormaker")
        .version("0.1.0")
        .about("Mirrors from one kafka to another")
        .setting(AppSettings::ColorAuto)
        .arg(Arg::with_name("source-bootstrap-servers")
            .long("source-bootstrap-servers")
            .value_name("KAFKA_BOOTSTRAP_SERVERS")
            .help("Set to a kafka bootstrap server, where you read the topic from")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("source-kafka-topic")
            .long("source-kafka-topic")
            .value_name("KAFKA_TOPIC")
            .help("Set the topic to mirror from")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("consumer-group-id")
            .long("consumer-group-id")
            .value_name("CONSUMER_GROUP")
            .help("Set the topic to mirror from")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("target-bootstrap-servers")
            .long("target-bootstrap-servers")
            .value_name("KAFKA_BOOTSTRAP_SERVERS")
            .help("Set to a kafka bootstrap server, where you write the topic")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("target-kafka-topic")
            .long("target-kafka-topic")
            .value_name("KAFKA_TOPIC")
            .help("Set the topic to mirror to")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("commit-interval-ms")
            .long("commit-interval-ms")
            .value_name("MILLIS")
            .help("Commit interval milliseconds")
            .takes_value(true)
            .default_value("1000"))
        .arg(Arg::with_name("auto-offset-reset")
            .long("auto-offset-reset")
            .value_name("RESET_OPTION")
            .help("kafka auto.offset.reset option")
            .takes_value(true)
            .possible_values(&vec!["earliest", "latest"])
            .default_value("earliest"))
        .arg(Arg::with_name("compression-type")
            .long("compression-type")
            .value_name("COMPRESSION_TYPE")
            .help("Compression type to choose")
            .possible_values(&vec!["none", "gzip", "snappy", "lz4", "zstd"])
            .takes_value(true)
            .default_value("none"))
        .arg(Arg::with_name("batch-size")
            .long("batch-size")
            .value_name("BATCH_SIZE")
            .help("Batch size to reach for a batch to be written before linger ms time elapsed")
            .takes_value(true)
            .default_value("786432"))
        .arg(Arg::with_name("batch-linger-ms")
            .long("batch-linger-ms")
            .value_name("BATCH_LINGER_MS")
            .help("Milliseconds to wait for messages to be in a single batch")
            .takes_value(true)
            .default_value("1000"))
        .get_matches();


    let consumer_brokers = String::from(matches.value_of("source-bootstrap-servers").expect("ArgumentError \"source-bootstrap-servers\""));
    let producer_brokers = String::from(matches.value_of("target-bootstrap-servers").expect("ArgumentError \"target-bootstrap-servers\""));
    let group_id = String::from(matches.value_of("consumer-group-id").expect("ArgumentError \"consumer-group-id\""));
    let input_topic = String::from(matches.value_of("source-kafka-topic").expect("ArgumentError \"source-kafka-topic\""));
    let output_topic = String::from(matches.value_of("target-kafka-topic").expect("ArgumentError \"target-kafka-topic\""));
    let commit_interval_ms = matches.value_of("commit-interval-ms").expect("ArgumentError \"commit-interval-ms\"").parse()
        .expect("Commit interval milliseconds must be a positive number");
    let auto_offset_reset =  String::from(matches.value_of("auto-offset-reset").expect("ArgumentError \"auto-offset-reset\""));
    let compression_type = matches.value_of("compression-type").expect("ArgumentError \"compression-type\"").to_string();
    let batch_size = matches.value_of("batch-size").expect("ArgumentError \"batch-size\"").to_string();
    let batch_linger_ms = matches.value_of("batch-linger-ms").expect("ArgumentError \"batch-linger-ms\"").to_string();

    let (consumed_messages, producer_input_messages) = mpsc::channel(256);
    let (commit_messages_sender, commit_messages_receiver) = mpsc::channel(256);

    tokio::spawn(run_producer(
        ProducerSettings {
            topic: output_topic,
            bootstrap_servers: producer_brokers,
            compression_type,
            batch_size,
            batch_linger_ms
        },
        producer_input_messages,
        commit_messages_sender,
    ));

    run_consumer(
        ConsumerSettings {
            topic: input_topic,
            bootstrap_servers: consumer_brokers,
            group_id,
            commit_interval_ms,
            auto_offset_reset
        },
        consumed_messages,
        commit_messages_receiver,
    ).await;
}

async fn run_consumer(
    consumer_settings: ConsumerSettings,
    consumer_messages_sender: Sender<OwnedMessage>,
    commits_receiver: Receiver<OwnedMessage>,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &consumer_settings.group_id)
        .set("bootstrap.servers", &consumer_settings.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", &consumer_settings.auto_offset_reset)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&consumer_settings.topic])
        .expect("Can't subscribe to specified topic");

    let consuming_stream = consumer.stream().map(|x| {
        ConsumerThreadTrigger::MessageToConsume(x.expect("Kafka consumer error").detach())
    });
    let commits_stream = ReceiverStream::new(commits_receiver)
        .map(|x| ConsumerThreadTrigger::MessageDoneProcessing(x));
    let commits_trigger_stream = create_periodic_commit_trigger(consumer_settings.commit_interval_ms);

    let mut consumer_stream = {
        select(
            consuming_stream,
            select(commits_stream, commits_trigger_stream),
        )
    };
    let mut offsets = TopicPartitionList::new();

    while let Some(trig) = consumer_stream.next().await {
        match trig {
            ConsumerThreadTrigger::MessageToConsume(msg) => {
                consumer_messages_sender
                    .send(msg)
                    .await
                    .expect("internal channel error");
            }
            ConsumerThreadTrigger::MessageDoneProcessing(x) => {
                let offset_to_consume_from = x.offset() + 1;
                offsets
                    .add_partition_offset(x.topic(), x.partition(), Offset::Offset(offset_to_consume_from))
                    .expect("error while updating commit offset batch");
            }
            ConsumerThreadTrigger::TimerEvent => {
                if offsets.count() != 0 {
                    consumer
                        .commit(&offsets, CommitMode::Async)
                        .expect("error while committing");
                    offsets = TopicPartitionList::new()
                }
            }
        }
    }
}

async fn run_producer(
    producer_settings: ProducerSettings,
    consumed_messages_receiver: Receiver<OwnedMessage>,
    commit_messages_sender: Sender<OwnedMessage>,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &producer_settings.bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("linger.ms", &producer_settings.batch_linger_ms)
        .set("batch.size", &producer_settings.batch_size)
        .set("compression.type", &producer_settings.compression_type)
        .create()
        .expect("Producer creation error");

    let (producer_died_sender, producer_died_receiver) = mpsc::channel(1);
    let mut producer_events_receiver = select(
        ReceiverStream::new(producer_died_receiver).map(|x| Left(x)),
        ReceiverStream::new(consumed_messages_receiver).map(|x| Right(x))
    );

    while let Some(message) = producer_events_receiver.next().await {
        match message {
            Right(owned_message) => {
                tokio::spawn(handle_message(
                    producer_settings.topic.clone(),
                    producer.clone(),
                    owned_message,
                    commit_messages_sender.clone(),
                    producer_died_sender.clone()
                ));
            }
            Left((err, owned_message)) => {
                panic!(
                    "Error while producing message (partition: {}, offset: {}): {}",
                    owned_message.partition(),
                    owned_message.offset(),
                    err
                );
            }
        }
    }
}

async fn handle_message(
    output_topic: String,
    producer: FutureProducer,
    owned_message: OwnedMessage,
    commit_messages_sender: Sender<OwnedMessage>,
    producer_died_sender: Sender<(KafkaError, OwnedMessage)>,
) {
    let mut record: FutureRecord<[u8], [u8]> = FutureRecord::to(&output_topic);
    if owned_message.key().is_some() {
        record = record.key(owned_message.key().expect("internalerror"));
    }
    if owned_message.payload().is_some() {
        record = record.payload(owned_message.payload().expect("internalerror"));
    }
    let r = producer
        .send(record, Timeout::Never)
        .await;
    match r {
        Err(e) => {
            producer_died_sender.send(e.clone()).await.expect("producer message failure failed to send");
        }
        Ok(_) => {
            commit_messages_sender
                .send(owned_message)
                .await
                .expect("internal channel error on commits");
        }
    }
}

fn create_periodic_commit_trigger(commit_interval_ms: u64) -> Map<IntervalStream, fn(tokio::time::Instant) -> ConsumerThreadTrigger> {
    let interval = time::interval(time::Duration::from_millis(commit_interval_ms));
    IntervalStream::new(interval).map(|_x| ConsumerThreadTrigger::TimerEvent)
}
#[derive(Debug)]
enum ConsumerThreadTrigger {
    MessageToConsume(OwnedMessage),
    MessageDoneProcessing(OwnedMessage),
    TimerEvent,
}

struct ProducerSettings {
    topic: String,
    bootstrap_servers: String,
    compression_type: String,
    batch_size: String,
    batch_linger_ms: String
}

struct ConsumerSettings {
    topic: String,
    bootstrap_servers: String,
    group_id: String,
    commit_interval_ms: u64,
    auto_offset_reset: String
}
