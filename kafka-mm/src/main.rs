use futures::prelude::stream::Map;
use futures::stream::select;
use futures::stream::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
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
        .arg(Arg::with_name("source-kafka")
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
        .arg(Arg::with_name("target-kafka")
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
        .get_matches();


    let consumer_brokers = String::from(matches.value_of("source-bootstrap-servers").expect("ArgumentError"));
    let producer_brokers = String::from(matches.value_of("target-bootstrap-servers").expect("ArgumentError"));
    let group_id = String::from(matches.value_of("consumer-group-id").expect("ArgumentError"));
    let input_topic = String::from(matches.value_of("source-kafka-topic").expect("ArgumentError"));
    let output_topic = String::from(matches.value_of("target-bootstrap-servers").expect("ArgumentError"));
    let commit_interval_ms = matches.value_of("commit-interval-ms").expect("ArgumentError").parse()
        .expect("Commit interval milliseconds must be a positive number");
    let auto_offset_reset =  String::from(matches.value_of("auto-offset-reset").expect("ArgumentError"));

    let (consumed_messages, producer_input_messages) = mpsc::channel(256);
    let (commit_messages_sender, commit_messages_receiver) = mpsc::channel(256);

    tokio::spawn(run_producer(
        producer_brokers,
        output_topic,
        producer_input_messages,
        commit_messages_sender,
    ));

    run_consumer(
        consumer_brokers,
        group_id,
        input_topic,
        commit_interval_ms,
        auto_offset_reset,
        consumed_messages,
        commit_messages_receiver,
    ).await;
}

async fn run_consumer(
    brokers: String,
    group_id: String,
    input_topic: String,
    commit_interval_ms: u64,
    auto_offset_reset: String,
    consumer_messages_sender: Sender<OwnedMessage>,
    commits_receiver: Receiver<OwnedMessage>,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", auto_offset_reset)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    let consuming_stream = consumer.stream().map(|x| {
        ConsumerThreadTrigger::MessageToConsume(x.expect("Kafka consumer error").detach())
    });
    let commits_stream = ReceiverStream::new(commits_receiver)
        .map(|x| ConsumerThreadTrigger::MessageDoneProcessing(x));
    let commits_trigger_stream = create_periodic_commit_trigger(commit_interval_ms);

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
    brokers: String,
    output_topic: String,
    mut consumed_messages_receiver: Receiver<OwnedMessage>,
    commit_messages_sender: Sender<OwnedMessage>,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .set("linger.ms", "2000")
        .set("batch.size", "131072")
        .create()
        .expect("Producer creation error");

    while let Some(owned_message) = consumed_messages_receiver.recv().await {
        tokio::spawn(handle_message(
            output_topic.clone(),
            producer.clone(),
            owned_message,
            commit_messages_sender.clone(),
        ));
    }
}

async fn handle_message(
    output_topic: String,
    producer: FutureProducer,
    owned_message: OwnedMessage,
    commit_messages_sender: Sender<OwnedMessage>,
) {
    let mut record: FutureRecord<[u8], [u8]> = FutureRecord::to(&output_topic);
    if owned_message.key().is_some() {
        record = record.key(owned_message.key().expect("internalerror"));
    }
    if owned_message.payload().is_some() {
        record = record.payload(owned_message.payload().expect("internalerror"));
    }
    producer
        .send(record, Timeout::Never)
        .await
        .expect("producer error");
    commit_messages_sender
        .send(owned_message)
        .await
        .expect("internal channel error on commits");
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
