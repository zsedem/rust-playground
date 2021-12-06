use futures::stream::{StreamExt};
use futures::stream::select;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, TopicPartitionList};
use rdkafka::message::{OwnedMessage};
use rdkafka::Offset;
use rdkafka::util::Timeout;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use futures::prelude::stream::Map;
use tokio::time;

#[tokio::main]
async fn main() {
  let brokers = String::from("kafka:9092");
  let group_id = String::from("rust-test-group");
  let input_topic = String::from("test-topic");
  let output_topic = String::from("rust-output-topic");

  let (consumed_messages, producer_input_messages) = mpsc::channel(4096);
  let (commit_messages_sender, commit_messages_receiver) = mpsc::channel(4096);
  tokio::spawn(run_consumer(
    brokers.clone(),
    group_id.clone(),
    input_topic.clone(),
    consumed_messages.clone(),
    commit_messages_receiver
  ));

  tokio::spawn(run_producer(
    brokers,
    output_topic,
    producer_input_messages,
    commit_messages_sender
  ));
  consumed_messages.closed().await
}


async fn run_consumer(
  brokers: String,
  group_id: String,
  input_topic: String,
  consumer_messages_sender: Sender<OwnedMessage>,
  commits_receiver: Receiver<OwnedMessage>
) {
  let consumer: StreamConsumer = ClientConfig::new()
      .set("group.id", &group_id)
      .set("bootstrap.servers", &brokers)
      .set("enable.partition.eof", "false")
      .set("session.timeout.ms", "6000")
      .set("enable.auto.commit", "false")
      .create()
      .expect("Consumer creation failed");

  consumer
      .subscribe(&[&input_topic])
      .expect("Can't subscribe to specified topic");

  let consuming_stream = consumer.stream().map(|x| ConsumerThreadTrigger::MessageToConsume(x.expect("Kafka consumer error").detach()));
  let commits_stream = ReceiverStream::new(commits_receiver).map(|x| ConsumerThreadTrigger::MessageDoneProcessing(x));
  let commits_trigger_stream = create_clock();

  let mut consumer_stream = select(consuming_stream, select(commits_stream, commits_trigger_stream));
  let mut offsets = TopicPartitionList::new();

  while let Some(trigger) = consumer_stream.next().await {
    match trigger {
      ConsumerThreadTrigger::MessageToConsume(msg) => {
        consumer_messages_sender.send(msg).await.expect("internal channel error");
      }
      ConsumerThreadTrigger::MessageDoneProcessing(x) => {
        offsets.add_partition_offset(x.topic(), x.partition(), Offset::Offset(x.offset())).expect("error while updating commit offset batch");
      }
      ConsumerThreadTrigger::TimerEvent => {
        if offsets.count() != 0 {
          consumer.commit(&offsets, CommitMode::Async).expect("error while committing");
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
  commit_messages_sender: Sender<OwnedMessage>
) {
  let producer: FutureProducer = ClientConfig::new()
      .set("bootstrap.servers", &brokers)
      .set("message.timeout.ms", "5000")
      .set("linger.ms", "2000")
      .set("batch.size", "131072")
      .create()
      .expect("Producer creation error");

  while let Some(owned_message) = consumed_messages_receiver.recv().await {
    tokio::spawn(handle_message(output_topic.clone(), producer.clone(), owned_message, commit_messages_sender.clone()));
  }
}

async fn handle_message(
  output_topic: String,
  producer: FutureProducer,
  owned_message: OwnedMessage,
  commit_messages_sender: Sender<OwnedMessage>
) {
  let mut record: FutureRecord<[u8], [u8]> = FutureRecord::to(&output_topic);
  if owned_message.key().is_some() {
    record = record.key(owned_message.key().expect("internalerror"));
  }
  if owned_message.payload().is_some() {
    record = record.payload(owned_message.payload().expect("internalerror"));
  }
  producer.send(
    record,
    Timeout::Never,
  ).await.expect("producer error");
  commit_messages_sender.send(owned_message).await.expect("internal channel error on commits");
}

fn create_clock() -> Map<IntervalStream, fn(tokio::time::Instant) -> ConsumerThreadTrigger> {
  let commit_interval_ms = 1000;
  let interval = time::interval(time::Duration::from_millis(commit_interval_ms));
  IntervalStream::new(interval).map(|_x| ConsumerThreadTrigger::TimerEvent)
}

enum ConsumerThreadTrigger {
  MessageToConsume(OwnedMessage),
  MessageDoneProcessing(OwnedMessage),
  TimerEvent
}

