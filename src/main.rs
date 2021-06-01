/*
 * Includes
 */
use serde::{Serialize, Deserialize};
use tracing::{Level, event, instrument};
use tracing_subscriber::fmt::format::FmtSpan;
use lapin;

const RABBITMQ_ADDR: &str = "amqp://127.0.0.1:5672/%2f";

#[derive(Serialize, Deserialize, Debug)]
struct RabbitIncomingMessage {
    account_sid: String,
    call_sid: String
}

#[derive(Serialize, Deserialize, Debug)]
struct RabbitResponseMessage {
    content: String
}

#[tokio::main]
#[instrument]
async fn main() -> Result<(), lapin::Error> {
    event!(Level::INFO, "Setting up consumer");
    let connection = lapin::Connection::connect(
        RABBITMQ_ADDR,
        lapin::ConnectionProperties::default().with_default_executor(8)
    ).await?;

    let channel = connection.create_channel().await?;

    let consumer = channel.basic_consume(
        "incoming_calls",
        "AIRLINE",
        lapin::options::BasicConsumeOptions::default(),
        lapin::types::FieldTable::default(),
    ).await?;

    let mut consumer_looper = consumer.into_iter();

    while let Some(message) = consumer_looper.next() {
        let (_, delivery) = message.expect("Could not consume");

        println!("delivery - {:?}", delivery);
        delivery.ack(lapin::options::BasicAckOptions::default()).await.expect("ack");

        let message = match std::str::from_utf8(&delivery.data) {
            Ok(message) => message,
            Err(_) => {
                println!("Could not decode message from utf87 charset.");
                continue
            } // should send to sentry at this point
        };

        let rabbit_message: RabbitIncomingMessage =
            match serde_json::from_str(message) {
                Ok(message) => message,
                Err(_) => {
                    println!("Error parsing message.");
                    continue
                } // should send to sentry at this point
            };

        println!("{:?}", rabbit_message);

        let response = RabbitResponseMessage {
            content: "This was sent asynchronously through Rabbit MQ".into()
        };

        match send_to_rabbit(response, rabbit_message.call_sid.clone()).await {
            Ok(confirmation) => println!("{} for {}", confirmation, rabbit_message.call_sid.clone()),
            Err(_) => {
                println!("Could not send to Rabbit Queue for {}", rabbit_message.call_sid);
                continue
            } // should send to sentry here
        }
    }

    Ok(())
}

async fn send_to_rabbit(message: RabbitResponseMessage, queue_name: String) -> Result<String, lapin::Error> {
    let connection = lapin::Connection::connect(
        RABBITMQ_ADDR,
        lapin::ConnectionProperties::default().with_default_executor(8)
    ).await?;

    let channel = connection.create_channel().await?;

    channel.queue_declare(
        &queue_name,
        lapin::options::QueueDeclareOptions::default(),
        lapin::types::FieldTable::default()
    ).await?;

    let payload = match serde_json::to_string(&message) {
        Ok(payload) => payload,
        Err(_) => String::new()
    }.as_bytes().to_vec();

    let confirmation = channel.basic_publish(
        "",
        &queue_name,
        lapin::options::BasicPublishOptions::default(),
        payload,
        lapin::BasicProperties::default(), // setup confirmation correctly
    ).await?.await?;

    let message = match confirmation {
        lapin::publisher_confirm::Confirmation::NotRequested => "Message in queue".into(),
        _ => format!("Received confirmation - {:?}", confirmation)
    };

    connection.close(0, "complete").await?;

    Ok(message.into())
}
