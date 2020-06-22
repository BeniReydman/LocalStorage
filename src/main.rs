extern crate rmp_serde as rmps;
mod database;

use database::{Database, Cursor};

use rumqtt::{MqttClient, MqttOptions, QoS};
use rumqtt::client::Notification;

use std::str;
use std::sync::Arc;
use std::io::{Error, ErrorKind};

use serde::{Serialize, Deserialize};
use rmps::{Serializer, Deserializer};

const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: u16 = 1883;

enum CurrentState {
    Available,
    Iterating,
    Busy,
}

struct State <R> {
    pub cursor:     Option<Cursor<R>>,
    pub topic:      Option<String>,
    pub curr_state: CurrentState
}

impl<T> State<T> {
    fn new(state: CurrentState) -> State<T> {
        State {
            cursor:     None,
            topic:      None,
            curr_state: state
        }
    }
}

/*** MQTT Structs ***/

#[derive(Serialize, Deserialize, Debug)]
struct GetData {
    table:      &'static str,
    start_ts:    i32,
    end_ts:      i32
}

/********************/

fn main() {
    handler();
}

fn handler() {
    // Initialize Variables
    let mqtt_options = MqttOptions::new("LocalDB", SERVER_IP, SERVER_PORT);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options).unwrap();
    let database = Database::new("data");
    let state = State::<()>::new(CurrentState::Available);
    //let state = State{cursor: None, topic: None, curr_state: CurrentState::Available};

    //Topics (Step 1 in adding command)
    let topic_test = String::from("Test");
    let topic_test1 = String::from("Test1");
    let topic_test2 = String::from("Test2");
    let topic_getdata = String::from("GetData");
    let topic_getdata = String::from("GetNext"); // Can only be done if GetData was called and in iterating state

    // Subscribe to servers to receive publishes (Step 2 in adding command)
    mqtt_client.subscribe(&topic_test, QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe(&topic_test1, QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe(&topic_test2, QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe(&topic_getdata, QoS::AtLeastOnce).unwrap();

    // Parse notifications
    for notification in notifications {
        match notification {
            Notification::Publish(publish) =>  {
                    // Get payloads
                    let payload = Arc::try_unwrap(publish.payload).unwrap();
                    // Match topics of notification (Step 3 in adding command)
                    let topic = publish.topic_name;
                    match topic {
                        topic if topic == topic_test => println!("{:?}", topic),
                        topic if topic == topic_test1 => println!("{:?}", topic),
                        topic if topic == topic_test2 => println!("{:?}", topic),
                        topic if topic == topic_getdata => getData(payload, &database).unwrap(),
                        _ => println!("Invalid Topic!") // Throw an error
                    }
                },
            _ => println!("Received something that's not a publish! {:?}. Ignoring...", notification)
        }
    }
}

fn getData(payload: Vec<u8>, database: &Database) -> Result<(), Error> {
    // Deserialize payload
    let mut de = Deserializer::new(&payload[..]);
    let data: GetData = Deserialize::deserialize(&mut de).unwrap();

    Ok(())
}

fn publish(mqtt_client: &mut MqttClient, topic: &str, data: &str) -> Result<(), Error> {
    // Publish request
    mqtt_client.publish(topic, QoS::AtLeastOnce, false, data).unwrap();

    Ok(())
}

// fn print_notification(payload: Vec<u8>) {
//     let mut de = Deserializer::new(&payload[..]);
//     let res: RawData = Deserialize::deserialize(&mut de).unwrap();
//     println!("Deserialized to: \n{:?}", res);
// }