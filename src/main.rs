extern crate rmp_serde as rmps;
mod database;
mod parser;

use database::{Database, MpdRecordType};

use log::{error, info, warn, debug};
use log4rs;

use rumqtt::{MqttClient, MqttOptions, QoS};
use rumqtt::client::Notification;

use std::str;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{thread, time};
use std::io::Error;


use serde::{Serialize, Deserialize};
use rmps::{Serializer, Deserializer};

const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: u16 = 1883;

/// STATE is used by 'initialize_handler()' which set the interrupt handler
static mut STATE: CurrentState = CurrentState::Available;

/// CurrentState
/// 
/// Is used to determine the current state of
/// the program for interrupts like 'ctrl-c'
enum CurrentState {
    Available,
    Busy,
}

/// MQTT Structs.
///
/// These are structs associated with MQTT.
/// They will be used mostly for deserialization/serialization
/// before receiving/sending
#[derive(Serialize, Deserialize, Debug)]
struct GetData {
    table:      String,
    start_ts:   u32,
    end_ts:     u32
}

fn main() {
    // Initialize logger
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    info!("Starting the program.");

    // Collect command line arguments
    let args: Vec<String> = env::args().collect();

    // Error check arguments
    if args.len() > 2 {
        error!("Too many arguments!");
        info!("Ending the program.\n");
        return;
    } else if args.len() == 1 {
        error!("No path given!");
        info!("Ending the program.\n");
        return;
    }

    // Attempt to parse file at path
    let path = &args[1];
    let config = match parser::parse(path) {
        Ok(config) => config,
        Err(_) => {
            error!("File could not be read!");
            return
        }
    };
 
    debug!("{:?}", config);

    // Start MQTT handler
    handler(config);

    info!("Ending the program.\n");
}

fn handler(config: parser::Config) {
    // Initialize Variables
    let mqtt_options = MqttOptions::new("LocalDB", SERVER_IP, SERVER_PORT);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options).unwrap();
    let database = Database::new("data");

    // Set up ctrl-c handler
    let running = initialize_handler();

    /*** 3 STEPS TO GET NOTIFICATIONS FROM SUBSCRIBED TOPIC ***/
    // Topics (Step 1 in adding command)
    let topics = config.topics;  // config comes from parser.rs which gets topics from toml file

    // Subscribe to servers to receive publishes (Step 2 in adding command)
    subscribe(&mut mqtt_client, topics);

    // Parse notifications
    for notification in notifications {
        // Change to Busy
        change_state();

        match notification {
            Notification::Publish(publish) =>  {
                    // Get payloads
                    let payload = Arc::try_unwrap(publish.payload).unwrap();
                    // Match topics of notification (Step 3 in adding command)
                    // Note, this has to be manually inputted at the moment as results are different for topics
                    let topic = publish.topic_name;
                    match topic {
                        topic if &topic == "topic1" => debug!("{:?}", topic), // Random topic
                        topic if &topic == "topic2" => debug!("{:?}", topic), // Random topic
                        topic if &topic == "topic3" => debug!("{:?}", topic), // Random topic
                        topic if &topic == "topic_add" => add(payload, &database).unwrap(), // Add data to DB
                        topic if &topic == "topic_delete" => delete(&database).unwrap(), // Delete data from DB
                        topic if &topic == "topic_getdata" => {
                            let result = get_data(payload, &database, &mut mqtt_client, &topic);
                            match result {
                                Ok(_) => info!("Successfully sent data."),
                                Err(error) => error!("There was an Error! {:?}", error)
                            }
                        },
                        _ => error!("Invalid Topic!") // Throw an error
                    }
                },
            _ => warn!("Received something that's not a publish! {:?}. Ignoring...", notification)
        }

        // Check to see if ctrl-c was used
        if !running.load(Ordering::SeqCst) {
            info!("Shutting down.");
            std::process::exit(0);
        }

        // Change to Available
        change_state();
    }
}


/// add()
/// 
/// add data to DB (not to be used by clients, only for testing)
#[allow(unused_assignments)]
fn add(payload: Vec<u8>, database: &Database) -> Result<(), Error> {
    // Deserialize payload
    let mut de = Deserializer::new(&payload[..]);
    let result: String = Deserialize::deserialize(&mut de).unwrap();
    let amount = result.trim().parse::<u32>().unwrap();

    let mut buf: Vec<u8> = Vec::new();
    for _ in 0..amount {
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "00", database::Entry{table: "levels", data: buf}).unwrap();
    }

    Ok(())
}

/// delete()
/// 
/// Deletes data from DB (not to be used by clients, only for testing)
fn delete(database: &Database) -> Result<(), Error> {
database.delete_file("levels", "20200102/00").unwrap();
Ok(())
}

/// change_state()
/// 
/// Swaps the current state
fn change_state() {
    unsafe {
        match STATE {
            CurrentState::Available => STATE = CurrentState::Busy,
            _ => STATE = CurrentState::Available
        };
    }
}

/// initialize_handler()
/// 
/// Initializes the ctrl-c handler 
fn initialize_handler() -> std::sync::Arc<AtomicBool> {
    // Initialize signal handler for ctrl-c
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        unsafe {
            match STATE {
                CurrentState::Available => {
                    info!("Ending the program.\n");
                    std::process::exit(0);
                },
                _ => { 
                    info!("Getting ready to shut down");
                    r.store(false, Ordering::SeqCst);
                }
            }
        }
    }).expect("Error setting Ctrl-C handler");

    return running;
}

/// get_data()
/// 
/// Grabs data from the database given the payload from MQTT
fn get_data(payload: Vec<u8>, database: &Database, mut mqtt_client: &mut MqttClient, topic: &str) -> Result<(), Error> {
    info!("Starting get_data()");
    debug!("Payload: {:?}", payload);
    // Deserialize payload
    let mut de = Deserializer::new(&payload[..]);
    let data: GetData = Deserialize::deserialize(&mut de).unwrap();

    debug!("Getting Cursor!");
    let mut cursor = database.get_data(string_to_static_str(data.table), data.start_ts, data.end_ts);

    // Set Variables
    let mut buf: Vec<u8> = Vec::new();
    let mut msg_pack = Serializer::new(&mut buf);
    let mut record: Option<MpdRecordType> = None;
    debug!("Looping!");
    loop {
        // Send 50 entries per packet
        for _ in 0..50 {
            cursor.next(&mut record);
            // Check to see if there is anything left to read
            if record.is_none() { 
                // Check if there are entries in buf
                if buf.len() > 0 {
                    publish(&mut mqtt_client, &topic, buf.clone()).unwrap();
                    // Sleep to ensure message is received
                    let ten_millis = time::Duration::from_millis(10);
                    thread::sleep(ten_millis);
                    // Publish nothing to indicate there is no more data left 
                    publish(&mut mqtt_client, &topic, Vec::new()).unwrap();
                } else {
                    // Publish nothing to indicate there is no more data left 
                    publish(&mut mqtt_client, &topic, Vec::new()).unwrap();
                }
                return Ok(());
            }
            else {
                record.serialize(&mut msg_pack).unwrap();
            }
        }
        publish(&mut mqtt_client, &topic, buf).unwrap();

        buf = Vec::new();
        msg_pack = Serializer::new(&mut buf);
        record = None;
    }
}

/// publish()
/// 
/// Publishes to MQTT given client, topic, and data
#[allow(unused_variables)]
fn publish(mqtt_client: &mut MqttClient, topic: &str, data: Vec<u8>) -> Result<(), Error> {
    // Publish request
    // Note, the same topic cannot be used as a reply as it gets caught by this subscriber as well
    // In the future, a topic reply might need to be given as well
    mqtt_client.publish("Client", QoS::AtLeastOnce, false, data).unwrap();
    info!("published");

    Ok(())
}

/// subscribe()
/// 
/// Subscribes to a list(Vec) of topics
fn subscribe(mqtt_client: &mut MqttClient, topics: Vec<String>) {
    // Subscribe to topics
    for topic in &topics {
        mqtt_client.subscribe(topic, QoS::AtLeastOnce).unwrap();
    }
}


/// string_to_static_str()
///
/// Convert String to &'static str
fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// Tests
#[cfg(test)]
mod file_sys_tests {
    use super::*;
    use std::fs::File;
    use database::Entry;

    #[test]
    fn test_cursor() {
        println!("Starting test_cursor test!");

        let database = Database::new("data");

        // Create fake data
        let mut buf: Vec<u8> = database::new_buf().unwrap();
        database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200101", "23", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

        // Test Cursor
        let mut cursor = database.get_data("levels", 1577916000, 1577926800);
        let mut record: Option<MpdRecordType> = None;
        println!("Looping.");
        let mut count = 0;
        loop {
            cursor.next(&mut record);
            if record.is_none() { break; }
            else {
                println!("{:?}\n", record);
                count += 1;
            }
        }

        // Delete all files made
        database.delete_file("levels", "20200101/22").unwrap();
        database.delete_file("levels", "20200101/23").unwrap();
        database.delete_file("levels", "20200102/00").unwrap();
        database.delete_file("levels", "20200102/01").unwrap();

        assert_eq!(count, 6);  // Was able to read all 6 entries

        println!("Finished test_cursor test!");
    }

    #[test]
    fn test2_cursor() {
        println!("Starting test2_cursor test!");

        let database = Database::new("data");

        // Create fake data
        let mut buf: Vec<u8> = database::new_buf().unwrap();
        database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200101", "23", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200101", "23", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

        // Test Cursor
        let mut cursor = database.get_data("levels", 1577916000, 1577926800);
        let mut record: Option<MpdRecordType> = None;
        println!("Looping.");
        let mut count = 0;
        loop {
            cursor.next(&mut record);
            if record.is_none() { break; }
            else {
                println!("{:?}\n", record);
                count += 1;
            }
        }

        // Delete all files made
        database.delete_file("levels", "20200101/22").unwrap();
        database.delete_file("levels", "20200101/23").unwrap();
        database.delete_file("levels", "20200102/00").unwrap();
        database.delete_file("levels", "20200102/01").unwrap();

        assert_eq!(count, 7);  // Was able to read all 7 entries

        println!("Finished test2_cursor test!");
    }

    #[test]
    fn test3_cursor() {
        println!("Starting test3_cursor test!");

        let database = Database::new("data");

        // Create fake data
        File::create(format!("{}/{}", database.source, "levels/20200101/22")).unwrap();
        File::create(format!("{}/{}", database.source, "levels/20200101/23")).unwrap();
        File::create(format!("{}/{}", database.source, "levels/20200102/00")).unwrap();
        let buf: Vec<u8> = database::new_buf().unwrap();
        database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

        // Test Cursor
        let mut cursor = database.get_data("levels", 1577916000, 1577926800);
        let mut record: Option<MpdRecordType> = None;
        println!("Looping.");
        let mut count = 0;
        loop {
            cursor.next(&mut record);
            if record.is_none() { break; }
            else {
                println!("{:?}\n", record);
                count += 1;
            }
        }

        // Delete all files made
        database.delete_file("levels", "20200101/22").unwrap();
        database.delete_file("levels", "20200101/23").unwrap();
        database.delete_file("levels", "20200102/00").unwrap();
        database.delete_file("levels", "20200102/01").unwrap();

        assert_eq!(count, 1);  // Was able to read all 1 entries

        println!("Finished test3_cursor test!");
    }

    #[test]
    fn test4_cursor() {
        println!("Starting test4_cursor test!");

        let database = Database::new("data");

        // Create fake data
        let mut buf: Vec<u8> = database::new_buf().unwrap();
        database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();

        // Test Cursor
        let mut cursor = database.get_data("levels", 1577916000, 1577926800);
        let mut record: Option<MpdRecordType> = None;
        println!("Looping.");
        let mut count = 0;
        loop {
            cursor.next(&mut record);
            if record.is_none() { break; }
            else {
                println!("{:?}\n", record);
                count += 1;
            }
        }

        // Delete all files made
        database.delete_file("levels", "20200101/22").unwrap();
        database.delete_file("levels", "20200102/00").unwrap();

        assert_eq!(count, 2);  // Was able to read all 2 entries

        println!("Finished test4_cursor test!");
    }

    #[test]
    fn test5_cursor() {
        println!("Starting test5_cursor test!");

        let database = Database::new("data");

        // Create fake data
        let mut buf: Vec<u8> = database::new_buf().unwrap();
        database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
        buf = database::new_buf().unwrap();
        database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

        // Test Cursor
        let mut cursor = database.get_data("levels", 1577916000, 1577926800);
        let mut record: Option<MpdRecordType> = None;
        println!("Looping.");
        let mut count = 0;
        loop {
            cursor.next(&mut record);
            if record.is_none() { break; }
            else {
                println!("{:?}\n", record);
                count += 1;
            }
        }

        // Delete all files made
        database.delete_file("levels", "20200102/00").unwrap();
        database.delete_file("levels", "20200102/01").unwrap();

        assert_eq!(count, 2);  // Was able to read all 2 entries

        println!("Finished test5_cursor test!");
    }
}