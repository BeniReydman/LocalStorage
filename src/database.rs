extern crate chrono;

use std::io;
use std::io::prelude::*;
use std::io::Cursor;
use std::fs;
use std::fs::File;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::path::Path;
use std::io::{Error, ErrorKind};
use chrono::prelude::*;
use chrono::Duration;
use serde::{Serialize, Deserialize};
use crc::crc32;
use rmps::{Serializer, Deserializer};
use rmps::decode::ReadReader;
use log::{error, info};

static DATE_FORMAT: &str = "%Y%m%d";
static TIME_FORMAT: &str = "%H";

#[derive(Debug)]
pub struct Database {
    pub source:         &'static str,
}

#[derive(Debug)]
pub struct Entry {
    pub table: &'static str,
    pub data:       Vec<u8>,
}

#[derive(Debug)]
pub struct MyCursor {
    pub database:       Database,
    pub table:          &'static str,
    pub de:             Deserializer<ReadReader<Cursor<Vec<u8>>>>,
    pub curr_ts:        DateTime<Utc>,
    pub start_ts:       u32,
    pub end_ts:         u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MpdRecordType {
    pub id:         u32,        // Record identifier
    pub datalog:    Vec<u8>,    // Byte array of length 'size'
    pub checksum:   u32,        // CRC-32 checksum of 'datalog'
}

pub trait DB {
    // Set a new source for the database
    fn set_source(&self, source: &str) -> Result<(), io::Error>;
    
    // Lists all the databases within the current data source
    fn list_db(&self);

    // Insert into database
    fn insert(&self, entry: Entry) -> Result<(), io::Error>;

    // Find a particular file
    fn find_file(&self, source: &str) -> Result<Vec<u8>, io::Error>;

    // Find a partical Entry
    fn find_data(&self, date: &str);
}

impl MyCursor {
    // Constructor
    pub fn new(db: Database, tb: &'static str, deserializer: Deserializer<ReadReader<Cursor<Vec<u8>>>>, dt: DateTime<Utc>, st: u32, et: u32) -> MyCursor {
        MyCursor {
            database:   db,
            table:      tb,
            de:         deserializer,
            curr_ts:    dt,
            start_ts:   st,
            end_ts:     et
        }
    }

    pub fn next(&mut self, record: &mut Option<MpdRecordType>) {
        loop {
            // Check if the end was reached
            if cursor_is_end(self) {
                *record = None;
            }

            // Attempt to deserialize
            let entry: MpdRecordType = match Deserialize::deserialize(&mut self.de) {
                Ok(entry) => entry,
                Err(error) => {
                    match error {
                        // End of file error, continue to next file *Note: other causes may trigger this*
                        rmps::decode::Error::InvalidMarkerRead(_) => {},
                        // Every other error, raise error and continue
                        _ => error!("Unexpected Error! {:?}\nIgnoring...", error),
                    }
                    // Add an hour of time and continue
                    self.curr_ts = self.curr_ts + Duration::hours(1); 
                    // Check if there exists another file
                    match get_next_file(self) {
                        Ok(buf) => {
                            self.de = Deserializer::new(Cursor::new(buf));
                        },
                        Err(_) => {
                            info!("Couldn't get another file, exiting loop.");
                            *record = None; 
                            return;
                        }
                    }
                    continue;
                }
            };
            info!("Checking entry at time: {:?} with entry id: {:?}", self.curr_ts, entry.id);

            // Check if entry ID is smaller than start_timestamp
            if entry.id < self.start_ts {
                info!("not what was being looked for, continuing");
                continue;
            }

            // Check if entry ID is biiger than end_timestamp
            if entry.id > self.end_ts {
                info!("Reached end time");
                *record = None;
                break;
            }

            // Set record and return
            *record = Some(entry);
            break;
    
        }
    }
}
 
/// get_next_file()
///
/// gets the next file in the database and returns error
/// if there is nothing to read
#[allow(unused_assignments)] // for curr_directory and curr_file 
fn get_next_file(cursor: &mut MyCursor) -> Result<Vec<u8>, Error> {
    // Setup variables
    let mut curr_directory = String::new();
    let mut curr_file = String::new();
    let mut buf = Vec::new();

    loop {
        buf.clear();
        curr_directory = format!("{}/{}/{}", cursor.database.source, cursor.table, cursor.curr_ts.format(DATE_FORMAT));
        curr_file = format!("{}/{}", curr_directory, cursor.curr_ts.format(TIME_FORMAT));

        // Check if Directory doesn't exist
        if !Path::new(&curr_directory).exists() {
            // Add a day of time, set hours, minutes and seconds to 0 and continue
            cursor.curr_ts = (cursor.curr_ts + Duration::days(1)).date().and_hms(0, 0, 0);  // += gives error 
            if cursor_is_end(cursor) {
                return Err(Error::new(ErrorKind::Other, "Nothing more to read."));
            }
            continue;
        }

        // Check if File doesn't exist
        if !Path::new(&curr_file).exists() {
            // Add an hour of time and continue
            cursor.curr_ts = cursor.curr_ts + Duration::hours(1);  // += gives error
            if cursor_is_end(cursor) {
                return Err(Error::new(ErrorKind::Other, "Nothing more to read."));
            }
            continue;
        }
        
        // Read File
        let mut file = File::open(curr_file).unwrap();
        file.read_to_end(&mut buf).unwrap();
        break;
    }

    return Ok(buf);
}

/// cursor_is_end()
///
/// Checks to see if the cursor is done reading files
fn cursor_is_end(cursor: &mut MyCursor) -> bool {
    if cursor.curr_ts.timestamp() > (cursor.end_ts as i64) {
        return true;
    }
    return false;
}

/// Implementation of Database
impl Database {
    /// Constructor
    pub fn new(source: &'static str) -> Database {
        Database {
            source: source
        }
    }

    /// set_source()
    ///
    /// Set a new source for the database
    #[allow(dead_code)]
    pub fn set_source(&mut self, source: &'static str) -> Result<(), io::Error> {
        self.source = source;
        Ok(())
    }
    
    /// list_db()
    ///
    /// Lists all the databases within the current data source
    #[allow(dead_code)]
    pub fn list_db(&self) {
        print_directories(self.source, 0);
    }

    /// insert_at()
    ///
    /// Insert into database
    #[allow(dead_code)]
    pub fn insert_at(&self, path: &str, file: &str, entry: Entry) -> Result<(), io::Error> {
        // Variables
        let ymd = String::from(path);
        let h = String::from(file);

        // Set the directory
        let mut directory = format!("{}/{}/{}", 
                    self.source,    // Database Directory
                    entry.table,    // Sub directory
                    path            // Current format of time
                );
        info!("Directory is: {:?}", directory);

        // Ensure directory/file exists
        create_dir_all(&directory).unwrap();
        directory.push_str(&format!("/{}", file));
        let path = Path::new(&mut directory);
        if !path.exists() {
            File::create(&directory)?;
            info!("File created!\n");
        }

        let dt = Utc.ymd(ymd[0..4].parse::<i32>().unwrap(), ymd[4..6].parse::<u32>().unwrap(), ymd[6..8].parse::<u32>().unwrap()).and_hms(h.parse::<u32>().unwrap(), 0, 0);
        // Set up data
        let new_data = MpdRecordType{
            id:         dt.timestamp() as u32,
            datalog:    entry.data.clone(),
            checksum:   crc32::checksum_ieee(&entry.data)
        };
        let serialized_data = serialize_struct(new_data).unwrap();

        // Write to database
        let mut file = OpenOptions::new().append(true).open(&directory)?;   // Write at end of file
        file.write_all(&serialized_data)?;
        info!("Wrote: {:?}\n", serialized_data);
        Ok(())
    }

    /// insert()
    ///
    /// Insert into database
    #[allow(dead_code)]
    pub fn insert(&self, entry: Entry) -> Result<(), io::Error> {
        // Set the directory
        let directory = format!("{}/{}/{}/{}", 
                    self.source,                     // Database Directory
                    entry.table,                     // Sub directory
                    get_local_datetime(DATE_FORMAT), // Current format of data Ex: &Y&m&d -> 19700101
                    get_local_datetime(TIME_FORMAT)  // Current format of time
                );
        println!("{:?}", directory);
        // Check if exists
        if !Path::new(&directory).exists() {
            File::create(&directory)?;
            info!("File created!\n")
        }
        

        // Write to database
        let mut file = OpenOptions::new().append(true).open(&directory)?;   // Write at end of file
        file.write_all(&entry.data)?;
        info!("Wrote: {:?}\n", entry.data);
        Ok(())
    }

    /// find_file()
    ///
    /// Find a particular file/folder
    #[allow(dead_code)]
    pub fn find_file(&self, source: &str) -> Result<Vec<u8>, io::Error> {
        // Set the directory
        let mut directory = String::new();
        directory.push_str(self.source);    // Database Directory
        directory.push_str(source);         // Sub directory

        // Read from file
        let mut buf: Vec<u8> = Vec::new();
        let mut file = File::open(&mut directory)?;
        file.read_to_end(&mut buf)?;
        info!("Read: {:?}\n", buf);
        return Ok(buf);
    }

    /// delete_file()
    ///
    /// Remove a particular file
    #[allow(dead_code)]
    pub fn delete_file(&self, table: &'static str, source: &'static str) -> io::Result<()> {
        let file = format!("{}/{}/{}", self.source, table, source);
        remove_file(file)?;
        Ok(())
    }

    /// find_data()
    ///
    /// Find a particular Entry
    #[allow(dead_code, unused_variables)]
    pub fn find_data(&self, date: &str) {
        
    }

    /// Different implementation of get_data can be found here: https://pastebin.com/z2pbbQxy
    /// 
    /// get_data()
    ///
    /// Grabs data from the database 
    pub fn get_data(&self, table: &'static str, start_time: u32, end_time: u32) -> MyCursor {
        let start_time = start_time - 3600; // an hour of time is taken off to account for initial failure adding an hour of time
        let cursor = MyCursor::new(Database::new(self.source), table, Deserializer::new(Cursor::new(Vec::new())), get_datetime(start_time), start_time, end_time);
        return cursor;
    }
}

/// read()
///
/// reads directory
#[allow(dead_code)]
fn read(mut buf: &mut Vec<u8>, mut directory: &mut String) -> std::io::Result<()> {
    let mut file = File::open(&mut directory)?;
    file.read_to_end(&mut buf)?;
    info!("Read: {:?}\n", buf);
    Ok(())
}

/// print_directories()
///
/// prints all the directories not including files
fn print_directories(path: &str, count: usize) {
    let paths = fs::read_dir(path).unwrap();

    for entry in paths {
        if let Ok(entry) = entry {
            if entry.path().is_dir() {
                // Print Directory
                print!("{:-<1$}", "", count);
                println!("{}", entry.file_name().into_string().unwrap());
                print_directories(entry.path().to_str().unwrap(), count + 1);
            }
        }
    }
}

/// print_db()
///
/// prints all the directories and files
#[allow(dead_code)]
fn print_db(path: &str, count: usize) {
    let paths = fs::read_dir(path).unwrap();

    for entry in paths {
        if let Ok(entry) = entry {
            // Print Directory
            print!("{:-<1$}", "", count);
            println!("{}", entry.file_name().into_string().unwrap());
            if entry.path().is_dir() {
                print_directories(entry.path().to_str().unwrap(), count + 1);
            }
        }
    }
}

/// serialize_struct()
///
/// Serializes structs
fn serialize_struct<T>(data: T) -> Result<Vec<u8>, ()> where T: Serialize, {
    let mut buf = Vec::new();
    let mut msg_pack = Serializer::new(&mut buf);
    match data.serialize(&mut msg_pack) {
        Ok(_) => return Ok(buf),
        Err(e) => {
            error!("Error serializing: {:?}", e);
            return Err(())
        }
    }

    
}

/// get_local_datetime()
///
/// Returns the current time UTC
fn get_local_datetime(format: &str) -> String {
    let local: DateTime<Utc> = Utc::now();
    return local.format(format).to_string();
}

/// get_datetime()
///
/// Converts timestamp to datetime
fn get_datetime(timestamp: u32) -> DateTime<Utc> {
    let naive_datetime = NaiveDateTime::from_timestamp(i64::from(timestamp), 0);  // the 0 represents nanoseconds for leap seconds
    let utc_datetime = DateTime::<Utc>::from_utc(naive_datetime, Utc);
    return utc_datetime;
}

/// print_error()
///
/// Prints custom errors
#[allow(dead_code)]
fn print_error(err: Error) {
    if let Some(inner_err) = err.into_inner() {
        error!("Inner error: {}", inner_err);
    } else {
        error!("No inner error");
    }
}

/** 
/*************************************************** TESTS **************************************************/
*/
use rand::Rng;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[allow(non_snake_case)]
pub struct RawData { // change all names
    pub AQHI:		Option<i32>,
	pub AQI:		Option<i32>,
	pub CO:			Option<f32>,
	pub CO2:		Option<f32>,
	pub NO:			Option<f32>,
	pub NO2:		Option<f32>,
	pub O3:			Option<f32>,
	pub PM1:		Option<f32>,
	pub PM2_5:		Option<f32>,
	pub PM10:		Option<f32>,
	pub SO2:		Option<f32>,
	pub T:			Option<f32>,
	pub RH:			Option<f32>,
	pub NOISE:		Option<f32>, 
	pub TimeStamp:	Option<String> // change ~ ticks
}

/// new_buf()
///
/// Serialize a randomly generated struct
#[allow(dead_code)]
pub fn new_buf() -> Result<Vec<u8>, Error> {
    match serialize_struct(generate_raw_data()) {
        Ok(buf) => return Ok(buf),
        Err(_) => return Err(Error::last_os_error())
    };
}

/// generate_raw_data()
///
/// generates random data for struct
fn generate_raw_data() -> RawData {
    let raw_data = RawData {
        AQHI:		generate_i32(),
        AQI:		generate_i32(),
        CO:			generate_f32(),
        CO2:		generate_f32(),
        NO:			generate_f32(),
        NO2:		generate_f32(),
        O3:			generate_f32(),
        PM1:		generate_f32(),
        PM2_5:		generate_f32(),
        PM10:		generate_f32(),
        SO2:		generate_f32(),
        T:			generate_f32(),
        RH:			generate_f32(),
        NOISE:		generate_f32(),
        TimeStamp:	Some("".to_string())
    };

    return raw_data;
}

/// generate_i32()
///
/// Generates random i32 from 0-10, if it's greater than 8, return null
#[allow(dead_code)]
fn generate_i32() -> Option<i32> {
    let mut rng = rand::thread_rng();
    let num: i32 = rng.gen_range(0,10);
    if num >= 8 {
        return None;
    }
    return Some(num);
}

/// generate_f32()
///
/// Generates random f32 from 0-10, if it's greater than 8, return null
#[allow(dead_code)]
fn generate_f32() -> Option<f32> {
    let mut rng = rand::thread_rng();
    let num: f32 = rng.gen_range(0.0,10.0);
    if num >= 8.0 {
        return None;
    }
    return Some(num);
}

#[cfg(test)]
mod file_sys_tests {
    #[allow(unused_imports)]
    use super::*;

    // #[test]
    // fn test1_get_data() {
    //     println!("Starting get_data test1!");

    //     let database = Database::new("data");

    //     let mut buf: Vec<u8> = new_buf().unwrap();
    //     database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200101", "23", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

    //     database.get_data("levels", 1577916000, 1577926800);

    //     database.delete_file("levels", "20200101/22").unwrap();
    //     database.delete_file("levels", "20200101/23").unwrap();
    //     database.delete_file("levels", "20200102/00").unwrap();
    //     database.delete_file("levels", "20200102/01").unwrap();

    //     println!("Finished get_data test1!");
    // }

    // #[test]
    // fn test2_get_data() {
    //     println!("Starting get_data test2!");

    //     let database = Database::new("data");

    //     let mut buf: Vec<u8> = new_buf().unwrap();
    //     database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200101", "23", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200101", "23", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

    //     database.get_data("levels", 1577916000, 1577926800);

    //     database.delete_file("levels", "20200101/22").unwrap();
    //     database.delete_file("levels", "20200101/23").unwrap();
    //     database.delete_file("levels", "20200102/00").unwrap();
    //     database.delete_file("levels", "20200102/01").unwrap();

    //     println!("Finished get_data test2!");
    // }

    // #[test]
    // fn test4_get_data() {
    //     println!("Starting get_data test4!");

    //     let database = Database::new("data");

    //     File::create(format!("{}/{}", database.source, "levels/20200101/22")).unwrap();
    //     File::create(format!("{}/{}", database.source, "levels/20200101/23")).unwrap();
    //     File::create(format!("{}/{}", database.source, "levels/20200102/00")).unwrap();
    //     let buf: Vec<u8> = new_buf().unwrap();
    //     database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

    //     database.get_data("levels", 1577916000, 1577926800);

    //     database.delete_file("levels", "20200101/22").unwrap();
    //     database.delete_file("levels", "20200101/23").unwrap();
    //     database.delete_file("levels", "20200102/00").unwrap();
    //     database.delete_file("levels", "20200102/01").unwrap();

    //     println!("Finished get_data test4!");
    // }

    // #[test]
    // fn test6_get_data() {
    //     println!("Starting get_data test6!");

    //     let database = Database::new("data");

    //     let mut buf: Vec<u8> = new_buf().unwrap();
    //     database.insert_at("20200101", "22", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();

    //     database.get_data("levels", 1577916000, 1577926800);

    //     database.delete_file("levels", "20200101/22").unwrap();
    //     database.delete_file("levels", "20200102/00").unwrap();

    //     println!("Finished get_data test6!");
    // }

    // #[test]
    // fn test7_get_data() {
    //     println!("Starting get_data test7!");

    //     let database = Database::new("data");

    //     let mut buf: Vec<u8> = new_buf().unwrap();
    //     database.insert_at("20200102", "00", Entry{table: "levels", data: buf}).unwrap();
    //     buf = new_buf().unwrap();
    //     database.insert_at("20200102", "01", Entry{table: "levels", data: buf}).unwrap();

    //     database.get_data("levels", 1577916000, 1577926800);

    //     database.delete_file("levels", "20200102/00").unwrap();
    //     database.delete_file("levels", "20200102/01").unwrap();

    //     println!("Finished get_data test7!");
    // }
}