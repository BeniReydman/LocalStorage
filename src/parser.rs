use std::fs::File;
use std::io::Read;
use serde::{Serialize, Deserialize};
use log::{error, warn};

/// Config is the config for initialize the server
/// Contain sensor initialize information
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
	pub ip:     String,
    pub port:   u32,
    pub topics: Vec<String>
}

/// Create a default empty struct
impl Default for Config {
    fn default () -> Config {
        Config {
            ip:     "127.0.0.1".to_string(),
            port:   1883,
            topics: vec!["topic1".to_string()]
        }
	}
}


/// parse()
///
/// Read toml files and return a struct consisting of
/// a map of sensors and modbus's
pub fn parse (path: &String) -> Result<Config, ()> {
    // Get toml file
    let toml_file = read_file(&path);

    // Attempt to Parse, if error return default empty Config
    let config: Config = match toml::from_str(&toml_file) {
        Ok(config) => config,
        Err(err) => {
            error!{"Error! Couldn't read file. \n{:?}", err};
            return Err(());
        }
    };

    return Ok(config);
}

/// read_file()
///
/// Read toml file and return read String 
pub fn read_file(path: &String) -> String {
    // Variables 
    let mut toml_file = String::new();

    // Attempting to open file
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(_)  => {
            error!("Could not find config file, returning empty string.");
            return String::new();
        }
    };

    // Attempt to read in the file
    file.read_to_string(&mut toml_file)
                .unwrap_or_else(|err| panic!("Error reading file: {0}", err));

    // Give a warning if file is empty (Not necessarily a bug, will return empty Config) 
    if toml_file.is_empty() {
        warn!("Empty file!");
    }

    return toml_file;
}