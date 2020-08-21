# A Filesystem-based Embedded Time-Series Data Store

## Features:

1. Quick save (INSERT)
2. Rolling files
3. Search/Fetch (SELECT)
4. Create

## Lacks / TODO:

1. CRUD - No Update or Delete
2. Atomic Operations
    * Thread safety for simultaneous read & write


## Concept Description

|  The Concept of a  |      is Implemented as a     |
|:------------------:|:----------------------------:|
|      Database      |      Directory / Folder      |
|       Table        |    Sub- Directory / Folder   |
|       Record       |      Message Pack "blob"     |
|       Field        |            N/A               |
|     Primary Key    |         Timestamp            |
|      Sharding      |     Date Directory + File    |

### Example

Consider a data store for AirSENCE<sup>TM</sup> device 40045 that has tables for pollutant measurements (table name: `levels`) and raw voltages (table name: `raw`). Further, consider that the device was deployed on 20th April 2020 starting at 11:00 UTC. A snapshot of the directory tree taken on 22nd April just before 16:00 will be as illustrated below
```
+---40045
¦   +---levels
¦   ¦   +---20200420
¦   ¦   ¦   +---11.mpd
¦   ¦   ¦   +---12.mpd
¦   ¦   ¦   +---13.mpd
¦   ¦   ¦    ...
¦   ¦   ¦   +---23.mpd
¦   ¦   +---20200421
¦   ¦   ¦   +---00.mpd
¦   ¦   ¦   +---01.mpd
¦   ¦   ¦   +---02.mpd
¦   ¦   ¦    ...
¦   ¦   ¦   +---23.mpd
¦   ¦   +---20200422
¦   ¦       +---00.mpd
¦   ¦       +---01.mpd
¦   ¦       +---02.mpd
¦   ¦        ...
¦   ¦       +---15.mpd
¦   +---raw
¦       +---20200420
¦       ¦   +---11.mpd
¦       ¦   +---12.mpd
¦       ¦   +---13.mpd
¦       ¦    ...
¦       ¦   +---23.mpd
¦       +---20200421
¦       ¦   +---00.mpd
¦       ¦   +---01.mpd
¦       ¦   +---02.mpd
¦       ¦    ...
¦       ¦   +---23.mpd
¦       +---20200422
¦           +---00.mpd
¦           +---01.mpd
¦           +---02.mpd
¦            ...
¦           +---15.mpd
+--- ...
```

The files, `NN.mp` store hourly records of measurements serialized using [Message Pack (MsgPack)](https://msgpack.org/).


# Data Management

Each *.mpd file must have a header to help manage and read data using correct versions of software tools/applications.

## File Header

The header should contain the following:

* [100 bytes (characters)] Product name / Company name (or both)
* [1 byte] Product version
    * _| Major.Minor |_ <- 1 nibble each for major and minor, e.g. `4.2` => `0x42`
* [4 bytes] Software Class / Structure (Schema) version (Release # / Build #)
    * _| Major.Minor | Revision | Build | Build |_
* [TBC] Message Pack (API/Spec) version
    * TODO
* [2 bytes] Number of Records stored
    * Since minimum reporting interval = 15 seconds, max records per hour = 240
* [2 bytes] Record separator
    * Probably not feasible since MsgPack is a binary format and can't guarantee a unique unused byte sequence
    * Use record length instead: _`...| Length MSB | Length LSB | +++ Record Data Payload +++ | ...`_

## Data Integrity

Use a CRC-32 checksum [[1](1), [2](2), [3](3), [4](4)] to validate each encoded record. CRC-32 checksums can be stored in Message Pack as an [extension format](5). A suitable option is to use the extension type `fixext 4` (type code: `0xd6`).

[1]: https://create.stephan-brumme.com/crc32/#git1
[2]: https://www.libcrc.org/download/
[3]: https://github.com/bakercp/CRC32/blob/master/src/CRC32.cpp
[4]: https://docs.rs/crc/1.8.1/crc/index.html
[5]: https://github.com/msgpack/msgpack/blob/master/spec.md#ext-format-family 

## Data Model

For scalability, it will be useful to consider a _data model_ to store in this data store. We can consider a _meta model_ that defines the record type for the database, and encapsulates the original MsgPack-serialized data, enabling the data store to be used universally across different classes/structures. The _meta model_ can be serialized and saved in the database, enabling predictable access to the stored values.

### Meta Model for Data Store

A possible _meta model_ is as follows
```
mpd_record :=  |   id   |   size   |  datalog  |  checksum  |

struct mpd_record_type {
    id: uint32,            # record identifier
    size: uint16,          # length of `datalog`
    datalog: bytes         # byte array of length `size`
    checksum: uint32       # CRC-32 checksum of `datalog`
}
```

where `mdp_record` is saved as a "packed" data stream that is serialized using Message Pack.

The byte array, `datalog` is the original/actual **record** to be stored in the database, which has been serialized using Message Pack.

### Mapping to Message Pack

Consider `payload := size + datalog`, the _meta model_, `mdp_record_type` can be mapped to Message Pack types easily as follows

|    Name   |   Common Type   | MsgPack Type (Code) |
|:----------|:---------------:|:-------------------:|
|     id    |      uint32     |     uint32 (0xce)   |
|  payload  |      bytes      |      bin16 (0xc5)   |
|  checksum |      uint32     |    fixext4 (0xd6)   |


### Variants on Meta Model

The data store can save a timestamp in the `id` of the record, in which case the table becomes

|    Name   |   Common Type   | MsgPack Type (Code) |
|:----------|:---------------:|:-------------------:|
|     id    |      uint32     | timestamp32 (0xd6)  |
|  payload  |      bytes      |      bin16 (0xc5)   |
|  checksum |      uint32     |    fixext4 (0xd6)   |

This enhances the time-series store features of the data store.

An alternative would be to save a time offset (e.g. number of seconds elapsed in this hour), which would consume fewer bytes. 
If we consider `id` to be a `uint16` that stores data in two bytes, we can store a maximum of 2^16 = 65_536 records per file (i.e. in one hour), supporting a maximum data rate of 65_536 / 3_600 ≈ 18 Hz. In contrast, AirSENCE<sup>TM</sup>'s maximum data rate is 0.0667 Hz (<< 1 Hz).

|    Name   |   Common Type   | MsgPack Type (Code) |
|:----------|:---------------:|:-------------------:|
|     id    |      uint16     |     uint16 (0xcd)   |
|  payload  |      bytes      |      bin16 (0xc5)   |
|  checksum |      uint32     |    fixext4 (0xd6)   |

AirSENCE<sup>TM</sup> has a data rate of << 1 Hz, while typical ITS telemetry applications consider a data rate of 10 Hz, both of which are comfortably within the maximum data rate limit of 18 Hz.

**Suggestion:** calculate `id` as increments of 100 ms, corresponding to 10 Hz, giving us the following

|  Time Elapsed  |     `id`      |
|:--------------:|:-------------:|
|     100 ms     |    0x0001     |
|     200 ms     |    0x0002     |
|     300 ms     |    0x0003     |
|      1 s       |    0x000a     |
|     1.1 s      |    0x000b     |
|      1 min     |    0x0258     |
|      5 min     |    0x0888     |
|     15m 5s     |    0x235a     |
|     1 hour     |    0x8ca0     |


## File Management

### Rolling Log

### Archival

### Deletion
