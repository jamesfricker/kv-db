use crate::db::DB;
use std::io::{self, Write};

pub fn start() {
    // Adjust as needed: DB::new likely takes (filename, max_level) or similar
    let mut db = DB::new("db.wal", 5);
    let stdin = io::stdin();

    loop {
        // Prompt
        print!("> ");
        // Make sure the prompt is actually printed before reading input
        io::stdout().flush().unwrap();

        // Read a line of input
        let mut line = String::new();
        let bytes_read = stdin
            .read_line(&mut line)
            .expect("Failed to read from stdin");

        // If we hit EOF (Ctrl+D) or zero bytes, just exit
        if bytes_read == 0 {
            println!("Exiting...");
            break;
        }

        // Split into tokens (by whitespace)
        let tokens: Vec<&str> = line.split_whitespace().collect();

        // If user just pressed Enter without typing anything, ignore
        if tokens.is_empty() {
            continue;
        }

        let command = tokens[0].to_lowercase();
        match command.as_str() {
            "quit" | "exit" => {
                println!("Goodbye!");
                break;
            }

            "get" => {
                if tokens.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                // Convert the typed key to raw bytes
                let key_bytes = tokens[1].as_bytes().to_vec();

                match db.get(key_bytes) {
                    Ok(value_bytes) => {
                        // If you want to interpret them as UTF-8, do so:
                        match String::from_utf8(value_bytes) {
                            Ok(s) => println!("Value: {}", s),
                            Err(e) => {
                                // Error type has the original bytes
                                let raw_bytes = e.into_bytes();
                                println!("(binary data) {:?}", raw_bytes);
                            }
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                }
            }

            "set" => {
                if tokens.len() < 3 {
                    eprintln!("Usage: set <key> <value>");
                    continue;
                }
                // Convert key to raw bytes
                let key_bytes = tokens[1].as_bytes().to_vec();
                // Join all subsequent tokens as the value, then convert to raw bytes
                let value_string = tokens[2..].join(" ");
                let value_bytes = value_string.into_bytes();

                match db.put(key_bytes, value_bytes) {
                    Ok(_) => println!("OK"),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }

            // Unknown command
            _ => {
                eprintln!("Unknown command: {}", command);
                eprintln!("Commands: get <key>, set <key> <value>, quit, exit");
            }
        }
    }
}
