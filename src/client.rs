use crate::db::DB;
use std::io::{self, Write};

pub fn start() {
    let mut db = DB::<String, String>::new("db.wal", 5);
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
        let tokens: Vec<&str> = line.trim().split_whitespace().collect();

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
                    eprintln!("Usage: get <key>");
                    continue;
                }
                let key = tokens[1];
                match db.get(&key.to_string()) {
                    Ok(value) => println!("Value: {}", value),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }

            "set" => {
                if tokens.len() < 3 {
                    eprintln!("Usage: set <key> <value>");
                    continue;
                }
                let key = tokens[1].to_string();
                // If there's more than one token after the key, join them with spaces.
                // e.g. "set mykey hello world" => value = "hello world"
                let value = tokens[2..].join(" ");

                match db.put(key, value) {
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
