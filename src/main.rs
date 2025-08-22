use std::io;

struct InputBuffer {
    buffer: String,
}

impl InputBuffer {
    fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    fn read_input(&mut self) {
        self.buffer.clear();
        io::stdin()
            .read_line(&mut self.buffer)
            .expect("Failed to read line");
        self.buffer = self.buffer.trim().to_string();
    }
}

enum MetaCommands {
    Exit,
}

fn parse_command(input: &str) -> Option<MetaCommands> {
    match input {
        ".exit" => Some(MetaCommands::Exit),
        _ => None,
    }
}

fn print_prompt() {
    print!("db > ");
    use std::io::Write;
    io::stdout().flush().unwrap();
}

fn main() {
    let mut input_buffer = InputBuffer::new();

    loop {
        print_prompt();
        input_buffer.read_input();

        match parse_command(&input_buffer.buffer) {
            Some(MetaCommands::Exit) => break,
            None => println!("Unrecognized command: {}", input_buffer.buffer),
        }
    }
}
