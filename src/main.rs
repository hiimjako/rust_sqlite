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

// Non-SQL statements like .exit are called “meta-commands”.
enum MetaCommands {
    Exit,
    Unrecognized,
}

impl MetaCommands {
    fn parse(input: &str) -> Option<MetaCommands> {
        match input {
            ".exit" => Some(MetaCommands::Exit),
            _ => {
                if input.starts_with(".") {
                    Some(MetaCommands::Unrecognized)
                } else {
                    None
                }
            }
        }
    }
}

enum StatementType {
    Select,
    Insert,
    Unrecognized,
}

impl StatementType {
    fn parse(input: &str) -> StatementType {
        match input {
            "select" => StatementType::Select,
            "insert" => StatementType::Insert,
            _ => StatementType::Unrecognized,
        }
    }
}

enum InputType {
    Meta(MetaCommands),
    Statement(StatementType),
}

impl InputType {
    fn parse(input: &str) -> InputType {
        if let Some(meta) = MetaCommands::parse(input) {
            InputType::Meta(meta)
        } else {
            InputType::Statement(StatementType::parse(input))
        }
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

        match InputType::parse(&input_buffer.buffer) {
            InputType::Meta(MetaCommands::Exit) => break,
            InputType::Meta(MetaCommands::Unrecognized) => {
                println!("Unrecognized meta-command: {}", input_buffer.buffer);
            }
            InputType::Statement(StatementType::Select) => println!("select call"),
            InputType::Statement(StatementType::Insert) => println!("insert call"),
            InputType::Statement(StatementType::Unrecognized) => {
                println!("Unrecognized command: {}", input_buffer.buffer)
            }
        }
    }
}
