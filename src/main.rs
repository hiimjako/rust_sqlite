use rust_sqlite::*;
use std::{fmt, io};

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

enum PrepareStatement {
    Success(Statement),
    SyntaxError,
    NegativeID,
    StringTooLong,
    Unrecognized,
}

impl PrepareStatement {
    fn parse(input: &str) -> PrepareStatement {
        if input.starts_with("select") {
            PrepareStatement::Success(Statement::Select)
        } else if input.starts_with("insert") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() != 4 {
                return PrepareStatement::SyntaxError;
            }

            let id = match parts[1].parse::<u32>() {
                Ok(val) => val,
                Err(_) => return PrepareStatement::NegativeID,
            };

            let mut username = [0u8; COLUMN_USERNAME_SIZE];
            let username_bytes = parts[2].as_bytes();
            if username_bytes.len() > COLUMN_USERNAME_SIZE {
                return PrepareStatement::StringTooLong;
            }
            username[..username_bytes.len()].copy_from_slice(username_bytes);

            let mut email = [0u8; COLUMN_EMAIL_SIZE];
            let email_bytes = parts[3].as_bytes();
            if email_bytes.len() > COLUMN_EMAIL_SIZE {
                return PrepareStatement::StringTooLong;
            }
            email[..email_bytes.len()].copy_from_slice(email_bytes);

            PrepareStatement::Success(Statement::Insert(Box::new(Row {
                id,
                username,
                email,
            })))
        } else {
            PrepareStatement::Unrecognized
        }
    }
}

enum ExecuteResult {
    Success,
    TableFull,
}

enum Statement {
    Select,
    Insert(Box<Row>),
}

impl Statement {
    fn execute(&self, table: &mut Table) -> ExecuteResult {
        match self {
            Statement::Select => Statement::execute_select(table),
            Statement::Insert(row) => Statement::execute_insert(row, table),
        }
    }

    fn execute_select(table: &Table) -> ExecuteResult {
        for i in 0..table.num_rows {
            let row_slot = table.fetch_row(i);
            let row = deserialize_row(row_slot);
            println!("{}", row);
        }
        ExecuteResult::Success
    }

    fn execute_insert(row: &Row, table: &mut Table) -> ExecuteResult {
        if table.num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::TableFull;
        }

        let row_slot = table.insert_row(table.num_rows);
        serialize_row(row, row_slot);
        table.num_rows += 1;

        ExecuteResult::Success
    }
}

struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let username_end = self
            .username
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(self.username.len());
        let username = std::str::from_utf8(&self.username[..username_end]).unwrap_or("");

        let email_end = self
            .email
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(self.email.len());
        let email = std::str::from_utf8(&self.email[..email_end]).unwrap_or("");

        write!(f, "({}, {}, {})", self.id, username, email)
    }
}

struct Table {
    num_rows: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES],
}

impl Table {
    fn new() -> Self {
        Table {
            num_rows: 0,
            pages: [(); TABLE_MAX_PAGES].map(|_| None),
        }
    }

    fn fetch_row(&self, row_num: usize) -> &[u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset_in_page = row_num % ROWS_PER_PAGE;
        let byte_offset_in_page = row_offset_in_page * ROW_SIZE;

        if self.pages[page_num].is_none() {
            panic!("Tried to access a row in a page that has not been allocated.");
        }

        let page = self.pages[page_num].as_ref().unwrap();
        &page[byte_offset_in_page..byte_offset_in_page + ROW_SIZE]
    }

    fn insert_row(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset_in_page = row_num % ROWS_PER_PAGE;
        let byte_offset_in_page = row_offset_in_page * ROW_SIZE;

        // Allocate a new page if necessary
        if self.pages[page_num].is_none() {
            let new_page = Box::new([0u8; PAGE_SIZE]);
            self.pages[page_num] = Some(new_page);
        }

        let page = self.pages[page_num].as_mut().unwrap();
        &mut page[byte_offset_in_page..byte_offset_in_page + ROW_SIZE]
    }
}

fn print_prompt() {
    print!("db > ");
    use std::io::Write;
    io::stdout().flush().unwrap();
}

fn serialize_row(source: &Row, destination: &mut [u8]) {
    destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&source.id.to_le_bytes());
    destination[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE].copy_from_slice(&source.username);
    destination[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE].copy_from_slice(&source.email);
}

fn deserialize_row(source: &[u8]) -> Row {
    let mut id_bytes = [0u8; ID_SIZE];
    id_bytes.copy_from_slice(&source[ID_OFFSET..ID_OFFSET + ID_SIZE]);
    let id = u32::from_le_bytes(id_bytes);

    let mut username = [0u8; COLUMN_USERNAME_SIZE];
    username.copy_from_slice(&source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]);

    let mut email = [0u8; COLUMN_EMAIL_SIZE];
    email.copy_from_slice(&source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]);

    Row {
        id,
        username,
        email,
    }
}

fn main() {
    let mut table = Table::new();
    let mut input_buffer = InputBuffer::new();

    loop {
        print_prompt();
        input_buffer.read_input();

        let statement = match InputType::parse(&input_buffer.buffer) {
            InputType::Meta(MetaCommands::Exit) => break,
            InputType::Meta(MetaCommands::Unrecognized) => {
                println!("Unrecognized command: {}.", input_buffer.buffer);
                continue;
            }
            InputType::PrepareStatement(PrepareStatement::SyntaxError) => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            InputType::PrepareStatement(PrepareStatement::StringTooLong) => {
                println!("String is too long.");
                continue;
            }
            InputType::PrepareStatement(PrepareStatement::NegativeID) => {
                println!("ID must be positive.");
                continue;
            }
            InputType::PrepareStatement(PrepareStatement::Unrecognized) => {
                println!("Unrecognized keyword at start of {}.", input_buffer.buffer);
                continue;
            }
            InputType::PrepareStatement(PrepareStatement::Success(st)) => st,
        };

        match statement.execute(&mut table) {
            ExecuteResult::Success => println!("Executed."),
            ExecuteResult::TableFull => println!("Error: Table full."),
        }
    }
}

enum InputType {
    Meta(MetaCommands),
    PrepareStatement(PrepareStatement),
}

impl InputType {
    fn parse(input: &str) -> InputType {
        if let Some(meta) = MetaCommands::parse(input) {
            InputType::Meta(meta)
        } else {
            InputType::PrepareStatement(PrepareStatement::parse(input))
        }
    }
}
