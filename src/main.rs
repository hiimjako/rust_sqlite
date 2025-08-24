use clap::{Parser, arg};
use rust_sqlite::*;
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fmt,
    fs::{File, OpenOptions},
    io,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

/// Represents a simple buffer for reading command-line input.
struct InputBuffer {
    buffer: String,
}

impl InputBuffer {
    /// Creates a new, empty InputBuffer.
    fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    /// Reads a line from standard input and trims whitespace.
    fn read_input(&mut self) {
        self.buffer.clear();
        io::stdin()
            .read_line(&mut self.buffer)
            .expect("Failed to read line");
        self.buffer = self.buffer.trim().to_string();
    }
}

/// Non-SQL statements like `.exit` are called "meta-commands".
enum MetaCommands {
    Exit,
    Unrecognized,
}

impl MetaCommands {
    /// Parses an input string to check for a valid meta-command.
    fn parse(input: &str) -> Option<MetaCommands> {
        match input {
            ".exit" => Some(MetaCommands::Exit),
            _ => {
                if input.starts_with('.') {
                    Some(MetaCommands::Unrecognized)
                } else {
                    None
                }
            }
        }
    }
}

/// Represents the result of preparing a statement.
enum PrepareStatement {
    Success(Statement),
    SyntaxError,
    NegativeID,
    StringTooLong,
    Unrecognized,
}

impl PrepareStatement {
    /// Parses an input string into a `Statement`.
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

/// Represents the result of executing a statement.
enum ExecuteResult {
    Success,
    TableFull,
}

/// Represents a database statement.
enum Statement {
    Select,
    Insert(Box<Row>),
}

impl Statement {
    /// Executes the statement against the provided table.
    fn execute(&self, table: &mut Table) -> ExecuteResult {
        match self {
            Statement::Select => Statement::execute_select(table),
            Statement::Insert(row) => Statement::execute_insert(row, table),
        }
    }

    /// Executes a `SELECT` statement.
    fn execute_select(table: &mut Table) -> ExecuteResult {
        for i in 0..table.num_rows {
            let row_slot = table.fetch_row(i);
            let row = Row::deserialize(row_slot);
            println!("{}", row);
        }
        ExecuteResult::Success
    }

    /// Executes an `INSERT` statement.
    fn execute_insert(row: &Row, table: &mut Table) -> ExecuteResult {
        if table.num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::TableFull;
        }

        let row_slot = table.insert_row(table.num_rows);
        row.serialize(row_slot);
        table.num_rows += 1;

        ExecuteResult::Success
    }
}

/// Represents a single row in the database table.
struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    /// Serializes a `Row` into a byte slice for writing to disk.
    fn serialize(&self, destination: &mut [u8]) {
        destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        destination[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]
            .copy_from_slice(&self.username);
        destination[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE].copy_from_slice(&self.email);
    }

    /// Deserializes a byte slice into a `Row` for reading from disk.
    fn deserialize(source: &[u8]) -> Row {
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
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Find the end of the null-terminated strings for printing.
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

/// Represents the database table structure.
struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    /// Create the database connection. It creates the file in case it doesn't exist.
    fn db_open<P: AsRef<Path>>(filename: P) -> Result<Self, io::Error> {
        let pager = Pager::open(filename)?;
        let num_rows = std::cmp::min(pager.file_length as usize / ROW_SIZE, TABLE_MAX_ROWS);

        Ok(Table { num_rows, pager })
    }

    /// Closes the database and flushes changes to disk.
    fn close(&mut self) {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;

        for page_num in 0..num_full_pages {
            self.pager.flush_page(page_num, PAGE_SIZE);
            self.pager.clear_page(page_num);
        }

        if num_additional_rows > 0 {
            let num_bytes_to_flush = num_additional_rows * ROW_SIZE;
            self.pager.flush_page(num_full_pages, num_bytes_to_flush);
            self.pager.clear_page(num_full_pages);
        }
    }

    /// Fetches a row from the table at the given index.
    fn fetch_row(&mut self, row_num: usize) -> &[u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset_in_page = row_num % ROWS_PER_PAGE;
        let byte_offset_in_page = row_offset_in_page * ROW_SIZE;

        let page = self.pager.get_page(page_num);
        &page[byte_offset_in_page..byte_offset_in_page + ROW_SIZE]
    }

    /// Gets a mutable slice to a row slot for insertion.
    fn insert_row(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset_in_page = row_num % ROWS_PER_PAGE;
        let byte_offset_in_page = row_offset_in_page * ROW_SIZE;

        let page = self.pager.get_page(page_num);
        &mut page[byte_offset_in_page..byte_offset_in_page + ROW_SIZE]
    }
}

/// Manages database file pages and an in-memory page cache.
struct Pager {
    file_descriptor: File,
    file_length: u64,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES],
}

impl Pager {
    /// Opens a database file and returns a new Pager instance.
    fn open<P: AsRef<Path>>(filename: P) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600) // S_IWUSR | S_IRUSR
            .open(filename)
            .expect("Error while opening pager");

        let file_length = file.seek(SeekFrom::End(0))?;

        let pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES] = std::array::from_fn(|_| None);

        Ok(Self {
            file_descriptor: file,
            file_length,
            pages,
        })
    }

    /// Retrieves a page from the pager's cache or loads it from the file.
    pub fn get_page(&mut self, page_num: usize) -> &mut Box<[u8; PAGE_SIZE]> {
        if page_num >= TABLE_MAX_PAGES {
            panic!(
                "Tried to fetch page number out of bounds. {} >= {}",
                page_num, TABLE_MAX_PAGES
            )
        }

        if self.pages[page_num].is_none() {
            // Cache miss. Allocate memory and load from file.
            let mut page = Box::new([0u8; PAGE_SIZE]);

            let num_pages = (self.file_length as usize).div_ceil(PAGE_SIZE);

            if page_num < num_pages {
                self.file_descriptor
                    .seek(io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .expect("Unable to set page offset in file.");

                let remaining_bytes = self.file_length as usize - (page_num * PAGE_SIZE);
                let bytes_to_read = std::cmp::min(remaining_bytes, PAGE_SIZE);
                if bytes_to_read > 0 {
                    self.file_descriptor
                        .read_exact(&mut page[..bytes_to_read])
                        .expect("Unable to read the page from file.");
                }
            }

            self.pages[page_num] = Some(page);
        }

        self.pages[page_num]
            .as_mut()
            .expect("Accessing to not existing page.")
    }

    /// Remove the page from memory.
    pub fn clear_page(&mut self, page_num: usize) {
        if self.pages[page_num].is_none() {
            return;
        }

        self.pages[page_num] = None
    }

    /// Writes a page to the file.
    pub fn flush_page(&mut self, page_num: usize, size: usize) {
        if self.pages[page_num].is_none() {
            panic!("Tried to flush a null page: {}", page_num);
        }

        self.file_descriptor
            .seek(io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .expect("Error seeking");

        if let Some(page) = self.pages[page_num].as_ref() {
            self.file_descriptor
                .write_all(&page[..size])
                .expect("Error writing");
        }
    }
}

/// Prints the prompt to the console.
fn print_prompt() {
    print!("db > ");
    use std::io::Write;
    io::stdout().flush().unwrap();
}

#[derive(Parser)]
struct Cli {
    #[arg(trailing_var_arg = true)]
    filename: Vec<String>,
}

/// The main entry point for the database REPL (Read-Eval-Print Loop).
fn main() {
    let args = Cli::parse();

    if args.filename.is_empty() {
        println!("Must provide a database filename, run --help for info");
        return;
    }
    let filename = args.filename.first().unwrap();

    let mut table = Table::db_open(filename).expect("Unable to create db connection.");
    let mut input_buffer = InputBuffer::new();

    loop {
        print_prompt();
        input_buffer.read_input();

        let statement = match InputType::parse(&input_buffer.buffer) {
            InputType::Meta(MetaCommands::Exit) => {
                table.close();
                break;
            }
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

/// A top-level enum to determine if the input is a meta-command or a SQL statement.
enum InputType {
    Meta(MetaCommands),
    PrepareStatement(PrepareStatement),
}

impl InputType {
    /// Parses the user's input to determine the type of command.
    fn parse(input: &str) -> InputType {
        if let Some(meta) = MetaCommands::parse(input) {
            InputType::Meta(meta)
        } else {
            InputType::PrepareStatement(PrepareStatement::parse(input))
        }
    }
}
