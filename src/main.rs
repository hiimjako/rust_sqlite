use clap::{Parser, arg};
use rust_sqlite::*;
use std::error::Error;
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
        if input.starts_with('.') {
            match input {
                ".exit" => Some(MetaCommands::Exit),
                _ => Some(MetaCommands::Unrecognized),
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
enum PrepareError {
    SyntaxError(String),
    StringTooLong,
    UnrecognizedStatement,
    InvalidId,
}

#[derive(Debug)]
enum ExecuteError {
    TableFull,
    Io(io::Error),
}

impl Error for PrepareError {}
impl Error for ExecuteError {}

impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrepareError::SyntaxError(s) => write!(f, "Syntax error: {}", s),
            PrepareError::StringTooLong => write!(f, "String is too long."),
            PrepareError::UnrecognizedStatement => write!(f, "Unrecognized statement."),
            PrepareError::InvalidId => write!(f, "ID must be positive."),
        }
    }
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecuteError::TableFull => write!(f, "Error: Table full."),
            ExecuteError::Io(e) => write!(f, "IO Error: {}", e),
        }
    }
}

impl From<io::Error> for ExecuteError {
    fn from(err: io::Error) -> Self {
        ExecuteError::Io(err)
    }
}

/// Represents a database statement.
enum Statement {
    Select,
    Insert(Box<Row>),
}

impl Statement {
    /// Parses a raw input string into a `Statement`.
    /// Returns a `Result` to handle parsing errors gracefully.
    fn prepare(input: &str) -> Result<Statement, PrepareError> {
        if input.starts_with("select") {
            Ok(Statement::Select)
        } else if input.starts_with("insert") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() != 4 {
                return Err(PrepareError::SyntaxError(
                    "Expected 'insert <id> <username> <email>'".to_string(),
                ));
            }

            let id = parts[1]
                .parse::<u32>()
                .map_err(|_| PrepareError::InvalidId)?;

            let username_bytes = parts[2].as_bytes();
            if username_bytes.len() > USERNAME_SIZE {
                return Err(PrepareError::StringTooLong);
            }
            let mut username = [0u8; USERNAME_SIZE];
            username[..username_bytes.len()].copy_from_slice(username_bytes);

            let email_bytes = parts[3].as_bytes();
            if email_bytes.len() > EMAIL_SIZE {
                return Err(PrepareError::StringTooLong);
            }
            let mut email = [0u8; EMAIL_SIZE];
            email[..email_bytes.len()].copy_from_slice(email_bytes);

            Ok(Statement::Insert(Box::new(Row {
                id,
                username,
                email,
            })))
        } else {
            Err(PrepareError::UnrecognizedStatement)
        }
    }

    /// Executes the statement against the provided table.
    fn execute(&self, table: &mut Table) -> Result<(), ExecuteError> {
        match self {
            Statement::Select => {
                self.select(table);
                Ok(())
            }
            Statement::Insert(row) => self.insert(table, row),
        }
    }

    fn select(&self, table: &mut Table) {
        for row in table.table_start() {
            println!("{}", row);
        }
    }

    fn insert(&self, table: &mut Table, row: &Row) -> Result<(), ExecuteError> {
        if table.num_rows >= TABLE_MAX_ROWS {
            return Err(ExecuteError::TableFull);
        }

        let mut cursor = table.table_end();
        row.serialize(cursor.value());
        table.num_rows += 1;
        Ok(())
    }
}

/// Represents a single row in the database table.
/// The `username` and `email` fields are fixed-size arrays to ensure
/// each row has a constant size, simplifying serialization and disk I/O.
#[derive(Debug)]
struct Row {
    id: u32,
    username: [u8; USERNAME_SIZE],
    email: [u8; EMAIL_SIZE],
}

impl Row {
    /// Serializes a `Row` into a byte slice for writing to disk.
    fn serialize(&self, destination: &mut [u8]) {
        destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        destination[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]
            .copy_from_slice(&self.username);
        destination[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE].copy_from_slice(&self.email);
    }

    /// Deserializes a byte slice into a `Row`.
    fn deserialize(source: &[u8]) -> Row {
        let mut id_bytes = [0u8; ID_SIZE];
        id_bytes.copy_from_slice(&source[ID_OFFSET..ID_OFFSET + ID_SIZE]);
        let id = u32::from_le_bytes(id_bytes);

        let mut username = [0u8; USERNAME_SIZE];
        username.copy_from_slice(&source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]);

        let mut email = [0u8; EMAIL_SIZE];
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

/// A cursor for iterating over the rows in a table.
struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

impl Cursor<'_> {
    /// Gets a mutable slice pointing to the memory location for the cursor's current row.
    fn value(&mut self) -> &mut [u8] {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.table.pager.get_page(page_num);

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }

    /// Advances the cursor to the next row.
    fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }
}

impl Iterator for Cursor<'_> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end_of_table {
            return None;
        }

        let row = Row::deserialize(self.value());
        self.advance();
        Some(row)
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
    fn db_close(mut self) -> io::Result<()> {
        self.pager.flush_all(self.num_rows)
    }

    /// Creates an iterator over the rows of the table.
    fn table_start(&mut self) -> Cursor {
        let end_of_table = self.num_rows == 0;
        Cursor {
            table: self,
            row_num: 0,
            end_of_table,
        }
    }

    /// Creates an iterator over the rows of the table.
    fn table_end(&mut self) -> Cursor {
        let row_num = self.num_rows;
        Cursor {
            table: self,
            row_num,
            end_of_table: true,
        }
    }
}

/// Manages reading and writing pages from the database file.
/// Implements an in-memory cache to reduce disk I/O.
struct Pager {
    file: File,
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
        let pages = std::array::from_fn(|_| None);

        Ok(Self {
            file,
            file_length,
            pages,
        })
    }

    /// Retrieves a page from the pager's cache or loads it from the file.
    pub fn get_page(&mut self, page_num: usize) -> &mut [u8; PAGE_SIZE] {
        assert!(page_num < TABLE_MAX_PAGES, "Page number out of bounds");

        if self.pages[page_num].is_none() {
            // Cache miss. Allocate memory and load from file.
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let num_pages_on_disk = (self.file_length as usize).div_ceil(PAGE_SIZE);

            if page_num < num_pages_on_disk {
                self.file
                    .seek(io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .expect("Unable to set page offset in file.");

                let remaining_bytes = self.file_length as usize - (page_num * PAGE_SIZE);
                let bytes_to_read = std::cmp::min(remaining_bytes, PAGE_SIZE);
                if bytes_to_read > 0 {
                    self.file
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

    /// Writes a page to the file.
    pub fn flush_page(&mut self, page_num: usize, size: usize) -> io::Result<()> {
        if self.pages[page_num].is_none() {
            panic!("Tried to flush a null page: {}", page_num);
        }

        self.file
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
        if let Some(page) = self.pages[page_num].as_ref() {
            self.file.write_all(&page[..size])?;
        }

        Ok(())
    }

    /// Flushes all dirty pages to disk before closing.
    fn flush_all(&mut self, num_rows: usize) -> io::Result<()> {
        let num_full_pages = num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            self.flush_page(i, PAGE_SIZE)?;
        }

        let num_additional_rows = num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let last_page_num = num_full_pages;
            let size_to_flush = num_additional_rows * ROW_SIZE;
            self.flush_page(last_page_num, size_to_flush)?;
        }

        self.file.flush()
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

        if input_buffer.buffer.is_empty() {
            continue;
        }

        let statement = match InputType::parse(&input_buffer.buffer) {
            InputType::Meta(MetaCommands::Exit) => {
                table.db_close().expect("Error while closing db");
                break;
            }
            InputType::Meta(MetaCommands::Unrecognized) => {
                println!("Unrecognized command: {}.", input_buffer.buffer);
                continue;
            }
            InputType::Statement(statement) => match statement {
                Ok(statement) => statement,
                Err(err) => {
                    println!("{}", err);
                    continue;
                }
            },
        };

        match statement.execute(&mut table) {
            Ok(_) => println!("Executed."),
            Err(err) => println!("{}", err),
        }
    }
}

/// A top-level enum to determine if the input is a meta-command or a SQL statement.
enum InputType {
    Meta(MetaCommands),
    Statement(Result<Statement, PrepareError>),
}

impl InputType {
    /// Parses the user's input to determine the type of command.
    fn parse(input: &str) -> InputType {
        if let Some(meta) = MetaCommands::parse(input) {
            InputType::Meta(meta)
        } else {
            InputType::Statement(Statement::prepare(input))
        }
    }
}
