#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    use assert_cmd::Command;
    use predicates::prelude::*;
    use rust_sqlite::{EMAIL_SIZE, TABLE_MAX_ROWS, USERNAME_SIZE};
    use tempfile::NamedTempFile;

    // Helper function to run the command with a temporary database file
    fn run_commands<T: AsRef<str>>(commands: &[T]) -> Command {
        let db_path = create_db_path();
        run_commands_with_args(commands, &db_path)
    }

    fn create_db_path() -> PathBuf {
        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
        let db_path: PathBuf = temp_file.path().to_path_buf();
        db_path
    }

    fn run_commands_with_args<T: AsRef<str>>(commands: &[T], db_path: &Path) -> Command {
        let mut cmd = Command::cargo_bin("rust-sqlite").expect("Failed to run command");
        cmd.arg(db_path.to_str().expect("Invalid path"));

        let input = commands
            .iter()
            .map(|s| s.as_ref())
            .collect::<Vec<_>>()
            .join("\n");
        cmd.write_stdin(input);
        cmd
    }

    #[test]
    fn it_inserts_and_retrieves_a_row() {
        let mut cmd = run_commands(&["insert 1 user1 person1@example.com", "select", ".exit"]);

        let expected = [
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ]
        .join("\n");

        cmd.assert().success().stdout(expected);
    }

    #[test]
    fn it_prints_error_message_when_table_is_full() {
        let mut commands = Vec::new();
        for i in 0..TABLE_MAX_ROWS + 1 {
            commands.push(format!("insert {i} user{i} person{i}@example.com"));
        }
        commands.push(String::from_str(".exit").unwrap());

        let mut cmd = run_commands(&commands);

        cmd.assert()
            .success()
            .stdout(predicate::str::contains("db > Error: Table full."));
    }

    #[test]
    fn it_fills_and_save_full_table() {
        let mut commands = Vec::new();
        let mut expected = Vec::new();
        for i in 0..TABLE_MAX_ROWS {
            commands.push(format!("insert {i} user{i} person{i}@example.com"));
            expected.push(format!("({i}, user{i}, person{i}@example.com)"));
        }
        commands.push(String::from_str(".exit").unwrap());

        let db_path = create_db_path();
        let mut cmd = run_commands_with_args(&commands, &db_path);

        cmd.assert()
            .success()
            .stdout(predicate::str::ends_with("db > "));

        let mut cmd = run_commands_with_args(&["select", ".exit"], &db_path);
        let expected = expected.join("\n");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(expected));
    }

    #[test]
    fn it_allows_inserting_strings_that_are_the_maximum_length() {
        let long_username = "a".repeat(USERNAME_SIZE);
        let long_email = "a".repeat(EMAIL_SIZE);

        let commands_string = [
            format!("insert 1 {} {}", &long_username, &long_email),
            String::from("select"),
            String::from(".exit"),
        ];

        let commands_slice: Vec<&str> = commands_string.iter().map(|s| s.as_str()).collect();

        let mut cmd = run_commands(&commands_slice);

        let expected = [
            String::from("db > Executed."),
            format!("db > (1, {}, {})", long_username, long_email),
            String::from("Executed."),
            String::from("db > "),
        ]
        .join("\n");

        cmd.assert().success().stdout(expected);
    }

    #[test]
    fn it_prints_error_message_if_strings_are_too_long() {
        let long_username = "a".repeat(USERNAME_SIZE + 1);
        let long_email = "a".repeat(EMAIL_SIZE + 1);

        let commands_string = [
            format!("insert 1 {} {}", &long_username, &long_email),
            String::from("select"),
            String::from(".exit"),
        ];

        let commands_slice: Vec<&str> = commands_string.iter().map(|s| s.as_str()).collect();

        let mut cmd = run_commands(&commands_slice);

        let expected = ["db > String is too long.", "db > Executed.", "db > "].join("\n");

        cmd.assert().success().stdout(expected);
    }

    #[test]
    fn it_prints_error_message_if_id_is_negative() {
        let mut cmd = run_commands(&["insert -1 user1 person1@example.com", "select", ".exit"]);

        let expected = ["db > ID must be positive.", "db > Executed.", "db > "].join("\n");

        cmd.assert().success().stdout(expected);
    }

    #[test]
    fn it_keeps_data_after_closing_connection() {
        let db_path = create_db_path();

        let mut cmd =
            run_commands_with_args(&["insert 1 user1 person1@example.com", ".exit"], &db_path);
        let expected = ["db > Executed.", "db > "].join("\n");
        cmd.assert().success().stdout(expected);

        let mut cmd = run_commands_with_args(&["select", ".exit"], &db_path);
        let expected = ["db > (1, user1, person1@example.com)\nExecuted.", "db > "].join("\n");
        cmd.assert().success().stdout(expected);
    }
}
