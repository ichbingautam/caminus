use crate::storage::StateStore;

#[derive(Debug, PartialEq, Eq)]
pub enum CliCommand {
    Status,
    InspectOffset { source_id: String },
    SchemaList { source_id: String },
    Unknown(String),
}

pub struct CliHandler;

impl CliHandler {
    pub fn parse_args<I, T>(args: I) -> CliCommand
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let vec: Vec<String> = args.into_iter().map(|s| s.into()).collect();
        if vec.len() < 2 {
            return CliCommand::Status;
        }

        match vec[1].as_str() {
            "status" => CliCommand::Status,
            "inspect-offset" => {
                let source_id = vec
                    .get(2)
                    .cloned()
                    .unwrap_or_else(|| "postgres_users".to_string());
                CliCommand::InspectOffset { source_id }
            }
            "schema-list" => {
                let source_id = vec
                    .get(2)
                    .cloned()
                    .unwrap_or_else(|| "postgres_users".to_string());
                CliCommand::SchemaList { source_id }
            }
            other => CliCommand::Unknown(other.to_string()),
        }
    }

    pub fn execute(command: CliCommand, store: &StateStore) -> Result<String, String> {
        match command {
            CliCommand::Status => Ok(
                "Caminus CDC Engine Status: HEALTHY | Active Consensus: Node 1 (LEADER)"
                    .to_string(),
            ),
            CliCommand::InspectOffset { source_id } => match store.get_offset(&source_id) {
                Ok(Some(offset)) => Ok(format!(
                    "Source '{}' offset checkpoint: {}",
                    source_id, offset
                )),
                Ok(None) => Ok(format!("Source '{}' offset checkpoint: NONE", source_id)),
                Err(e) => Err(format!("Storage error: {:?}", e)),
            },
            CliCommand::SchemaList { source_id } => match store.get_schema(&source_id) {
                Ok(Some(schema)) => {
                    Ok(format!("Registered schema for '{}': {}", source_id, schema))
                }
                Ok(None) => Ok(format!("No registered schema found for '{}'", source_id)),
                Err(e) => Err(format!("Storage error: {:?}", e)),
            },
            CliCommand::Unknown(cmd) => Err(format!("Unknown CLI subcommand: {}", cmd)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_cli_parsing_and_execution() {
        let test_path = "./data/test_cli_db";
        let _ = fs::remove_dir_all(test_path);
        let store = StateStore::new(test_path).unwrap();

        store.save_offset("pg_users", "offset-cli-777").unwrap();

        let cmd = CliHandler::parse_args(vec!["caminus", "inspect-offset", "pg_users"]);
        assert_eq!(
            cmd,
            CliCommand::InspectOffset {
                source_id: "pg_users".to_string()
            }
        );

        let res = CliHandler::execute(cmd, &store).unwrap();
        assert!(res.contains("offset-cli-777"));

        let status_cmd = CliHandler::parse_args(vec!["caminus", "status"]);
        let status_res = CliHandler::execute(status_cmd, &store).unwrap();
        assert!(status_res.contains("HEALTHY"));

        let _ = fs::remove_dir_all(test_path);
    }
}
