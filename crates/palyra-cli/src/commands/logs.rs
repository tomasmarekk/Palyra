use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader},
};

use palyra_control_plane as control_plane;

use crate::*;

#[derive(Debug, Serialize)]
struct CliLogRecord {
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    line_number: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl From<control_plane::LogRecord> for CliLogRecord {
    fn from(value: control_plane::LogRecord) -> Self {
        Self {
            source: value.source,
            seq: None,
            event_id: value.event_name,
            kind: None,
            timestamp_unix_ms: Some(value.timestamp_unix_ms),
            line_number: None,
            message: Some(value.message),
        }
    }
}

enum LogInput {
    Journal { db_path: PathBuf },
    File { source: String, path: PathBuf },
    Unavailable { message: String },
}

pub(crate) fn run_logs(
    db_path: Option<String>,
    lines: usize,
    follow: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    let runtime = build_runtime()?;
    if let Ok(()) = runtime.block_on(run_console_logs(lines, follow, poll_interval_ms)) {
        return Ok(());
    }

    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for logs command"))?;
    if root_context.prefers_json() && follow {
        anyhow::bail!("`palyra logs --follow --json` is not supported; use NDJSON output instead");
    }

    let lines = lines.clamp(1, 500);
    let input = resolve_log_input(db_path.clone())?;
    match input {
        LogInput::Journal { db_path } => {
            run_journal_logs(db_path.as_path(), lines, follow, poll_interval_ms)
        }
        LogInput::File { source, path } => {
            run_file_logs(source.as_str(), path.as_path(), lines, follow, poll_interval_ms)
        }
        LogInput::Unavailable { message } => {
            run_unavailable_logs(db_path, lines, follow, poll_interval_ms, message)
        }
    }
}

async fn run_console_logs(lines: usize, follow: bool, poll_interval_ms: u64) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let mut query =
        control_plane::LogListQuery { limit: Some(lines.clamp(1, 500)), ..Default::default() };
    let initial = context.client.list_logs(&query).await?;
    let records = initial.records.into_iter().map(CliLogRecord::from).collect::<Vec<_>>();
    emit_log_records(records.as_slice())?;
    if !follow {
        return Ok(());
    }

    let sleep_duration = Duration::from_millis(poll_interval_ms.clamp(250, 30_000));
    let mut cursor = initial.newest_cursor;
    loop {
        tokio::time::sleep(sleep_duration).await;
        query.cursor = cursor.clone();
        query.direction = cursor.as_ref().map(|_| "after".to_owned());
        let response = context.client.list_logs(&query).await?;
        if response.records.is_empty() {
            continue;
        }
        cursor = response.newest_cursor.clone().or(cursor);
        let records = response.records.into_iter().map(CliLogRecord::from).collect::<Vec<_>>();
        emit_log_records(records.as_slice())?;
    }
}

fn resolve_log_input(db_path: Option<String>) -> Result<LogInput> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for logs command"))?;
    let db_path = resolve_daemon_journal_db_path(db_path)?;
    if db_path.is_file() {
        return Ok(LogInput::Journal { db_path });
    }

    if let Some(metadata) = support::service::load_service_metadata(root_context.state_root())? {
        let stdout_path = PathBuf::from(metadata.stdout_log_path);
        if stdout_path.is_file() {
            return Ok(LogInput::File { source: "service.stdout".to_owned(), path: stdout_path });
        }
        let stderr_path = PathBuf::from(metadata.stderr_log_path);
        if stderr_path.is_file() {
            return Ok(LogInput::File { source: "service.stderr".to_owned(), path: stderr_path });
        }
    }

    if !db_path.exists() {
        return Ok(LogInput::Unavailable {
            message: format!(
                "no journal or service logs exist yet for state_root={}; start the daemon in foreground with `palyra gateway run` to inspect startup errors",
                root_context.state_root().display()
            ),
        });
    }
    ensure_journal_db_exists(db_path.as_path())?;
    Ok(LogInput::Journal { db_path })
}

fn run_unavailable_logs(
    db_path: Option<String>,
    lines: usize,
    follow: bool,
    poll_interval_ms: u64,
    message: String,
) -> Result<()> {
    let notice = CliLogRecord {
        source: "diagnostic".to_owned(),
        seq: None,
        event_id: None,
        kind: None,
        timestamp_unix_ms: None,
        line_number: None,
        message: Some(message),
    };
    emit_log_records(&[notice])?;
    if !follow {
        return Ok(());
    }

    let sleep_duration = Duration::from_millis(poll_interval_ms.clamp(250, 30_000));
    loop {
        thread::sleep(sleep_duration);
        match resolve_log_input(db_path.clone())? {
            LogInput::Unavailable { .. } => continue,
            LogInput::Journal { db_path } => {
                return run_journal_logs(db_path.as_path(), lines, true, poll_interval_ms);
            }
            LogInput::File { source, path } => {
                return run_file_logs(
                    source.as_str(),
                    path.as_path(),
                    lines,
                    true,
                    poll_interval_ms,
                );
            }
        }
    }
}

fn run_journal_logs(
    db_path: &Path,
    lines: usize,
    follow: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    let records = collect_recent_journal_records(db_path, lines)?;
    emit_log_records(records.as_slice())?;
    if !follow {
        return Ok(());
    }

    let sleep_duration = Duration::from_millis(poll_interval_ms.clamp(250, 30_000));
    let mut last_seq = records.last().and_then(|record| record.seq).unwrap_or(0);
    loop {
        thread::sleep(sleep_duration);
        let follow_records = collect_follow_journal_records(db_path, last_seq)?;
        if let Some(value) = follow_records.last().and_then(|record| record.seq) {
            last_seq = value;
        }
        emit_log_records(follow_records.as_slice())?;
    }
}

fn collect_recent_journal_records(db_path: &Path, limit: usize) -> Result<Vec<CliLogRecord>> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open journal database {}", db_path.display()))?;
    let mut statement = connection.prepare(
        "SELECT seq, event_ulid, kind, timestamp_unix_ms, payload_json
         FROM journal_events
         ORDER BY seq DESC
         LIMIT ?1",
    )?;
    let mut rows = statement.query([limit as i64])?;
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        records.push(read_journal_log_record(row)?);
    }
    records.reverse();
    Ok(records)
}

fn collect_follow_journal_records(db_path: &Path, after_seq: i64) -> Result<Vec<CliLogRecord>> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open journal database {}", db_path.display()))?;
    let mut statement = connection.prepare(
        "SELECT seq, event_ulid, kind, timestamp_unix_ms, payload_json
         FROM journal_events
         WHERE seq > ?1
         ORDER BY seq ASC",
    )?;
    let mut rows = statement.query([after_seq])?;
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        records.push(read_journal_log_record(row)?);
    }
    Ok(records)
}

fn read_journal_log_record(row: &rusqlite::Row<'_>) -> Result<CliLogRecord> {
    let payload_json: String = row.get(4)?;
    Ok(CliLogRecord {
        source: "journal".to_owned(),
        seq: Some(row.get(0)?),
        event_id: Some(row.get(1)?),
        kind: Some(row.get(2)?),
        timestamp_unix_ms: Some(row.get(3)?),
        line_number: None,
        message: extract_support_bundle_error_message(payload_json.as_str()),
    })
}

fn run_file_logs(
    source: &str,
    path: &Path,
    lines: usize,
    follow: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    let records = collect_recent_file_records(source, path, lines)?;
    emit_log_records(records.as_slice())?;
    if !follow {
        return Ok(());
    }

    let sleep_duration = Duration::from_millis(poll_interval_ms.clamp(250, 30_000));
    let mut last_line_number = records.last().and_then(|record| record.line_number).unwrap_or(0);
    loop {
        thread::sleep(sleep_duration);
        let follow_records = collect_follow_file_records(source, path, last_line_number)?;
        if let Some(value) = follow_records.last().and_then(|record| record.line_number) {
            last_line_number = value;
        }
        emit_log_records(follow_records.as_slice())?;
    }
}

fn collect_recent_file_records(
    source: &str,
    path: &Path,
    limit: usize,
) -> Result<Vec<CliLogRecord>> {
    let file =
        File::open(path).with_context(|| format!("failed to open log file {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = VecDeque::with_capacity(limit);
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read log file {}", path.display()))?;
        if lines.len() == limit {
            lines.pop_front();
        }
        lines.push_back(CliLogRecord {
            source: source.to_owned(),
            seq: None,
            event_id: None,
            kind: None,
            timestamp_unix_ms: None,
            line_number: Some(index + 1),
            message: Some(line),
        });
    }
    Ok(lines.into_iter().collect())
}

fn collect_follow_file_records(
    source: &str,
    path: &Path,
    after_line_number: usize,
) -> Result<Vec<CliLogRecord>> {
    let file =
        File::open(path).with_context(|| format!("failed to open log file {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        if line_number <= after_line_number {
            continue;
        }
        let line = line.with_context(|| format!("failed to read log file {}", path.display()))?;
        records.push(CliLogRecord {
            source: source.to_owned(),
            seq: None,
            event_id: None,
            kind: None,
            timestamp_unix_ms: None,
            line_number: Some(line_number),
            message: Some(line),
        });
    }
    Ok(records)
}

fn emit_log_records(records: &[CliLogRecord]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for logs command"))?;
    if root_context.prefers_json() {
        return output::print_json_pretty(&records, "failed to encode logs output as JSON");
    }
    for record in records {
        emit_log_record(record)?;
    }
    Ok(())
}

fn emit_log_record(record: &CliLogRecord) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for logs command"))?;
    if root_context.prefers_ndjson() {
        return output::print_json_line(record, "failed to encode logs output as NDJSON");
    }
    if record.source == "journal" {
        println!(
            "logs.event source={} seq={} event_id={} kind={} timestamp_unix_ms={} message={}",
            record.source,
            record.seq.unwrap_or(0),
            record.event_id.as_deref().unwrap_or("none"),
            record.kind.unwrap_or(0),
            record.timestamp_unix_ms.unwrap_or(0),
            record.message.as_deref().unwrap_or("none")
        );
    } else if record.source == "diagnostic" {
        println!("logs.notice message={}", record.message.as_deref().unwrap_or("none"));
    } else {
        println!(
            "logs.line source={} line={} message={}",
            record.source,
            record.line_number.unwrap_or(0),
            record.message.as_deref().unwrap_or("none")
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

#[cfg(test)]
mod tests {
    use super::{collect_follow_file_records, collect_recent_file_records};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn collect_recent_file_records_keeps_last_n_lines() {
        let tempdir = tempdir().expect("tempdir");
        let log_path = tempdir.path().join("gateway.log");
        fs::write(log_path.as_path(), "one\ntwo\nthree\n").expect("write log");

        let records = collect_recent_file_records("service.stdout", log_path.as_path(), 2)
            .expect("collect recent records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].line_number, Some(2));
        assert_eq!(records[0].message.as_deref(), Some("two"));
        assert_eq!(records[1].line_number, Some(3));
        assert_eq!(records[1].message.as_deref(), Some("three"));
    }

    #[test]
    fn collect_follow_file_records_only_returns_new_lines() {
        let tempdir = tempdir().expect("tempdir");
        let log_path = tempdir.path().join("gateway.log");
        fs::write(log_path.as_path(), "one\ntwo\nthree\n").expect("write log");

        let records = collect_follow_file_records("service.stdout", log_path.as_path(), 2)
            .expect("collect follow records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].line_number, Some(3));
        assert_eq!(records[0].message.as_deref(), Some("three"));
    }
}
