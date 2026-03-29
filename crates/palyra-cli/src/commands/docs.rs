use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;

use crate::DocsCommand;

const DOCS_DIR: &str = "docs";
const HELP_SOURCE_DIR: &str = "crates/palyra-cli/tests/help_snapshots";
const HELP_BUNDLED_DIR: &str = "docs/help_snapshots";
const TOP_LEVEL_README: &str = "README.md";

#[derive(Debug, Clone)]
struct DocsLayout {
    docs_root: PathBuf,
    help_root: PathBuf,
    readme_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum IndexedDocKind {
    Docs,
    Help,
}

impl IndexedDocKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Docs => "docs",
            Self::Help => "help",
        }
    }
}

#[derive(Debug, Clone)]
struct IndexedDoc {
    slug: String,
    title: String,
    kind: IndexedDocKind,
    relative_path: String,
    absolute_path: PathBuf,
    content: String,
}

#[derive(Debug, Serialize)]
struct IndexedDocSummary<'a> {
    slug: &'a str,
    title: &'a str,
    kind: &'a str,
    path: &'a str,
}

#[derive(Debug, Serialize)]
struct SearchResult<'a> {
    score: usize,
    slug: &'a str,
    title: &'a str,
    kind: &'a str,
    path: &'a str,
}

#[derive(Debug, Serialize)]
struct ShowResult<'a> {
    slug: &'a str,
    title: &'a str,
    kind: &'a str,
    path: &'a str,
    content: &'a str,
}

pub(crate) fn run_docs(command: DocsCommand) -> Result<()> {
    let index = build_docs_index()?;
    match command {
        DocsCommand::List { json } => emit_docs_list(index.as_slice(), json),
        DocsCommand::Search { query, limit, json } => {
            emit_docs_search(index.as_slice(), &query, limit, json)
        }
        DocsCommand::Show { slug_or_path, json } => {
            emit_docs_show(index.as_slice(), &slug_or_path, json)
        }
    }
}

fn emit_docs_list(index: &[IndexedDoc], json: bool) -> Result<()> {
    if json {
        let payload = index
            .iter()
            .map(|entry| IndexedDocSummary {
                slug: entry.slug.as_str(),
                title: entry.title.as_str(),
                kind: entry.kind.as_str(),
                path: entry.relative_path.as_str(),
            })
            .collect::<Vec<_>>();
        serde_json::to_writer_pretty(std::io::stdout(), &payload)
            .context("failed to serialize docs list JSON")?;
        println!();
        return Ok(());
    }

    for entry in index {
        println!(
            "docs.item kind={} slug={} path={} title={}",
            entry.kind.as_str(),
            entry.slug,
            entry.relative_path,
            entry.title
        );
    }
    Ok(())
}

fn emit_docs_search(index: &[IndexedDoc], query: &str, limit: usize, json: bool) -> Result<()> {
    let query = query.trim();
    if query.is_empty() {
        bail!("docs search requires a non-empty query");
    }

    let mut results = index
        .iter()
        .filter_map(|entry| {
            let score = score_doc_match(entry, query);
            (score > 0).then_some(SearchResult {
                score,
                slug: entry.slug.as_str(),
                title: entry.title.as_str(),
                kind: entry.kind.as_str(),
                path: entry.relative_path.as_str(),
            })
        })
        .collect::<Vec<_>>();
    results.sort_by(|left, right| {
        right.score.cmp(&left.score).then_with(|| left.slug.cmp(right.slug))
    });
    results.truncate(limit.max(1));

    if results.is_empty() {
        bail!("no committed docs/help matched query `{query}`");
    }

    if json {
        serde_json::to_writer_pretty(std::io::stdout(), &results)
            .context("failed to serialize docs search JSON")?;
        println!();
        return Ok(());
    }

    for result in results {
        println!(
            "docs.match score={} kind={} slug={} path={} title={}",
            result.score, result.kind, result.slug, result.path, result.title
        );
    }
    Ok(())
}

fn emit_docs_show(index: &[IndexedDoc], requested: &str, json: bool) -> Result<()> {
    let entry = resolve_requested_doc(index, requested)?;
    if json {
        let payload = ShowResult {
            slug: entry.slug.as_str(),
            title: entry.title.as_str(),
            kind: entry.kind.as_str(),
            path: entry.relative_path.as_str(),
            content: entry.content.as_str(),
        };
        serde_json::to_writer_pretty(std::io::stdout(), &payload)
            .context("failed to serialize docs show JSON")?;
        println!();
        return Ok(());
    }

    println!(
        "docs.show kind={} slug={} path={} title={}",
        entry.kind.as_str(),
        entry.slug,
        entry.relative_path,
        entry.title
    );
    println!();
    print!("{}", entry.content);
    if !entry.content.ends_with('\n') {
        println!();
    }
    Ok(())
}

fn build_docs_index() -> Result<Vec<IndexedDoc>> {
    let layout = resolve_docs_layout()?;

    let mut entries = Vec::new();
    index_tree(
        layout.docs_root.as_path(),
        layout.docs_root.as_path(),
        IndexedDocKind::Docs,
        "docs",
        &mut entries,
    )?;
    index_tree(
        layout.help_root.as_path(),
        layout.help_root.as_path(),
        IndexedDocKind::Help,
        "help",
        &mut entries,
    )?;

    if let Some(readme_path) = layout.readme_path.as_ref().filter(|path| path.is_file()) {
        entries.push(load_indexed_doc(
            IndexedDocKind::Docs,
            readme_path.as_path(),
            TOP_LEVEL_README.to_owned(),
        )?);
    }

    entries.sort_by(|left, right| left.slug.cmp(&right.slug));
    Ok(entries)
}

fn index_tree(
    root: &Path,
    current: &Path,
    kind: IndexedDocKind,
    logical_prefix: &str,
    entries: &mut Vec<IndexedDoc>,
) -> Result<()> {
    for entry in fs::read_dir(current)
        .with_context(|| format!("failed to read docs directory {}", current.display()))?
    {
        let entry = entry
            .with_context(|| format!("failed to enumerate docs entry in {}", current.display()))?;
        let path = entry.path();
        if path.is_dir() {
            index_tree(root, &path, kind, logical_prefix, entries)?;
            continue;
        }
        if !is_indexable_doc_path(&path) {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("failed to relativize docs path {}", path.display()))?;
        let normalized_relative = normalize_display_path(relative);
        let logical_relative = if normalized_relative.is_empty() {
            logical_prefix.to_owned()
        } else {
            format!("{logical_prefix}/{normalized_relative}")
        };
        entries.push(load_indexed_doc(kind, path.as_path(), logical_relative)?);
    }
    Ok(())
}

fn load_indexed_doc(
    kind: IndexedDocKind,
    path: &Path,
    logical_relative: String,
) -> Result<IndexedDoc> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read committed docs file {}", path.display()))?;
    Ok(IndexedDoc {
        slug: doc_slug(logical_relative.as_str(), kind),
        title: doc_title(path, &content),
        kind,
        relative_path: logical_relative,
        absolute_path: path.to_path_buf(),
        content,
    })
}

fn source_repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("failed to resolve repository root from CLI manifest directory"))
}

fn resolve_docs_layout() -> Result<DocsLayout> {
    let source_root = source_repo_root()?;
    let source_docs = source_root.join(DOCS_DIR);
    let source_help = source_root.join(HELP_SOURCE_DIR);
    if source_docs.is_dir() && source_help.is_dir() {
        return Ok(DocsLayout {
            docs_root: source_docs,
            help_root: source_help,
            readme_path: Some(source_root.join(TOP_LEVEL_README)),
        });
    }

    let current_exe = env::current_exe().context("failed to resolve current CLI executable")?;
    let install_root = current_exe.parent().map(Path::to_path_buf).ok_or_else(|| {
        anyhow!("failed to resolve install root from current CLI path {}", current_exe.display())
    })?;
    let bundled_docs = install_root.join(DOCS_DIR);
    let bundled_help = install_root.join(HELP_BUNDLED_DIR);
    if bundled_docs.is_dir() && bundled_help.is_dir() {
        return Ok(DocsLayout {
            docs_root: bundled_docs,
            help_root: bundled_help,
            readme_path: None,
        });
    }

    bail!(
        "docs index roots are unavailable; expected either source docs at {} and {} or bundled docs at {} and {}",
        source_docs.display(),
        source_help.display(),
        bundled_docs.display(),
        bundled_help.display()
    )
}

fn is_indexable_doc_path(path: &Path) -> bool {
    matches!(path.extension().and_then(|value| value.to_str()), Some("md" | "txt"))
}

fn doc_slug(logical_relative: &str, kind: IndexedDocKind) -> String {
    let mut without_extension = PathBuf::from(logical_relative);
    without_extension.set_extension("");
    let normalized = normalize_display_path(&without_extension);
    if normalized.eq_ignore_ascii_case("README") {
        return "readme".to_owned();
    }
    if normalized.ends_with("/README") {
        return normalized[..normalized.len() - "/README".len()].to_ascii_lowercase();
    }
    if kind == IndexedDocKind::Help {
        return normalized.to_ascii_lowercase();
    }
    normalized.trim_start_matches("docs/").to_ascii_lowercase()
}

fn doc_title(relative: &Path, content: &str) -> String {
    if let Some(title) = content.lines().find_map(|line| {
        line.strip_prefix("# ")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }) {
        return title;
    }
    relative.file_stem().and_then(|value| value.to_str()).unwrap_or("untitled").replace('-', " ")
}

fn normalize_display_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => value.to_str().map(ToOwned::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn normalize_requested_doc(value: &str) -> String {
    value.trim().replace('\\', "/").trim_start_matches("./").to_ascii_lowercase()
}

fn resolve_requested_doc<'a>(index: &'a [IndexedDoc], requested: &str) -> Result<&'a IndexedDoc> {
    let requested = requested.trim();
    if requested.is_empty() {
        bail!("docs show requires a non-empty slug or path");
    }

    if let Some(by_path) = resolve_requested_doc_by_path(index, requested)? {
        return Ok(by_path);
    }

    let normalized = normalize_requested_doc(requested);
    if let Some(exact) = index.iter().find(|entry| {
        entry.slug == normalized
            || normalize_requested_doc(entry.relative_path.as_str()) == normalized
    }) {
        return Ok(exact);
    }

    let basename_matches = index
        .iter()
        .filter(|entry| {
            entry
                .absolute_path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(|value| value.eq_ignore_ascii_case(normalized.as_str()))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    match basename_matches.as_slice() {
        [entry] => Ok(*entry),
        [] => bail!("no committed docs/help entry matched `{requested}`"),
        many => {
            let matches = many
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{requested}` matched multiple docs entries: {matches}");
        }
    }
}

fn resolve_requested_doc_by_path<'a>(
    index: &'a [IndexedDoc],
    requested: &str,
) -> Result<Option<&'a IndexedDoc>> {
    let layout = resolve_docs_layout()?;
    let candidate = PathBuf::from(requested);
    let candidate =
        if candidate.is_absolute() { candidate } else { env::current_dir()?.join(candidate) };
    if !candidate.exists() {
        return Ok(None);
    }
    let canonical = candidate
        .canonicalize()
        .with_context(|| format!("failed to canonicalize docs path {}", candidate.display()))?;
    let docs_root = layout.docs_root.canonicalize().with_context(|| {
        format!("failed to canonicalize docs directory {}", layout.docs_root.display())
    })?;
    let help_root = layout.help_root.canonicalize().with_context(|| {
        format!("failed to canonicalize help snapshots directory {}", layout.help_root.display())
    })?;
    let readme_path = layout.readme_path.as_ref().and_then(|path| path.canonicalize().ok());
    let allowed = canonical.starts_with(&docs_root)
        || canonical.starts_with(&help_root)
        || readme_path.as_ref().is_some_and(|path| canonical == *path);
    if !allowed {
        bail!("docs show only allows committed docs/ and CLI help snapshot paths");
    }
    Ok(index.iter().find(|entry| entry.absolute_path == canonical))
}

fn score_doc_match(entry: &IndexedDoc, query: &str) -> usize {
    let query = query.to_ascii_lowercase();
    let slug_hits = match_count(entry.slug.as_str(), query.as_str()) * 8;
    let title_hits = match_count(entry.title.as_str(), query.as_str()) * 6;
    let path_hits = match_count(entry.relative_path.as_str(), query.as_str()) * 4;
    let content_hits = match_count(entry.content.as_str(), query.as_str());
    slug_hits + title_hits + path_hits + content_hits
}

fn match_count(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return 0;
    }
    haystack.to_ascii_lowercase().match_indices(needle).count()
}

#[cfg(test)]
mod tests {
    use super::{doc_slug, doc_title, match_count, normalize_requested_doc, IndexedDocKind};
    use std::path::Path;

    #[test]
    fn normalize_requested_doc_accepts_windows_style_paths() {
        assert_eq!(
            normalize_requested_doc(r".\docs\architecture\README.md"),
            "docs/architecture/readme.md"
        );
    }

    #[test]
    fn doc_title_prefers_markdown_heading() {
        assert_eq!(
            doc_title(Path::new("docs/example.md"), "# Example Title\n\nBody"),
            "Example Title"
        );
    }

    #[test]
    fn doc_slug_keeps_bundled_help_prefix() {
        assert_eq!(doc_slug("help/docs-help.txt", IndexedDocKind::Help), "help/docs-help");
    }

    #[test]
    fn match_count_is_case_insensitive() {
        assert_eq!(match_count("ACP bridge acp", "acp"), 2);
    }
}
