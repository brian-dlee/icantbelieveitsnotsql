use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum ButterError {
    #[error("{0}")]
    Config(String),

    #[error("unsupported dialect: {0}")]
    UnsupportedDialect(String),

    #[error("{}", format_diagnostics(.0))]
    Diagnostics(Vec<Diagnostic>),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A problem tied to a location in a SQL file.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub path: PathBuf,
    pub line: usize,
    pub message: String,
}

impl Diagnostic {
    pub fn new(path: &Path, line: usize, message: impl Into<String>) -> Diagnostic {
        Diagnostic {
            path: path.to_path_buf(),
            line,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.line > 0 {
            write!(f, "{}:{}: {}", self.path.display(), self.line, self.message)
        } else {
            write!(f, "{}: {}", self.path.display(), self.message)
        }
    }
}

fn format_diagnostics(diagnostics: &[Diagnostic]) -> String {
    let lines: Vec<String> = diagnostics.iter().map(|d| d.to_string()).collect();
    format!(
        "{} error{}:\n{}",
        diagnostics.len(),
        if diagnostics.len() == 1 { "" } else { "s" },
        lines.join("\n")
    )
}
