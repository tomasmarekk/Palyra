//! Backend-agnostic step engine for guided CLI wizards.
//!
//! `WizardSession` drives typed steps (text, select, confirm, ...) against a
//! `WizardBackend`: interactive stdin/stdout prompting, non-interactive flag-driven
//! answers, or scripted answers in tests. Onboarding and setup flows build on this
//! so the same step sequence works across all three modes.

#[cfg(test)]
use std::collections::VecDeque;
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    io::{self, Write},
};

/// Input modality of a single wizard step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StepKind {
    Note,
    Select,
    Text,
    Confirm,
    MultiSelect,
    Progress,
    Action,
}

/// One selectable option of a select/multi-select step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepChoice {
    pub(crate) value: String,
    pub(crate) label: String,
    pub(crate) hint: Option<String>,
}

/// Declarative description of a single wizard step; `id` keys prefilled and scripted
/// answers, so it must stay stable across releases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WizardStep {
    pub(crate) id: &'static str,
    pub(crate) kind: StepKind,
    pub(crate) title: Option<String>,
    pub(crate) message: String,
    pub(crate) default_value: Option<String>,
    pub(crate) placeholder: Option<String>,
    pub(crate) sensitive: bool,
    pub(crate) allow_empty: bool,
    pub(crate) options: Vec<StepChoice>,
}

impl WizardStep {
    /// Builds an informational step that prints text and expects no input.
    pub(crate) fn note(
        id: &'static str,
        title: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            id,
            kind: StepKind::Note,
            title: Some(title.into()),
            message: message.into(),
            default_value: None,
            placeholder: None,
            sensitive: false,
            allow_empty: true,
            options: Vec::new(),
        }
    }

    /// Builds a step that pauses for an explicit Enter/cancel acknowledgement.
    pub(crate) fn action(
        id: &'static str,
        title: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            id,
            kind: StepKind::Action,
            title: Some(title.into()),
            message: message.into(),
            default_value: None,
            placeholder: None,
            sensitive: false,
            allow_empty: true,
            options: Vec::new(),
        }
    }

    /// Builds a step that announces a long-running operation before it executes.
    pub(crate) fn progress(
        id: &'static str,
        title: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            id,
            kind: StepKind::Progress,
            title: Some(title.into()),
            message: message.into(),
            default_value: None,
            placeholder: None,
            sensitive: false,
            allow_empty: true,
            options: Vec::new(),
        }
    }
}

/// Typed answer produced by a wizard step.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum WizardValue {
    None,
    Text(String),
    SensitiveText(String),
    Bool(bool),
    Choice(String),
    Multi(Vec<String>),
}

// INTENTIONAL: manual Debug impl so sensitive input is never leaked through the
// `{other:?}` interpolation in validation error messages. Do not derive Debug.
impl fmt::Debug for WizardValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Text(value) => f.debug_tuple("Text").field(value).finish(),
            Self::SensitiveText(_) => f.write_str("SensitiveText(<redacted>)"),
            Self::Bool(value) => f.debug_tuple("Bool").field(value).finish(),
            Self::Choice(value) => f.debug_tuple("Choice").field(value).finish(),
            Self::Multi(values) => f.debug_tuple("Multi").field(values).finish(),
        }
    }
}

/// Failure modes of a wizard run; every variant carries the offending step id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WizardError {
    Cancelled { step_id: String },
    MissingInput { step_id: String, message: String },
    Validation { step_id: String, message: String },
    Io { step_id: String, message: String },
}

impl fmt::Display for WizardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled { step_id } => write!(f, "wizard cancelled at step {step_id}"),
            Self::MissingInput { step_id, message } => {
                write!(f, "wizard input missing at step {step_id}: {message}")
            }
            Self::Validation { step_id, message } => {
                write!(f, "wizard validation failed at step {step_id}: {message}")
            }
            Self::Io { step_id, message } => {
                write!(f, "wizard I/O failed at step {step_id}: {message}")
            }
        }
    }
}

impl Error for WizardError {}

/// Answer source for wizard steps (interactive prompts, flag defaults, or test scripts).
pub(crate) trait WizardBackend {
    /// Resolves one step to a value, prompting or looking up answers as needed.
    fn execute_step(&mut self, step: &WizardStep) -> Result<WizardValue, WizardError>;

    /// Reports a validation failure; backends either surface it and allow a retry
    /// or convert it into a terminal [`WizardError::Validation`].
    fn on_validation_error(&mut self, step: &WizardStep, message: &str) -> Result<(), WizardError>;

    /// Returns true when the backend can re-prompt after a validation failure.
    fn retries_on_validation_error(&self) -> bool;
}

/// Drives a sequence of typed steps against a [`WizardBackend`], enforcing that each
/// answer matches the step kind and re-running validation according to backend policy.
pub(crate) struct WizardSession<'a, B: ?Sized> {
    backend: &'a mut B,
}

impl<'a, B> WizardSession<'a, B>
where
    B: WizardBackend + ?Sized,
{
    pub(crate) fn new(backend: &'a mut B) -> Self {
        Self { backend }
    }

    /// Shows an informational step; never produces a value.
    ///
    /// # Errors
    /// Propagates backend I/O or cancellation errors.
    pub(crate) fn note(&mut self, step: WizardStep) -> Result<(), WizardError> {
        let _ = self.backend.execute_step(&step)?;
        Ok(())
    }

    /// Runs an acknowledgement step.
    ///
    /// # Errors
    /// Returns [`WizardError::Cancelled`] when the operator aborts, or backend I/O errors.
    pub(crate) fn action(&mut self, step: WizardStep) -> Result<(), WizardError> {
        let _ = self.backend.execute_step(&step)?;
        Ok(())
    }

    /// Announces `step`, then runs `op` and returns its result.
    ///
    /// # Errors
    /// Propagates backend errors from the announcement and any error returned by `op`.
    pub(crate) fn progress<T, F>(&mut self, step: WizardStep, op: F) -> Result<T, WizardError>
    where
        F: FnOnce() -> Result<T, WizardError>,
    {
        let _ = self.backend.execute_step(&step)?;
        op()
    }

    /// Collects a text answer, re-prompting on validation failures when the backend allows.
    ///
    /// # Errors
    /// Returns [`WizardError::Validation`] for non-text answers or terminal validation
    /// failures, and propagates cancellation and I/O errors from the backend.
    pub(crate) fn text<F>(&mut self, step: WizardStep, validator: F) -> Result<String, WizardError>
    where
        F: Fn(&str) -> Result<(), String>,
    {
        loop {
            let value = match self.backend.execute_step(&step)? {
                WizardValue::Text(value)
                | WizardValue::SensitiveText(value)
                | WizardValue::Choice(value) => value,
                WizardValue::None if step.allow_empty => String::new(),
                other => {
                    return Err(WizardError::Validation {
                        step_id: step.id.to_owned(),
                        message: format!("expected text-like input, received {other:?}"),
                    });
                }
            };
            match validator(value.as_str()) {
                Ok(()) => return Ok(value),
                Err(message) if self.backend.retries_on_validation_error() => {
                    self.backend.on_validation_error(&step, message.as_str())?;
                }
                Err(message) => {
                    return Err(WizardError::Validation { step_id: step.id.to_owned(), message })
                }
            }
        }
    }

    /// Collects a yes/no answer.
    ///
    /// # Errors
    /// Returns [`WizardError::Validation`] for non-boolean answers and propagates
    /// backend errors.
    pub(crate) fn confirm(&mut self, step: WizardStep) -> Result<bool, WizardError> {
        match self.backend.execute_step(&step)? {
            WizardValue::Bool(value) => Ok(value),
            other => Err(WizardError::Validation {
                step_id: step.id.to_owned(),
                message: format!("expected boolean input, received {other:?}"),
            }),
        }
    }

    /// Collects a single-choice answer (text-like answers are accepted as choice values).
    ///
    /// # Errors
    /// Returns [`WizardError::Validation`] for incompatible answers and propagates
    /// backend errors.
    pub(crate) fn select(&mut self, step: WizardStep) -> Result<String, WizardError> {
        match self.backend.execute_step(&step)? {
            WizardValue::Choice(value)
            | WizardValue::Text(value)
            | WizardValue::SensitiveText(value) => Ok(value),
            other => Err(WizardError::Validation {
                step_id: step.id.to_owned(),
                message: format!("expected selection input, received {other:?}"),
            }),
        }
    }

    /// Collects a multi-choice answer.
    ///
    /// # Errors
    /// Returns [`WizardError::Validation`] for non-multi answers and propagates
    /// backend errors.
    pub(crate) fn multiselect(&mut self, step: WizardStep) -> Result<Vec<String>, WizardError> {
        match self.backend.execute_step(&step)? {
            WizardValue::Multi(values) => Ok(values),
            other => Err(WizardError::Validation {
                step_id: step.id.to_owned(),
                message: format!("expected multi-select input, received {other:?}"),
            }),
        }
    }
}

/// Terminal backend that prompts on stdout/stdin; CLI flags may prefill answers,
/// each consumed at most once before falling back to a live prompt.
pub(crate) struct InteractiveWizardBackend {
    answers: BTreeMap<String, WizardValue>,
}

impl InteractiveWizardBackend {
    pub(crate) fn with_answers(answers: BTreeMap<String, WizardValue>) -> Self {
        Self { answers }
    }

    fn print_step_header(step: &WizardStep) {
        if let Some(title) = step.title.as_ref() {
            println!();
            println!("{title}");
        }
        println!("{}", step.message);
    }

    fn read_line(step: &WizardStep) -> Result<String, WizardError> {
        let mut value = String::new();
        io::stdout().flush().map_err(|error| WizardError::Io {
            step_id: step.id.to_owned(),
            message: error.to_string(),
        })?;
        io::stdin().read_line(&mut value).map_err(|error| WizardError::Io {
            step_id: step.id.to_owned(),
            message: error.to_string(),
        })?;
        Ok(value.trim().to_owned())
    }

    fn is_cancelled(value: &str) -> bool {
        matches!(value.trim().to_ascii_lowercase().as_str(), "cancel" | "abort" | "quit" | "q")
    }

    fn resolve_select_choice(step: &WizardStep, raw: &str) -> Result<String, WizardError> {
        if let Ok(index) = raw.parse::<usize>() {
            if let Some(choice) = step.options.get(index.saturating_sub(1)) {
                return Ok(choice.value.clone());
            }
        }
        if let Some(choice) = step.options.iter().find(|choice| {
            choice.value.eq_ignore_ascii_case(raw) || choice.label.eq_ignore_ascii_case(raw)
        }) {
            return Ok(choice.value.clone());
        }
        Err(WizardError::Validation {
            step_id: step.id.to_owned(),
            message: format!("unknown selection: {raw}"),
        })
    }
}

impl WizardBackend for InteractiveWizardBackend {
    fn execute_step(&mut self, step: &WizardStep) -> Result<WizardValue, WizardError> {
        // Prefilled answers only short-circuit input steps; notes, progress, and action
        // steps must still render so the operator sees the full narrative.
        if !matches!(step.kind, StepKind::Note | StepKind::Progress | StepKind::Action) {
            if let Some(value) = self.answers.remove(step.id) {
                return Ok(value);
            }
        }
        Self::print_step_header(step);
        match step.kind {
            StepKind::Note | StepKind::Progress => Ok(WizardValue::None),
            StepKind::Action => {
                print!("Press Enter to continue or type 'cancel' to abort: ");
                let raw = Self::read_line(step)?;
                if Self::is_cancelled(raw.as_str()) {
                    return Err(WizardError::Cancelled { step_id: step.id.to_owned() });
                }
                Ok(WizardValue::None)
            }
            StepKind::Text => {
                if !step.options.is_empty() {
                    for (index, choice) in step.options.iter().enumerate() {
                        if let Some(hint) = choice.hint.as_ref() {
                            println!("  {}. {} ({hint})", index + 1, choice.label);
                        } else {
                            println!("  {}. {}", index + 1, choice.label);
                        }
                    }
                }
                if step.sensitive {
                    let prompt = if let Some(default_value) = step.default_value.as_ref() {
                        format!("{} [{}]: ", step.id, default_value)
                    } else {
                        format!("{}: ", step.id)
                    };
                    let raw = rpassword::prompt_password(prompt).map_err(|error| {
                        WizardError::Io { step_id: step.id.to_owned(), message: error.to_string() }
                    })?;
                    if Self::is_cancelled(raw.as_str()) {
                        return Err(WizardError::Cancelled { step_id: step.id.to_owned() });
                    }
                    if raw.is_empty() {
                        return Ok(WizardValue::SensitiveText(
                            step.default_value.clone().unwrap_or_default(),
                        ));
                    }
                    return Ok(WizardValue::SensitiveText(raw));
                }
                if let Some(default_value) = step.default_value.as_ref() {
                    print!("> [{default_value}] ");
                } else if let Some(placeholder) = step.placeholder.as_ref() {
                    print!("> ({placeholder}) ");
                } else {
                    print!("> ");
                }
                let raw = Self::read_line(step)?;
                if Self::is_cancelled(raw.as_str()) {
                    return Err(WizardError::Cancelled { step_id: step.id.to_owned() });
                }
                if raw.is_empty() {
                    return Ok(WizardValue::Text(step.default_value.clone().unwrap_or_default()));
                }
                Ok(WizardValue::Text(raw))
            }
            StepKind::Confirm => {
                let default_yes = step.default_value.as_deref() == Some("true");
                let suffix = if default_yes { "[Y/n]" } else { "[y/N]" };
                print!("{suffix} ");
                let raw = Self::read_line(step)?;
                if Self::is_cancelled(raw.as_str()) {
                    return Err(WizardError::Cancelled { step_id: step.id.to_owned() });
                }
                if raw.is_empty() {
                    return Ok(WizardValue::Bool(default_yes));
                }
                match raw.to_ascii_lowercase().as_str() {
                    "y" | "yes" | "true" => Ok(WizardValue::Bool(true)),
                    "n" | "no" | "false" => Ok(WizardValue::Bool(false)),
                    _ => Err(WizardError::Validation {
                        step_id: step.id.to_owned(),
                        message: format!("expected yes/no response, received {raw}"),
                    }),
                }
            }
            StepKind::Select => {
                for (index, choice) in step.options.iter().enumerate() {
                    if let Some(hint) = choice.hint.as_ref() {
                        println!("  {}. {} ({hint})", index + 1, choice.label);
                    } else {
                        println!("  {}. {}", index + 1, choice.label);
                    }
                }
                if let Some(default_value) = step.default_value.as_ref() {
                    print!("> [{default_value}] ");
                } else {
                    print!("> ");
                }
                let raw = Self::read_line(step)?;
                if Self::is_cancelled(raw.as_str()) {
                    return Err(WizardError::Cancelled { step_id: step.id.to_owned() });
                }
                let resolved = if raw.is_empty() {
                    step.default_value.clone().unwrap_or_default()
                } else {
                    Self::resolve_select_choice(step, raw.as_str())?
                };
                Ok(WizardValue::Choice(resolved))
            }
            StepKind::MultiSelect => {
                for (index, choice) in step.options.iter().enumerate() {
                    if let Some(hint) = choice.hint.as_ref() {
                        println!("  {}. {} ({hint})", index + 1, choice.label);
                    } else {
                        println!("  {}. {}", index + 1, choice.label);
                    }
                }
                print!("> ");
                let raw = Self::read_line(step)?;
                if Self::is_cancelled(raw.as_str()) {
                    return Err(WizardError::Cancelled { step_id: step.id.to_owned() });
                }
                let tokens = if raw.is_empty() {
                    step.default_value
                        .as_deref()
                        .unwrap_or_default()
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                } else {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                };
                let mut resolved = Vec::new();
                for token in tokens {
                    resolved.push(Self::resolve_select_choice(step, token.as_str())?);
                }
                Ok(WizardValue::Multi(resolved))
            }
        }
    }

    fn on_validation_error(
        &mut self,
        _step: &WizardStep,
        message: &str,
    ) -> Result<(), WizardError> {
        println!("Validation error: {message}");
        Ok(())
    }

    fn retries_on_validation_error(&self) -> bool {
        true
    }
}

/// Prompt-free backend for `--non-interactive` runs: answers come only from provided
/// values or step defaults, and any gap fails fast instead of blocking on stdin.
pub(crate) struct NonInteractiveWizardBackend {
    answers: BTreeMap<String, WizardValue>,
}

impl NonInteractiveWizardBackend {
    pub(crate) fn new(answers: BTreeMap<String, WizardValue>) -> Self {
        Self { answers }
    }
}

impl WizardBackend for NonInteractiveWizardBackend {
    fn execute_step(&mut self, step: &WizardStep) -> Result<WizardValue, WizardError> {
        if matches!(step.kind, StepKind::Note | StepKind::Progress | StepKind::Action) {
            return Ok(WizardValue::None);
        }
        if let Some(value) = self.answers.remove(step.id) {
            return Ok(value);
        }
        if let Some(default_value) = step.default_value.as_ref() {
            return Ok(match step.kind {
                StepKind::Confirm => WizardValue::Bool(default_value == "true"),
                StepKind::MultiSelect => WizardValue::Multi(
                    default_value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect(),
                ),
                StepKind::Select => WizardValue::Choice(default_value.clone()),
                _ => WizardValue::Text(default_value.clone()),
            });
        }
        Err(WizardError::MissingInput {
            step_id: step.id.to_owned(),
            message: missing_non_interactive_input_message(step).to_owned(),
        })
    }

    fn on_validation_error(&mut self, step: &WizardStep, message: &str) -> Result<(), WizardError> {
        Err(WizardError::Validation { step_id: step.id.to_owned(), message: message.to_owned() })
    }

    fn retries_on_validation_error(&self) -> bool {
        false
    }
}

/// Maps a step id to the actionable error text shown when non-interactive input is missing.
fn missing_non_interactive_input_message(step: &WizardStep) -> &'static str {
    match step.id {
        "accept_risk_ack" => {
            "non-interactive mode requires --accept-risk to acknowledge setup and configuration mutations"
        }
        _ => "non-interactive mode requires an explicit value",
    }
}

/// Test backend that replays scripted per-step answers and records validation messages.
#[cfg(test)]
pub(crate) struct ScriptedWizardBackend {
    scripted: BTreeMap<String, VecDeque<Result<WizardValue, WizardError>>>,
    retries: bool,
    pub(crate) validation_messages: Vec<String>,
}

#[cfg(test)]
impl ScriptedWizardBackend {
    pub(crate) fn new(
        scripted: BTreeMap<String, VecDeque<Result<WizardValue, WizardError>>>,
        retries: bool,
    ) -> Self {
        Self { scripted, retries, validation_messages: Vec::new() }
    }
}

#[cfg(test)]
impl WizardBackend for ScriptedWizardBackend {
    fn execute_step(&mut self, step: &WizardStep) -> Result<WizardValue, WizardError> {
        self.scripted.get_mut(step.id).and_then(VecDeque::pop_front).unwrap_or_else(|| {
            Err(WizardError::MissingInput {
                step_id: step.id.to_owned(),
                message: "missing scripted answer".to_owned(),
            })
        })
    }

    fn on_validation_error(
        &mut self,
        _step: &WizardStep,
        message: &str,
    ) -> Result<(), WizardError> {
        self.validation_messages.push(message.to_owned());
        Ok(())
    }

    fn retries_on_validation_error(&self) -> bool {
        self.retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_step(id: &'static str) -> WizardStep {
        WizardStep {
            id,
            kind: StepKind::Text,
            title: Some("Prompt".to_owned()),
            message: "Enter a value".to_owned(),
            default_value: None,
            placeholder: None,
            sensitive: false,
            allow_empty: false,
            options: Vec::new(),
        }
    }

    #[test]
    fn scripted_backend_retries_after_validation_error() {
        let mut scripted = BTreeMap::new();
        scripted.insert(
            "workspace".to_owned(),
            VecDeque::from([
                Ok(WizardValue::Text(String::new())),
                Ok(WizardValue::Text("C:/workspace".to_owned())),
            ]),
        );
        let mut backend = ScriptedWizardBackend::new(scripted, true);
        let mut wizard = WizardSession::new(&mut backend);
        let value = wizard
            .text(text_step("workspace"), |value| {
                if value.trim().is_empty() {
                    return Err("workspace root cannot be empty".to_owned());
                }
                Ok(())
            })
            .expect("wizard should retry and succeed");
        assert_eq!(value, "C:/workspace");
        assert_eq!(backend.validation_messages, vec!["workspace root cannot be empty"]);
    }

    #[test]
    fn scripted_backend_propagates_cancel() {
        let mut scripted = BTreeMap::new();
        scripted.insert(
            "workspace".to_owned(),
            VecDeque::from([Err(WizardError::Cancelled { step_id: "workspace".to_owned() })]),
        );
        let mut backend = ScriptedWizardBackend::new(scripted, true);
        let mut wizard = WizardSession::new(&mut backend);
        let error = wizard
            .text(text_step("workspace"), |_| Ok(()))
            .expect_err("wizard should stop on cancellation");
        assert_eq!(error, WizardError::Cancelled { step_id: "workspace".to_owned() });
    }

    #[test]
    fn non_interactive_backend_requires_explicit_values_for_missing_inputs() {
        let mut backend = NonInteractiveWizardBackend::new(BTreeMap::new());
        let mut wizard = WizardSession::new(&mut backend);
        let error = wizard
            .text(text_step("workspace"), |_| Ok(()))
            .expect_err("missing non-interactive input should fail");
        assert_eq!(
            error,
            WizardError::MissingInput {
                step_id: "workspace".to_owned(),
                message: "non-interactive mode requires an explicit value".to_owned(),
            }
        );
    }

    #[test]
    fn non_interactive_backend_names_accept_risk_flag_for_missing_risk_ack() {
        let mut backend = NonInteractiveWizardBackend::new(BTreeMap::new());
        let mut wizard = WizardSession::new(&mut backend);
        let error = wizard
            .confirm(WizardStep {
                id: "accept_risk_ack",
                kind: StepKind::Confirm,
                title: Some("Risk Acknowledgement".to_owned()),
                message: "Proceed with setup?".to_owned(),
                default_value: None,
                placeholder: None,
                sensitive: false,
                allow_empty: false,
                options: Vec::new(),
            })
            .expect_err("missing risk acknowledgement should fail");
        let message = error.to_string();
        assert!(message.contains("accept_risk_ack"), "message should name the missing step");
        assert!(message.contains("--accept-risk"), "message should name the required flag");
    }

    #[test]
    fn interactive_backend_consumes_prefilled_answers_before_prompting() {
        let mut answers = BTreeMap::new();
        answers
            .insert("workspace".to_owned(), WizardValue::SensitiveText("sk-prefilled".to_owned()));
        let mut backend = InteractiveWizardBackend::with_answers(answers);
        let mut wizard = WizardSession::new(&mut backend);
        let value = wizard
            .text(text_step("workspace"), |value| {
                if value.trim().is_empty() {
                    return Err("workspace root cannot be empty".to_owned());
                }
                Ok(())
            })
            .expect("interactive backend should accept prefilled answer");
        assert_eq!(value, "sk-prefilled");
    }
}
