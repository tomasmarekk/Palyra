use serde::Serialize;

pub const MAX_MODEL_TOKENS_PER_EVENT: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunTransition {
    Accept,
    StartStreaming,
    Complete,
    Fail,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLifecycleState {
    Pending,
    Accepted,
    InProgress,
    Done,
    Failed,
    Cancelled,
}

impl RunLifecycleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::InProgress => "in_progress",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "accepted" => Some(Self::Accepted),
            "in_progress" => Some(Self::InProgress),
            "done" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RunStateMachineError {
    #[error("invalid run state transition from {from:?} via {transition:?}")]
    InvalidTransition { from: RunLifecycleState, transition: RunTransition },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunStateMachine {
    state: RunLifecycleState,
}

impl Default for RunStateMachine {
    fn default() -> Self {
        Self { state: RunLifecycleState::Pending }
    }
}

impl RunStateMachine {
    #[must_use]
    pub fn state(self) -> RunLifecycleState {
        self.state
    }

    pub fn transition(
        &mut self,
        transition: RunTransition,
    ) -> Result<RunLifecycleState, RunStateMachineError> {
        let next_state = match (self.state, transition) {
            (RunLifecycleState::Pending, RunTransition::Accept) => RunLifecycleState::Accepted,
            (RunLifecycleState::Accepted, RunTransition::StartStreaming) => {
                RunLifecycleState::InProgress
            }
            (RunLifecycleState::Accepted, RunTransition::Cancel) => RunLifecycleState::Cancelled,
            (RunLifecycleState::Accepted, RunTransition::Fail) => RunLifecycleState::Failed,
            (RunLifecycleState::InProgress, RunTransition::Complete) => RunLifecycleState::Done,
            (RunLifecycleState::InProgress, RunTransition::Fail) => RunLifecycleState::Failed,
            (RunLifecycleState::InProgress, RunTransition::Cancel) => RunLifecycleState::Cancelled,
            _ => {
                return Err(RunStateMachineError::InvalidTransition {
                    from: self.state,
                    transition,
                });
            }
        };
        self.state = next_state;
        Ok(next_state)
    }
}

#[must_use]
pub fn estimate_token_count(input: &str) -> u64 {
    input.split_whitespace().count() as u64
}

#[must_use]
#[cfg(test)]
pub fn split_model_tokens(input: &str, max_tokens: usize) -> Vec<String> {
    if max_tokens == 0 || input.trim().is_empty() {
        return Vec::new();
    }

    let mut tokens = Vec::new();
    let mut pending_whitespace = String::new();
    let mut current_token = String::new();
    let mut truncated = false;

    for ch in input.chars() {
        if ch.is_whitespace() {
            if current_token.is_empty() {
                pending_whitespace.push(ch);
            } else {
                tokens.push(std::mem::take(&mut current_token));
                pending_whitespace.push(ch);
            }
            continue;
        }

        if current_token.is_empty() {
            if tokens.len() >= max_tokens {
                truncated = true;
                break;
            }
            current_token.push_str(pending_whitespace.as_str());
            pending_whitespace.clear();
        }
        current_token.push(ch);
    }

    if !current_token.is_empty() && tokens.len() < max_tokens {
        tokens.push(current_token);
    } else if !pending_whitespace.is_empty() && !tokens.is_empty() && !truncated {
        if let Some(last) = tokens.last_mut() {
            last.push_str(pending_whitespace.as_str());
        }
    }

    tokens
}

#[must_use]
pub fn is_cancel_command(input: &str) -> bool {
    let trimmed = input.trim();
    trimmed.eq_ignore_ascii_case("/cancel") || trimmed.eq_ignore_ascii_case("cancel")
}

#[cfg(test)]
mod tests {
    use super::{
        estimate_token_count, is_cancel_command, split_model_tokens, RunLifecycleState,
        RunStateMachine, RunTransition,
    };

    #[test]
    fn run_state_machine_accepts_happy_path() {
        let mut machine = RunStateMachine::default();
        assert_eq!(
            machine.transition(RunTransition::Accept).expect("accept transition should succeed"),
            RunLifecycleState::Accepted
        );
        assert_eq!(
            machine
                .transition(RunTransition::StartStreaming)
                .expect("streaming transition should succeed"),
            RunLifecycleState::InProgress
        );
        assert_eq!(
            machine
                .transition(RunTransition::Complete)
                .expect("complete transition should succeed"),
            RunLifecycleState::Done
        );
    }

    #[test]
    fn run_state_machine_rejects_invalid_transition() {
        let mut machine = RunStateMachine::default();
        let error = machine
            .transition(RunTransition::Complete)
            .expect_err("completing directly from pending must be rejected");
        assert!(
            error.to_string().contains("invalid run state transition from Pending via Complete"),
            "error should include explicit transition context"
        );
    }

    #[test]
    fn token_helpers_are_deterministic_and_bounded() {
        let input = "alpha beta gamma delta";
        assert_eq!(estimate_token_count(input), 4);
        let tokens = split_model_tokens(input, 2);
        assert_eq!(tokens, vec!["alpha".to_owned(), " beta".to_owned()]);
        assert_eq!(tokens.concat(), "alpha beta");
    }

    #[test]
    fn split_model_tokens_preserves_stream_spacing() {
        let input = "Hello! How can I help today?\nNext line.";
        let tokens = split_model_tokens(input, 16);
        assert_eq!(tokens.concat(), input);
        assert_eq!(
            tokens,
            vec![
                "Hello!".to_owned(),
                " How".to_owned(),
                " can".to_owned(),
                " I".to_owned(),
                " help".to_owned(),
                " today?".to_owned(),
                "\nNext".to_owned(),
                " line.".to_owned(),
            ]
        );
    }

    #[test]
    fn cancel_command_accepts_supported_variants() {
        assert!(is_cancel_command("/cancel"));
        assert!(is_cancel_command("cancel"));
        assert!(is_cancel_command("  Cancel  "));
        assert!(!is_cancel_command("cancelled"));
    }
}
