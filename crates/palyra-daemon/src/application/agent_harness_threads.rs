//! External thread binding and lease contracts for harness-owned runtimes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Component that owns compaction for an external harness thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionOwner {
    Palyra,
    Harness,
    ExternalRuntime,
    Disabled,
}

/// Conflict state for a mirrored external thread binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalThreadConflictState {
    None,
    LeaseExpired,
    ConcurrentResume,
    MirrorDiverged,
    Archived,
}

/// Redacted binding between a Palyra session and a harness-owned external thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalThreadBinding {
    pub palyra_session_id: String,
    pub harness_id: String,
    pub external_thread_id: String,
    pub runtime_kind: String,
    pub owner_plugin_id: Option<String>,
    pub lease_token: String,
    pub lease_expires_at_ms: i64,
    pub compaction_owner: CompactionOwner,
    pub native_revision: u64,
    pub mirror_revision: u64,
    pub conflict_state: ExternalThreadConflictState,
    pub mirror_transcript_redacted: Vec<String>,
}

impl ExternalThreadBinding {
    /// Returns true when the lease is valid at `now_ms`.
    #[must_use]
    pub fn lease_active_at(&self, now_ms: i64) -> bool {
        self.conflict_state != ExternalThreadConflictState::Archived
            && self.lease_expires_at_ms > now_ms
    }
}

/// Binding repository failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExternalThreadBindingError {
    #[error("binding already exists for session {session_id}")]
    AlreadyBound { session_id: String },
    #[error("binding is missing for session {session_id}")]
    MissingBinding { session_id: String },
    #[error("lease token does not match for session {session_id}")]
    LeaseTokenMismatch { session_id: String },
    #[error("lease expired for session {session_id}")]
    LeaseExpired { session_id: String },
}

/// In-memory deterministic binding store used by runtime tests and future adapters.
#[derive(Debug, Default, Clone)]
pub struct ExternalThreadBindingStore {
    bindings: BTreeMap<String, ExternalThreadBinding>,
}

impl ExternalThreadBindingStore {
    /// Inserts a new binding.
    ///
    /// # Errors
    /// Returns [`ExternalThreadBindingError::AlreadyBound`] when the session already has a binding.
    pub fn bind(
        &mut self,
        binding: ExternalThreadBinding,
    ) -> Result<(), ExternalThreadBindingError> {
        if self.bindings.contains_key(binding.palyra_session_id.as_str()) {
            return Err(ExternalThreadBindingError::AlreadyBound {
                session_id: binding.palyra_session_id,
            });
        }
        self.bindings.insert(binding.palyra_session_id.clone(), binding);
        Ok(())
    }

    /// Returns a binding by Palyra session id.
    #[must_use]
    pub fn get(&self, session_id: &str) -> Option<&ExternalThreadBinding> {
        self.bindings.get(session_id)
    }

    /// Renews a binding lease.
    ///
    /// # Errors
    /// Returns typed failures for missing, mismatched, or expired leases.
    pub fn renew_lease(
        &mut self,
        session_id: &str,
        lease_token: &str,
        now_ms: i64,
        new_expires_at_ms: i64,
    ) -> Result<(), ExternalThreadBindingError> {
        let binding = self.bindings.get_mut(session_id).ok_or_else(|| {
            ExternalThreadBindingError::MissingBinding { session_id: session_id.to_owned() }
        })?;
        ensure_lease(binding, lease_token, now_ms)?;
        binding.lease_expires_at_ms = new_expires_at_ms;
        Ok(())
    }

    /// Releases and removes a binding lease.
    ///
    /// # Errors
    /// Returns typed failures for missing, mismatched, or expired leases.
    pub fn release_lease(
        &mut self,
        session_id: &str,
        lease_token: &str,
        now_ms: i64,
    ) -> Result<ExternalThreadBinding, ExternalThreadBindingError> {
        {
            let binding = self.bindings.get(session_id).ok_or_else(|| {
                ExternalThreadBindingError::MissingBinding { session_id: session_id.to_owned() }
            })?;
            ensure_lease(binding, lease_token, now_ms)?;
        }
        self.bindings.remove(session_id).ok_or_else(|| ExternalThreadBindingError::MissingBinding {
            session_id: session_id.to_owned(),
        })
    }

    /// Marks an existing binding as conflicted.
    ///
    /// # Errors
    /// Returns [`ExternalThreadBindingError::MissingBinding`] when the session is not bound.
    pub fn mark_conflict(
        &mut self,
        session_id: &str,
        conflict_state: ExternalThreadConflictState,
    ) -> Result<(), ExternalThreadBindingError> {
        let binding = self.bindings.get_mut(session_id).ok_or_else(|| {
            ExternalThreadBindingError::MissingBinding { session_id: session_id.to_owned() }
        })?;
        binding.conflict_state = conflict_state;
        Ok(())
    }

    /// Forks a binding into a new Palyra session with a fresh lease.
    ///
    /// # Errors
    /// Returns [`ExternalThreadBindingError::MissingBinding`] when the source session is not bound.
    pub fn fork_binding(
        &mut self,
        source_session_id: &str,
        new_session_id: impl Into<String>,
        new_lease_token: impl Into<String>,
        new_expires_at_ms: i64,
    ) -> Result<ExternalThreadBinding, ExternalThreadBindingError> {
        let source = self.bindings.get(source_session_id).cloned().ok_or_else(|| {
            ExternalThreadBindingError::MissingBinding { session_id: source_session_id.to_owned() }
        })?;
        let mut forked = source;
        forked.palyra_session_id = new_session_id.into();
        forked.lease_token = new_lease_token.into();
        forked.lease_expires_at_ms = new_expires_at_ms;
        forked.conflict_state = ExternalThreadConflictState::None;
        self.bind(forked.clone())?;
        Ok(forked)
    }

    /// Archives an existing binding in place.
    ///
    /// # Errors
    /// Returns [`ExternalThreadBindingError::MissingBinding`] when the session is not bound.
    pub fn archive_binding(&mut self, session_id: &str) -> Result<(), ExternalThreadBindingError> {
        self.mark_conflict(session_id, ExternalThreadConflictState::Archived)
    }
}

fn ensure_lease(
    binding: &ExternalThreadBinding,
    lease_token: &str,
    now_ms: i64,
) -> Result<(), ExternalThreadBindingError> {
    if binding.lease_token != lease_token {
        return Err(ExternalThreadBindingError::LeaseTokenMismatch {
            session_id: binding.palyra_session_id.clone(),
        });
    }
    if !binding.lease_active_at(now_ms) {
        return Err(ExternalThreadBindingError::LeaseExpired {
            session_id: binding.palyra_session_id.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> ExternalThreadBinding {
        ExternalThreadBinding {
            palyra_session_id: "session-1".to_owned(),
            harness_id: "embedded_palyra".to_owned(),
            external_thread_id: "thread-1".to_owned(),
            runtime_kind: "embedded".to_owned(),
            owner_plugin_id: None,
            lease_token: "lease-1".to_owned(),
            lease_expires_at_ms: 2_000,
            compaction_owner: CompactionOwner::Palyra,
            native_revision: 1,
            mirror_revision: 1,
            conflict_state: ExternalThreadConflictState::None,
            mirror_transcript_redacted: vec!["user:<redacted>".to_owned()],
        }
    }

    #[test]
    fn lease_acquire_renew_and_release_are_explicit() {
        let mut store = ExternalThreadBindingStore::default();
        store.bind(binding()).expect("binding should insert");

        store.renew_lease("session-1", "lease-1", 1_000, 3_000).expect("lease should renew");
        assert_eq!(store.get("session-1").unwrap().lease_expires_at_ms, 3_000);

        let released =
            store.release_lease("session-1", "lease-1", 2_000).expect("lease should release");
        assert_eq!(released.external_thread_id, "thread-1");
        assert!(store.get("session-1").is_none());
    }

    #[test]
    fn concurrent_resume_without_lease_marks_conflict_path() {
        let mut store = ExternalThreadBindingStore::default();
        store.bind(binding()).expect("binding should insert");

        let error = store
            .renew_lease("session-1", "wrong-lease", 1_000, 3_000)
            .expect_err("wrong lease must fail");
        store
            .mark_conflict("session-1", ExternalThreadConflictState::ConcurrentResume)
            .expect("conflict should mark");

        assert_eq!(
            error,
            ExternalThreadBindingError::LeaseTokenMismatch { session_id: "session-1".to_owned() }
        );
        assert_eq!(
            store.get("session-1").unwrap().conflict_state,
            ExternalThreadConflictState::ConcurrentResume
        );
    }

    #[test]
    fn fork_binding_keeps_mirror_separate_from_canonical_thread() {
        let mut store = ExternalThreadBindingStore::default();
        store.bind(binding()).expect("binding should insert");

        let forked = store
            .fork_binding("session-1", "session-2", "lease-2", 4_000)
            .expect("binding should fork");

        assert_eq!(forked.palyra_session_id, "session-2");
        assert_eq!(forked.external_thread_id, "thread-1");
        assert_eq!(forked.mirror_transcript_redacted, vec!["user:<redacted>"]);
        assert_eq!(forked.compaction_owner, CompactionOwner::Palyra);
    }
}
