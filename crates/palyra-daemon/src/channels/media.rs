//! Media-store pass-throughs on [`ChannelPlatform`]: console chat
//! attachments and the derived-artifact lifecycle (upsert, listing,
//! linking, quarantine, purge, stats). Each method delegates to
//! [`MediaArtifactStore`], converting errors to [`ChannelPlatformError`].

use super::*;

impl ChannelPlatform {
    /// Returns the global media diagnostics snapshot as JSON.
    ///
    /// # Errors
    /// Returns media-store errors and
    /// [`ChannelPlatformError::InvalidInput`] when serialization fails.
    pub fn media_snapshot(&self) -> Result<Value, ChannelPlatformError> {
        serde_json::to_value(self.media_store.build_global_snapshot()?).map_err(|error| {
            ChannelPlatformError::InvalidInput(format!(
                "failed to serialize media diagnostics snapshot: {error}"
            ))
        })
    }

    /// Stores a console chat upload under a fresh attachment id, scoped to
    /// the session/principal/device identity in the request.
    ///
    /// # Errors
    /// Propagates media-store policy and IO errors.
    pub fn store_console_chat_attachment(
        &self,
        request: ConsoleChatAttachmentStoreRequestView<'_>,
    ) -> Result<MediaArtifactPayload, ChannelPlatformError> {
        let attachment_id = Ulid::new().to_string();
        self.media_store
            .store_console_attachment(ConsoleAttachmentStoreRequest {
                connector_id: "console_chat",
                session_id: request.session_id,
                principal: request.principal,
                device_id: request.device_id,
                channel: request.channel,
                attachment_id: attachment_id.as_str(),
                filename: request.filename,
                declared_content_type: request.declared_content_type,
                bytes: request.bytes,
            })
            .map_err(ChannelPlatformError::from)
    }

    /// Loads one console chat attachment; the identity arguments must
    /// match the stored scope or the lookup returns `None`.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn load_console_chat_attachment(
        &self,
        artifact_id: &str,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Option<MediaArtifactPayload>, ChannelPlatformError> {
        self.media_store
            .load_console_attachment(artifact_id, session_id, principal, device_id, channel)
            .map_err(ChannelPlatformError::from)
    }

    /// Lists the console chat attachments visible to one
    /// session/principal/device scope.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn list_console_chat_attachments(
        &self,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<MediaArtifactPayload>, ChannelPlatformError> {
        self.media_store
            .list_console_attachment_payloads(session_id, principal, device_id, channel)
            .map_err(ChannelPlatformError::from)
    }

    /// Records (or refreshes) a successful derived artifact for a source
    /// attachment.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn upsert_console_chat_derived_artifact(
        &self,
        request: MediaDerivedArtifactUpsertRequest<'_>,
    ) -> Result<MediaDerivedArtifactRecord, ChannelPlatformError> {
        self.media_store.upsert_derived_artifact(request).map_err(ChannelPlatformError::from)
    }

    /// Records a failed derivation attempt so the failure is visible and
    /// retryable.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn upsert_console_chat_failed_derived_artifact(
        &self,
        request: MediaFailedDerivedArtifactUpsertRequest<'_>,
    ) -> Result<MediaDerivedArtifactRecord, ChannelPlatformError> {
        self.media_store.upsert_failed_derived_artifact(request).map_err(ChannelPlatformError::from)
    }

    /// Lists derived artifacts for one session scope.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn list_console_chat_derived_artifacts(
        &self,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .list_session_derived_artifacts(session_id, principal, device_id, channel)
            .map_err(ChannelPlatformError::from)
    }

    /// Lists derived artifacts produced from one source attachment.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn list_attachment_derived_artifacts(
        &self,
        source_artifact_id: &str,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .list_attachment_derived_artifacts(source_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    /// Returns one derived artifact by id, `None` when unknown.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn get_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .get_derived_artifact(derived_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    /// Lists derived artifacts linked to a workspace document and/or
    /// memory item.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn list_linked_derived_artifacts(
        &self,
        workspace_document_id: Option<&str>,
        memory_item_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .list_linked_derived_artifacts(workspace_document_id, memory_item_id, limit)
            .map_err(ChannelPlatformError::from)
    }

    /// Links a derived artifact to a workspace document and/or memory
    /// item.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn link_derived_artifact_targets(
        &self,
        derived_artifact_id: &str,
        workspace_document_id: Option<&str>,
        memory_item_id: Option<&str>,
    ) -> Result<(), ChannelPlatformError> {
        self.media_store
            .link_derived_artifact_targets(
                derived_artifact_id,
                workspace_document_id,
                memory_item_id,
            )
            .map_err(ChannelPlatformError::from)
    }

    /// Selects derived text chunks relevant to `query` for prompt
    /// assembly, within an optional character budget.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn select_console_chat_derived_chunks(
        &self,
        source_artifact_ids: &[String],
        query: &str,
        selection_budget_chars: Option<usize>,
    ) -> Result<Vec<MediaDerivedArtifactSelection>, ChannelPlatformError> {
        self.media_store
            .select_derived_prompt_chunks(source_artifact_ids, query, selection_budget_chars)
            .map_err(ChannelPlatformError::from)
    }

    /// Quarantines a derived artifact so it is excluded from prompt
    /// selection; `None` when unknown.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn quarantine_derived_artifact(
        &self,
        derived_artifact_id: &str,
        reason: Option<&str>,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .quarantine_derived_artifact(derived_artifact_id, reason)
            .map_err(ChannelPlatformError::from)
    }

    /// Releases a quarantined derived artifact back into use; `None` when
    /// unknown.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn release_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .release_derived_artifact(derived_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    /// Flags (or clears) a derived artifact as needing recomputation;
    /// `None` when unknown.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn mark_derived_artifact_recompute_required(
        &self,
        derived_artifact_id: &str,
        required: bool,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .mark_derived_artifact_recompute_required(derived_artifact_id, required)
            .map_err(ChannelPlatformError::from)
    }

    /// Permanently deletes a derived artifact and its stored content;
    /// `None` when unknown.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn purge_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .purge_derived_artifact(derived_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    /// Returns aggregate derived-artifact statistics.
    ///
    /// # Errors
    /// Propagates media-store errors.
    pub fn derived_stats(&self) -> Result<MediaDerivedStatsSnapshot, ChannelPlatformError> {
        self.media_store.derived_stats().map_err(ChannelPlatformError::from)
    }
}
