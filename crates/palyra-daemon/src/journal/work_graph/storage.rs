//! Transactional creation, projection, and state transitions for durable work graphs.

use crate::domain::work_graph::{
    reason, validate_graph_create_request, validate_loaded_graph, validate_transition,
    WorkGraphCreateRequest, WorkGraphRecordV1, WorkGraphState, WorkGraphValidationError,
    WorkItemRecordV1, WorkItemState, WorkItemTransitionOutcome, WorkItemTransitionRequest,
    WorkResourceClass, WorkVerificationState, WORK_GRAPH_SCHEMA_VERSION,
};

use super::*;

/// Complete durable projection returned to host coordinators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkGraphSnapshotV1 {
    pub(crate) graph: WorkGraphRecordV1,
    pub(crate) items: Vec<WorkItemRecordV1>,
}

/// One append-only work graph lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkGraphEventRecordV1 {
    pub(crate) sequence: u64,
    pub(crate) event_id: String,
    pub(crate) graph_id: String,
    pub(crate) work_item_id: Option<String>,
    pub(crate) graph_revision: u64,
    pub(crate) item_revision: Option<u64>,
    pub(crate) event_type: String,
    pub(crate) actor_principal: String,
    pub(crate) from_state: Option<String>,
    pub(crate) to_state: Option<String>,
    pub(crate) reason_code: String,
    pub(crate) payload_json: String,
    pub(crate) created_at_unix_ms: i64,
}

impl JournalStore {
    /// Atomically validates and creates an entire work graph.
    pub(crate) fn create_work_graph(
        &self,
        request: &WorkGraphCreateRequest,
    ) -> Result<WorkGraphSnapshotV1, JournalError> {
        validate_graph_create_request(request).map_err(validation_error)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let budget_json = serde_json::to_string(&request.budget)?;
        match transaction.execute(
            r#"
                INSERT INTO work_graphs (
                    graph_ulid, schema_version, owner_principal, device_id, channel,
                    session_ulid, origin_run_ulid, objective_id, routine_id, flow_ulid,
                    flow_step_id, state, budget_json, revision, reason_code,
                    created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 1, ?14, ?15, ?15
                )
            "#,
            params![
                request.graph_id,
                i64::from(WORK_GRAPH_SCHEMA_VERSION),
                request.owner.principal,
                request.owner.device_id,
                request.owner.channel,
                request.owner.session_id,
                request.owner.origin_run_id,
                request.objective_id,
                request.routine_id,
                request.flow_id,
                request.flow_step_id,
                WorkGraphState::Active.as_str(),
                budget_json,
                reason::CREATED,
                now,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == ErrorCode::ConstraintViolation =>
            {
                return Err(JournalError::DuplicateWorkGraphId {
                    graph_id: request.graph_id.clone(),
                });
            }
            Err(error) => return Err(error.into()),
        }

        for item in &request.items {
            let initial_state =
                if item.compensates_work_item_id.is_some() || !item.dependency_ids.is_empty() {
                    WorkItemState::BlockedByDependencies
                } else {
                    WorkItemState::Ready
                };
            let initial_reason = if initial_state == WorkItemState::Ready {
                reason::READY
            } else {
                reason::DEPENDENCY_BLOCKED
            };
            transaction.execute(
                r#"
                    INSERT INTO work_graph_items (
                        graph_ulid, work_item_ulid, schema_version, title, description, state,
                        priority, capability_profile, dependencies_json,
                        compensates_work_item_ulid, serialization_key, resource_class,
                        provider_profile, workspace_scope, budget_json, max_runtime_ms,
                        requires_review, verification_state, revision, reason_code,
                        evidence_refs_json, artifact_refs_json, created_at_unix_ms,
                        updated_at_unix_ms
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                        ?15, ?16, ?17, ?18, 1, ?19, '[]', '[]', ?20, ?20
                    )
                "#,
                params![
                    request.graph_id,
                    item.work_item_id,
                    i64::from(WORK_GRAPH_SCHEMA_VERSION),
                    item.title,
                    item.description,
                    initial_state.as_str(),
                    i64::from(item.priority),
                    item.capability_profile,
                    serde_json::to_string(&item.dependency_ids)?,
                    item.compensates_work_item_id,
                    item.serialization_key,
                    item.resource_class.as_str(),
                    item.provider_profile,
                    item.workspace_scope,
                    serde_json::to_string(&item.budget)?,
                    u64_to_sqlite(item.max_runtime_ms, "max_runtime_ms")?,
                    bool_to_sqlite(item.requires_review),
                    WorkVerificationState::Unverified.as_str(),
                    initial_reason,
                    now,
                ],
            )?;
            insert_event(
                &transaction,
                EventInsert {
                    graph_id: request.graph_id.as_str(),
                    work_item_id: Some(item.work_item_id.as_str()),
                    graph_revision: 1,
                    item_revision: Some(1),
                    event_type: "work_graph.item.created",
                    actor_principal: request.actor_principal.as_str(),
                    from_state: None,
                    to_state: Some(initial_state.as_str()),
                    reason_code: initial_reason,
                    payload_json: "{}",
                    created_at_unix_ms: now,
                },
            )?;
        }
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.graph_id.as_str(),
                work_item_id: None,
                graph_revision: 1,
                item_revision: None,
                event_type: "work_graph.created",
                actor_principal: request.actor_principal.as_str(),
                from_state: None,
                to_state: Some(WorkGraphState::Active.as_str()),
                reason_code: reason::CREATED,
                payload_json: "{}",
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        drop(guard);
        self.work_graph_snapshot(request.graph_id.as_str())?
            .ok_or_else(|| JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() })
    }

    /// Loads and fail-closed revalidates a durable graph projection.
    pub(crate) fn work_graph_snapshot(
        &self,
        graph_id: &str,
    ) -> Result<Option<WorkGraphSnapshotV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(snapshot) = query_snapshot(&guard, graph_id)? else {
            return Ok(None);
        };
        validate_loaded_graph(&snapshot.graph, snapshot.items.as_slice())
            .map_err(validation_error)?;
        Ok(Some(snapshot))
    }

    /// Applies one expected-revision transition and projects dependency effects atomically.
    pub(crate) fn transition_work_graph_item(
        &self,
        request: &WorkItemTransitionRequest,
    ) -> Result<WorkItemTransitionOutcome, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let snapshot =
            query_snapshot(&transaction, request.graph_id.as_str())?.ok_or_else(|| {
                JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
            })?;
        validate_loaded_graph(&snapshot.graph, snapshot.items.as_slice())
            .map_err(validation_error)?;
        if snapshot.graph.state != WorkGraphState::Active {
            return Err(JournalError::InvalidWorkGraph {
                reason_code: reason::INVALID_TRANSITION.to_owned(),
                message: format!("graph {} is not active", request.graph_id),
            });
        }
        let current = snapshot
            .items
            .iter()
            .find(|item| item.work_item_id == request.work_item_id)
            .ok_or_else(|| JournalError::WorkGraphItemNotFound {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
            })?;
        if current.revision != request.expected_revision {
            return Err(JournalError::WorkGraphRevisionConflict {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
                expected_revision: request.expected_revision,
                actual_revision: current.revision,
            });
        }
        let verification = request.verification_state.unwrap_or(current.verification_state);
        validate_transition(current.state, request.target_state, verification)
            .map_err(validation_error)?;
        ensure_dependencies_allow_target(current, snapshot.items.as_slice(), request.target_state)?;

        let next_item_revision = current.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let completed_at = request.target_state.is_terminal().then_some(now);
        let changed = transaction.execute(
            r#"
                UPDATE work_graph_items
                SET state = ?3,
                    verification_state = ?4,
                    revision = ?5,
                    reason_code = ?6,
                    updated_at_unix_ms = ?7,
                    completed_at_unix_ms = ?8
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND revision = ?9
            "#,
            params![
                request.graph_id,
                request.work_item_id,
                request.target_state.as_str(),
                verification.as_str(),
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                request.reason_code,
                now,
                completed_at,
                u64_to_sqlite(request.expected_revision, "expected_revision")?,
            ],
        )?;
        if changed != 1 {
            return Err(JournalError::WorkGraphRevisionConflict {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
                expected_revision: request.expected_revision,
                actual_revision: current.revision,
            });
        }
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.graph_id.as_str(),
                work_item_id: Some(request.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.transitioned",
                actor_principal: request.actor_principal.as_str(),
                from_state: Some(current.state.as_str()),
                to_state: Some(request.target_state.as_str()),
                reason_code: request.reason_code.as_str(),
                payload_json: "{}",
                created_at_unix_ms: now,
            },
        )?;

        let dependency_states_changed = project_dependency_states(
            &transaction,
            request.graph_id.as_str(),
            next_graph_revision,
            request.actor_principal.as_str(),
            now,
        )?;
        let graph_state = project_graph_state(&transaction, request.graph_id.as_str())?;
        transaction.execute(
            r#"
                UPDATE work_graphs
                SET state = ?2,
                    revision = ?3,
                    reason_code = ?4,
                    updated_at_unix_ms = ?5,
                    completed_at_unix_ms = CASE WHEN ?6 = 1 THEN ?5 ELSE NULL END
                WHERE graph_ulid = ?1
            "#,
            params![
                request.graph_id,
                graph_state.as_str(),
                u64_to_sqlite(next_graph_revision, "graph_revision")?,
                request.reason_code,
                now,
                bool_to_sqlite(graph_state.is_terminal()),
            ],
        )?;
        transaction.commit()?;
        drop(guard);

        let snapshot = self.work_graph_snapshot(request.graph_id.as_str())?.ok_or_else(|| {
            JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
        })?;
        let item = snapshot
            .items
            .into_iter()
            .find(|item| item.work_item_id == request.work_item_id)
            .ok_or_else(|| JournalError::WorkGraphItemNotFound {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
            })?;
        Ok(WorkItemTransitionOutcome {
            item,
            graph_revision: snapshot.graph.revision,
            dependency_states_changed,
        })
    }

    /// Lists bounded append-only graph evidence in sequence order.
    pub(crate) fn work_graph_events(
        &self,
        graph_id: &str,
        limit: usize,
    ) -> Result<Vec<WorkGraphEventRecordV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT seq, event_ulid, graph_ulid, work_item_ulid, graph_revision,
                       item_revision, event_type, actor_principal, from_state, to_state,
                       reason_code, payload_json, created_at_unix_ms
                FROM work_graph_events
                WHERE graph_ulid = ?1
                ORDER BY seq ASC
                LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(
            params![graph_id, i64::try_from(limit.clamp(1, 10_000)).unwrap_or(10_000)],
            |row| {
                Ok(WorkGraphEventRecordV1 {
                    sequence: row.get::<_, i64>(0)?.max(0) as u64,
                    event_id: row.get(1)?,
                    graph_id: row.get(2)?,
                    work_item_id: row.get(3)?,
                    graph_revision: row.get::<_, i64>(4)?.max(0) as u64,
                    item_revision: row.get::<_, Option<i64>>(5)?.map(|value| value.max(0) as u64),
                    event_type: row.get(6)?,
                    actor_principal: row.get(7)?,
                    from_state: row.get(8)?,
                    to_state: row.get(9)?,
                    reason_code: row.get(10)?,
                    payload_json: row.get(11)?,
                    created_at_unix_ms: row.get(12)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

fn validation_error(error: WorkGraphValidationError) -> JournalError {
    JournalError::InvalidWorkGraph {
        reason_code: error.reason_code.to_owned(),
        message: error.message,
    }
}

fn ensure_dependencies_allow_target(
    item: &WorkItemRecordV1,
    items: &[WorkItemRecordV1],
    target: WorkItemState,
) -> Result<(), JournalError> {
    if !matches!(target, WorkItemState::Claimed | WorkItemState::Running) {
        return Ok(());
    }
    let states = items
        .iter()
        .map(|candidate| (candidate.work_item_id.as_str(), candidate.state))
        .collect::<BTreeMap<_, _>>();
    if !dependencies_satisfied(item, &states) {
        return Err(JournalError::InvalidWorkGraph {
            reason_code: reason::DEPENDENCY_BLOCKED.to_owned(),
            message: format!("dependencies are not satisfied for {}", item.work_item_id),
        });
    }
    Ok(())
}

fn dependencies_satisfied(item: &WorkItemRecordV1, states: &BTreeMap<&str, WorkItemState>) -> bool {
    let ordinary_ready = item
        .dependency_ids
        .iter()
        .all(|id| states.get(id.as_str()) == Some(&WorkItemState::Succeeded));
    let compensation_ready = item.compensates_work_item_id.as_deref().is_none_or(|id| {
        matches!(states.get(id), Some(WorkItemState::Failed | WorkItemState::Cancelled))
    });
    ordinary_ready && compensation_ready
}

fn dependency_failed(item: &WorkItemRecordV1, states: &BTreeMap<&str, WorkItemState>) -> bool {
    item.dependency_ids.iter().any(|id| {
        matches!(
            states.get(id.as_str()),
            Some(WorkItemState::Failed | WorkItemState::Cancelled | WorkItemState::Archived)
        )
    })
}

fn project_dependency_states(
    transaction: &Transaction<'_>,
    graph_id: &str,
    graph_revision: u64,
    actor_principal: &str,
    now: i64,
) -> Result<Vec<String>, JournalError> {
    let mut changed_ids = Vec::new();
    loop {
        let snapshot = query_snapshot(transaction, graph_id)?
            .ok_or_else(|| JournalError::WorkGraphNotFound { graph_id: graph_id.to_owned() })?;
        let states = snapshot
            .items
            .iter()
            .map(|item| (item.work_item_id.as_str(), item.state))
            .collect::<BTreeMap<_, _>>();
        let mut pass_changed = false;
        for item in
            snapshot.items.iter().filter(|item| item.state == WorkItemState::BlockedByDependencies)
        {
            let (target, reason_code) = if dependencies_satisfied(item, &states) {
                (WorkItemState::Ready, reason::READY)
            } else if dependency_failed(item, &states) {
                (WorkItemState::Failed, reason::DEPENDENCY_FAILED)
            } else {
                continue;
            };
            let next_revision = item.revision.saturating_add(1);
            transaction.execute(
                r#"
                    UPDATE work_graph_items
                    SET state = ?3, revision = ?4, reason_code = ?5,
                        updated_at_unix_ms = ?6,
                        completed_at_unix_ms = CASE WHEN ?7 = 1 THEN ?6 ELSE NULL END
                    WHERE graph_ulid = ?1 AND work_item_ulid = ?2 AND revision = ?8
                "#,
                params![
                    graph_id,
                    item.work_item_id,
                    target.as_str(),
                    u64_to_sqlite(next_revision, "work_item_revision")?,
                    reason_code,
                    now,
                    bool_to_sqlite(target.is_terminal()),
                    u64_to_sqlite(item.revision, "work_item_revision")?,
                ],
            )?;
            insert_event(
                transaction,
                EventInsert {
                    graph_id,
                    work_item_id: Some(item.work_item_id.as_str()),
                    graph_revision,
                    item_revision: Some(next_revision),
                    event_type: "work_graph.item.dependency_projected",
                    actor_principal,
                    from_state: Some(item.state.as_str()),
                    to_state: Some(target.as_str()),
                    reason_code,
                    payload_json: "{}",
                    created_at_unix_ms: now,
                },
            )?;
            changed_ids.push(item.work_item_id.clone());
            pass_changed = true;
        }
        if !pass_changed {
            break;
        }
    }
    Ok(changed_ids)
}

fn project_graph_state(
    connection: &Connection,
    graph_id: &str,
) -> Result<WorkGraphState, JournalError> {
    let snapshot = query_snapshot(connection, graph_id)?
        .ok_or_else(|| JournalError::WorkGraphNotFound { graph_id: graph_id.to_owned() })?;
    let failed_ids = snapshot
        .items
        .iter()
        .filter(|item| item.state == WorkItemState::Failed)
        .map(|item| item.work_item_id.as_str())
        .collect::<BTreeSet<_>>();
    let compensation_in_progress = snapshot.items.iter().any(|item| {
        item.compensates_work_item_id.as_deref().is_some_and(|id| failed_ids.contains(id))
            && !item.state.is_terminal()
    });
    // A failed operation cannot become successful, but its compensation must retain execution
    // authority until it settles so cleanup is not stranded behind a terminal graph header.
    if !failed_ids.is_empty() && !compensation_in_progress {
        return Ok(WorkGraphState::Failed);
    }
    if snapshot
        .items
        .iter()
        .all(|item| matches!(item.state, WorkItemState::Succeeded | WorkItemState::Archived))
    {
        return Ok(WorkGraphState::Succeeded);
    }
    if snapshot.items.iter().all(|item| item.state.is_terminal())
        && snapshot.items.iter().any(|item| item.state == WorkItemState::Cancelled)
    {
        return Ok(WorkGraphState::Cancelled);
    }
    Ok(WorkGraphState::Active)
}

fn query_snapshot(
    connection: &Connection,
    graph_id: &str,
) -> Result<Option<WorkGraphSnapshotV1>, JournalError> {
    let raw = connection
        .query_row(
            r#"
                SELECT schema_version, graph_ulid, owner_principal, device_id, channel,
                       session_ulid, origin_run_ulid, objective_id, routine_id, flow_ulid,
                       flow_step_id, state, budget_json, revision, reason_code,
                       created_at_unix_ms, updated_at_unix_ms, completed_at_unix_ms
                FROM work_graphs
                WHERE graph_ulid = ?1
            "#,
            params![graph_id],
            |row| {
                Ok(RawGraphRow {
                    schema_version: row.get(0)?,
                    graph_id: row.get(1)?,
                    owner_principal: row.get(2)?,
                    device_id: row.get(3)?,
                    channel: row.get(4)?,
                    session_id: row.get(5)?,
                    origin_run_id: row.get(6)?,
                    objective_id: row.get(7)?,
                    routine_id: row.get(8)?,
                    flow_id: row.get(9)?,
                    flow_step_id: row.get(10)?,
                    state: row.get(11)?,
                    budget_json: row.get(12)?,
                    revision: row.get(13)?,
                    reason_code: row.get(14)?,
                    created_at_unix_ms: row.get(15)?,
                    updated_at_unix_ms: row.get(16)?,
                    completed_at_unix_ms: row.get(17)?,
                })
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let graph = raw.into_record()?;
    let mut statement = connection.prepare(
        r#"
            SELECT schema_version, graph_ulid, work_item_ulid, title, description, state,
                   priority, capability_profile, dependencies_json,
                   compensates_work_item_ulid, serialization_key, resource_class,
                   provider_profile, workspace_scope, budget_json, max_runtime_ms,
                   requires_review, verification_state, revision, reason_code,
                   evidence_refs_json, artifact_refs_json, created_at_unix_ms,
                   updated_at_unix_ms, completed_at_unix_ms
            FROM work_graph_items
            WHERE graph_ulid = ?1
            ORDER BY priority DESC, created_at_unix_ms ASC, work_item_ulid ASC
        "#,
    )?;
    let raw_items = statement
        .query_map(params![graph_id], |row| {
            Ok(RawItemRow {
                schema_version: row.get(0)?,
                graph_id: row.get(1)?,
                work_item_id: row.get(2)?,
                title: row.get(3)?,
                description: row.get(4)?,
                state: row.get(5)?,
                priority: row.get(6)?,
                capability_profile: row.get(7)?,
                dependencies_json: row.get(8)?,
                compensates_work_item_id: row.get(9)?,
                serialization_key: row.get(10)?,
                resource_class: row.get(11)?,
                provider_profile: row.get(12)?,
                workspace_scope: row.get(13)?,
                budget_json: row.get(14)?,
                max_runtime_ms: row.get(15)?,
                requires_review: row.get(16)?,
                verification_state: row.get(17)?,
                revision: row.get(18)?,
                reason_code: row.get(19)?,
                evidence_refs_json: row.get(20)?,
                artifact_refs_json: row.get(21)?,
                created_at_unix_ms: row.get(22)?,
                updated_at_unix_ms: row.get(23)?,
                completed_at_unix_ms: row.get(24)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let items =
        raw_items.into_iter().map(RawItemRow::into_record).collect::<Result<Vec<_>, _>>()?;
    Ok(Some(WorkGraphSnapshotV1 { graph, items }))
}

struct RawGraphRow {
    schema_version: i64,
    graph_id: String,
    owner_principal: String,
    device_id: String,
    channel: Option<String>,
    session_id: Option<String>,
    origin_run_id: Option<String>,
    objective_id: Option<String>,
    routine_id: Option<String>,
    flow_id: Option<String>,
    flow_step_id: Option<String>,
    state: String,
    budget_json: String,
    revision: i64,
    reason_code: String,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    completed_at_unix_ms: Option<i64>,
}

impl RawGraphRow {
    fn into_record(self) -> Result<WorkGraphRecordV1, JournalError> {
        Ok(WorkGraphRecordV1 {
            schema_version: u32::try_from(self.schema_version).map_err(|_| corrupt("schema"))?,
            graph_id: self.graph_id,
            owner: crate::domain::work_graph::WorkGraphOwnerScopeV1 {
                principal: self.owner_principal,
                device_id: self.device_id,
                channel: self.channel,
                session_id: self.session_id,
                origin_run_id: self.origin_run_id,
            },
            objective_id: self.objective_id,
            routine_id: self.routine_id,
            flow_id: self.flow_id,
            flow_step_id: self.flow_step_id,
            state: WorkGraphState::parse(self.state.as_str())
                .ok_or_else(|| corrupt("graph state"))?,
            budget: serde_json::from_str(self.budget_json.as_str())?,
            revision: u64::try_from(self.revision).map_err(|_| corrupt("graph revision"))?,
            reason_code: self.reason_code,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
            completed_at_unix_ms: self.completed_at_unix_ms,
        })
    }
}

struct RawItemRow {
    schema_version: i64,
    graph_id: String,
    work_item_id: String,
    title: String,
    description: String,
    state: String,
    priority: i64,
    capability_profile: String,
    dependencies_json: String,
    compensates_work_item_id: Option<String>,
    serialization_key: Option<String>,
    resource_class: String,
    provider_profile: Option<String>,
    workspace_scope: Option<String>,
    budget_json: String,
    max_runtime_ms: i64,
    requires_review: i64,
    verification_state: String,
    revision: i64,
    reason_code: String,
    evidence_refs_json: String,
    artifact_refs_json: String,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    completed_at_unix_ms: Option<i64>,
}

impl RawItemRow {
    fn into_record(self) -> Result<WorkItemRecordV1, JournalError> {
        Ok(WorkItemRecordV1 {
            schema_version: u32::try_from(self.schema_version).map_err(|_| corrupt("schema"))?,
            graph_id: self.graph_id,
            work_item_id: self.work_item_id,
            title: self.title,
            description: self.description,
            state: WorkItemState::parse(self.state.as_str())
                .ok_or_else(|| corrupt("item state"))?,
            priority: i32::try_from(self.priority).map_err(|_| corrupt("priority"))?,
            capability_profile: self.capability_profile,
            dependency_ids: serde_json::from_str(self.dependencies_json.as_str())?,
            compensates_work_item_id: self.compensates_work_item_id,
            serialization_key: self.serialization_key,
            resource_class: WorkResourceClass::parse(self.resource_class.as_str())
                .ok_or_else(|| corrupt("resource class"))?,
            provider_profile: self.provider_profile,
            workspace_scope: self.workspace_scope,
            budget: serde_json::from_str(self.budget_json.as_str())?,
            max_runtime_ms: u64::try_from(self.max_runtime_ms)
                .map_err(|_| corrupt("max runtime"))?,
            requires_review: self.requires_review != 0,
            verification_state: WorkVerificationState::parse(self.verification_state.as_str())
                .ok_or_else(|| corrupt("verification state"))?,
            revision: u64::try_from(self.revision).map_err(|_| corrupt("item revision"))?,
            reason_code: self.reason_code,
            evidence_refs: serde_json::from_str(self.evidence_refs_json.as_str())?,
            artifact_refs: serde_json::from_str(self.artifact_refs_json.as_str())?,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
            completed_at_unix_ms: self.completed_at_unix_ms,
        })
    }
}

fn corrupt(field: &str) -> JournalError {
    JournalError::InvalidWorkGraph {
        reason_code: reason::INVALID_GRAPH.to_owned(),
        message: format!("invalid durable {field}"),
    }
}

struct EventInsert<'a> {
    graph_id: &'a str,
    work_item_id: Option<&'a str>,
    graph_revision: u64,
    item_revision: Option<u64>,
    event_type: &'a str,
    actor_principal: &'a str,
    from_state: Option<&'a str>,
    to_state: Option<&'a str>,
    reason_code: &'a str,
    payload_json: &'a str,
    created_at_unix_ms: i64,
}

fn insert_event(transaction: &Transaction<'_>, event: EventInsert<'_>) -> Result<(), JournalError> {
    ensure_json_field(event.payload_json, "work_graph_event.payload_json")?;
    transaction.execute(
        r#"
            INSERT INTO work_graph_events (
                event_ulid, graph_ulid, work_item_ulid, graph_revision, item_revision,
                event_type, actor_principal, from_state, to_state, reason_code,
                payload_json, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            Ulid::new().to_string(),
            event.graph_id,
            event.work_item_id,
            u64_to_sqlite(event.graph_revision, "graph_revision")?,
            event.item_revision.map(|value| u64_to_sqlite(value, "item_revision")).transpose()?,
            event.event_type,
            event.actor_principal,
            event.from_state,
            event.to_state,
            event.reason_code,
            event.payload_json,
            event.created_at_unix_ms,
        ],
    )?;
    Ok(())
}
