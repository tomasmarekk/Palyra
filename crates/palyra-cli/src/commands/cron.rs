use crate::*;

pub(crate) fn run_cron(command: CronCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for cron command"))?;
    let connection = root_context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::USER)?;
    let runtime = build_runtime()?;
    runtime.block_on(run_cron_async(command, connection))
}

pub(crate) async fn run_cron_async(
    command: CronCommand,
    connection: AgentConnection,
) -> Result<()> {
    let mut client =
        cron_v1::cron_service_client::CronServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url)
            })?;

    match command {
        CronCommand::List { after, limit, enabled, owner, channel, json } => {
            let mut request = Request::new(cron_v1::ListJobsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                after_job_ulid: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
                enabled,
                owner_principal: owner,
                channel,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_jobs(request)
                .await
                .context("failed to call cron ListJobs")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "jobs": response.jobs.iter().map(cron_job_to_json).collect::<Vec<_>>(),
                        "next_after_job_ulid": response.next_after_job_ulid,
                    }))
                    .context("failed to serialize JSON output")?
                );
            } else {
                println!(
                    "cron.list jobs={} next_after={}",
                    response.jobs.len(),
                    if response.next_after_job_ulid.is_empty() {
                        "none"
                    } else {
                        response.next_after_job_ulid.as_str()
                    }
                );
                for job in response.jobs {
                    let id =
                        job.job_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown");
                    println!(
                        "cron.job id={} name={} enabled={} owner={} channel={} next_run_at_ms={}",
                        id,
                        job.name,
                        job.enabled,
                        job.owner_principal,
                        job.channel,
                        job.next_run_at_unix_ms
                    );
                }
            }
        }
        CronCommand::Show { id, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::GetJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_job(request).await.context("failed to call cron GetJob")?.into_inner();
            let job = response.job.context("cron GetJob returned empty job payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                println!(
                    "cron.show id={} name={} enabled={} owner={} channel={} schedule_type={}",
                    id,
                    job.name,
                    job.enabled,
                    job.owner_principal,
                    job.channel,
                    job.schedule.map(|s| s.r#type).unwrap_or_default()
                );
            }
        }
        CronCommand::Add {
            name,
            prompt,
            schedule_type,
            schedule,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            owner,
            channel,
            session_key,
            session_label,
            json,
        } => {
            let schedule = build_cron_schedule(schedule_type, schedule)?;
            let mut request = Request::new(cron_v1::CreateJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                name,
                prompt,
                owner_principal: owner.unwrap_or_else(|| connection.principal.clone()),
                channel: channel.unwrap_or_else(|| "system:cron".to_owned()),
                session_key: session_key.unwrap_or_default(),
                session_label: session_label.unwrap_or_default(),
                schedule: Some(schedule),
                enabled,
                concurrency_policy: cron_concurrency_to_proto(concurrency),
                retry_policy: Some(cron_v1::RetryPolicy {
                    max_attempts: retry_max_attempts.max(1),
                    backoff_ms: retry_backoff_ms.max(1),
                }),
                misfire_policy: cron_misfire_to_proto(misfire),
                jitter_ms,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .create_job(request)
                .await
                .context("failed to call cron CreateJob")?
                .into_inner();
            let job = response.job.context("cron CreateJob returned empty job payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                let id = job.job_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown");
                println!(
                    "cron.add id={} name={} enabled={} owner={} channel={}",
                    id, job.name, job.enabled, job.owner_principal, job.channel
                );
            }
        }
        CronCommand::Update {
            id,
            name,
            prompt,
            schedule_type,
            schedule,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            owner,
            channel,
            session_key,
            session_label,
            json,
        } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let schedule = match (schedule_type, schedule) {
                (Some(schedule_type), Some(schedule)) => {
                    Some(build_cron_schedule(schedule_type, schedule)?)
                }
                (None, None) => None,
                _ => {
                    unreachable!("clap requires schedule-type and schedule to be provided together")
                }
            };
            let retry_policy = match (retry_max_attempts, retry_backoff_ms) {
                (Some(max_attempts), Some(backoff_ms)) => Some(cron_v1::RetryPolicy {
                    max_attempts: max_attempts.max(1),
                    backoff_ms: backoff_ms.max(1),
                }),
                (None, None) => None,
                _ => unreachable!("clap requires retry policy fields to be provided together"),
            };
            let has_changes = name.is_some()
                || prompt.is_some()
                || owner.is_some()
                || channel.is_some()
                || session_key.is_some()
                || session_label.is_some()
                || schedule.is_some()
                || enabled.is_some()
                || concurrency.is_some()
                || retry_policy.is_some()
                || misfire.is_some()
                || jitter_ms.is_some();
            if !has_changes {
                return Err(anyhow!("cron update requires at least one field to change"));
            }

            let mut request = Request::new(cron_v1::UpdateJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id }),
                name,
                prompt,
                owner_principal: owner,
                channel,
                session_key,
                session_label,
                schedule,
                enabled,
                concurrency_policy: concurrency.map(cron_concurrency_to_proto),
                retry_policy,
                misfire_policy: misfire.map(cron_misfire_to_proto),
                jitter_ms,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .update_job(request)
                .await
                .context("failed to call cron UpdateJob")?
                .into_inner();
            let job = response.job.context("cron UpdateJob returned empty job payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                println!(
                    "cron.update id={} enabled={} owner={} channel={}",
                    job.job_id.map(|value| value.ulid).unwrap_or_default(),
                    job.enabled,
                    job.owner_principal,
                    job.channel
                );
            }
        }
        CronCommand::Enable { id, json } => {
            let response = update_cron_enabled(&mut client, &connection, id, true).await?;
            if json {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!(
                    "cron.enable id={} enabled={}",
                    job.job_id.map(|value| value.ulid).unwrap_or_default(),
                    job.enabled
                );
            }
        }
        CronCommand::Disable { id, json } => {
            let response = update_cron_enabled(&mut client, &connection, id, false).await?;
            if json {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!(
                    "cron.disable id={} enabled={}",
                    job.job_id.map(|value| value.ulid).unwrap_or_default(),
                    job.enabled
                );
            }
        }
        CronCommand::RunNow { id, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::RunJobNowRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .run_job_now(request)
                .await
                .context("failed to call cron RunJobNow")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "run_id": response.run_id.map(|value| value.ulid),
                        "status": response.status,
                        "message": response.message,
                    }))?
                );
            } else {
                println!(
                    "cron.run_now id={} run_id={} status={} message={}",
                    id,
                    response.run_id.map(|value| value.ulid).unwrap_or_default(),
                    response.status,
                    response.message
                );
            }
        }
        CronCommand::Delete { id, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::DeleteJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .delete_job(request)
                .await
                .context("failed to call cron DeleteJob")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "deleted": response.deleted,
                    }))?
                );
            } else {
                println!("cron.delete id={} deleted={}", id, response.deleted);
            }
        }
        CronCommand::Logs { id, after, limit, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::ListJobRunsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
                after_run_ulid: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_job_runs(request)
                .await
                .context("failed to call cron ListJobRuns")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "runs": response.runs.iter().map(cron_run_to_json).collect::<Vec<_>>(),
                        "next_after_run_ulid": response.next_after_run_ulid,
                    }))?
                );
            } else {
                println!(
                    "cron.logs id={} runs={} next_after={}",
                    id,
                    response.runs.len(),
                    if response.next_after_run_ulid.is_empty() {
                        "none"
                    } else {
                        response.next_after_run_ulid.as_str()
                    }
                );
                for run in response.runs {
                    println!(
                        "cron.run run_id={} status={} started_at_ms={} finished_at_ms={} tool_calls={} tool_denies={}",
                        run.run_id.map(|value| value.ulid).unwrap_or_default(),
                        run.status,
                        run.started_at_unix_ms,
                        run.finished_at_unix_ms,
                        run.tool_calls,
                        run.tool_denies
                    );
                }
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}
