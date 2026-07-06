//! Runtime-boundary metrics catalog and safe exporter projection.

#![allow(dead_code)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const RUNTIME_BOUNDARY_METRICS_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMetricKind {
    Counter,
    Histogram,
    Gauge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMetricDescriptor {
    pub name: &'static str,
    pub kind: RuntimeMetricKind,
    pub help: &'static str,
    pub labels: &'static [&'static str],
}

pub const RUNTIME_BOUNDARY_METRICS: [RuntimeMetricDescriptor; 18] = [
    RuntimeMetricDescriptor {
        name: "palyra_runtime_harness_selection_total",
        kind: RuntimeMetricKind::Counter,
        help: "Harness selection decisions.",
        labels: &["runtime", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_harness_errors_total",
        kind: RuntimeMetricKind::Counter,
        help: "Harness runtime errors by stable class.",
        labels: &["runtime", "failure_class"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_hook_invocations_total",
        kind: RuntimeMetricKind::Counter,
        help: "Inline runtime hook invocations.",
        labels: &["hook", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_hook_timeouts_total",
        kind: RuntimeMetricKind::Counter,
        help: "Inline runtime hook timeout decisions.",
        labels: &["hook"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_middleware_invocations_total",
        kind: RuntimeMetricKind::Counter,
        help: "Tool-result middleware invocations.",
        labels: &["middleware", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_provider_repairs_total",
        kind: RuntimeMetricKind::Counter,
        help: "Provider repair attempts.",
        labels: &["provider", "repair_class"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_recovery_failures_total",
        kind: RuntimeMetricKind::Counter,
        help: "Provider recovery failures.",
        labels: &["provider", "failure_class"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_harness_duration_ms",
        kind: RuntimeMetricKind::Histogram,
        help: "Harness attempt duration.",
        labels: &["runtime", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_approval_relay_latency_ms",
        kind: RuntimeMetricKind::Histogram,
        help: "Approval relay latency.",
        labels: &["surface", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_provider_recovery_duration_ms",
        kind: RuntimeMetricKind::Histogram,
        help: "Provider recovery duration.",
        labels: &["provider", "recovery_class"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_terminal_duration_ms",
        kind: RuntimeMetricKind::Histogram,
        help: "Persistent terminal command duration.",
        labels: &["backend", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_lsp_request_duration_ms",
        kind: RuntimeMetricKind::Histogram,
        help: "LSP request duration.",
        labels: &["language", "method"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_active_terminal_sessions",
        kind: RuntimeMetricKind::Gauge,
        help: "Active terminal sessions.",
        labels: &["backend"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_acp_handles",
        kind: RuntimeMetricKind::Gauge,
        help: "Active ACP runtime handles.",
        labels: &["runtime", "state"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_quarantined_credentials",
        kind: RuntimeMetricKind::Gauge,
        help: "Quarantined credential records.",
        labels: &["provider"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_background_processes",
        kind: RuntimeMetricKind::Gauge,
        help: "Background process handles.",
        labels: &["backend", "state"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_acp_permission_relay_total",
        kind: RuntimeMetricKind::Counter,
        help: "ACP permission relay decisions.",
        labels: &["runtime", "outcome"],
    },
    RuntimeMetricDescriptor {
        name: "palyra_runtime_cleanup_failures_total",
        kind: RuntimeMetricKind::Counter,
        help: "Runtime cleanup failures.",
        labels: &["boundary", "failure_class"],
    },
];

#[must_use]
pub fn runtime_boundary_metric_catalog() -> &'static [RuntimeMetricDescriptor] {
    &RUNTIME_BOUNDARY_METRICS
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeMetricSample {
    pub name: String,
    pub kind: RuntimeMetricKind,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeMetricsExportOutcome {
    pub attempted: bool,
    pub blocked_run: bool,
    pub exported_samples: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBoundaryDiagnosticsSummary {
    pub schema_version: u32,
    pub counters: usize,
    pub histograms: usize,
    pub gauges: usize,
    pub acp_metrics_present: bool,
    pub health: String,
}

#[must_use]
pub fn sanitize_metric_label(key: &str, value: &str) -> String {
    if palyra_common::redaction::is_sensitive_key(key)
        || value.to_ascii_lowercase().contains("token")
        || value.contains('\\')
        || value.contains('/')
    {
        return "redacted".to_owned();
    }
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    sanitized.trim_matches('_').chars().take(48).collect::<String>()
}

#[must_use]
pub fn runtime_metric_sample(
    descriptor: RuntimeMetricDescriptor,
    labels: impl IntoIterator<Item = (&'static str, String)>,
    value: f64,
) -> RuntimeMetricSample {
    RuntimeMetricSample {
        name: descriptor.name.to_owned(),
        kind: descriptor.kind,
        labels: labels
            .into_iter()
            .map(|(key, value)| (key.to_owned(), sanitize_metric_label(key, value.as_str())))
            .collect(),
        value,
    }
}

#[must_use]
pub fn render_prometheus_text(samples: &[RuntimeMetricSample]) -> String {
    let mut rendered = String::new();
    for sample in samples {
        rendered.push_str(sample.name.as_str());
        if !sample.labels.is_empty() {
            let labels = sample
                .labels
                .iter()
                .map(|(key, value)| format!("{key}=\"{value}\""))
                .collect::<Vec<_>>()
                .join(",");
            rendered.push('{');
            rendered.push_str(labels.as_str());
            rendered.push('}');
        }
        rendered.push(' ');
        rendered.push_str(sample.value.to_string().as_str());
        rendered.push('\n');
    }
    rendered
}

#[must_use]
pub fn export_runtime_metrics_non_blocking(
    samples: &[RuntimeMetricSample],
    exporter_available: bool,
) -> RuntimeMetricsExportOutcome {
    if exporter_available {
        RuntimeMetricsExportOutcome {
            attempted: true,
            blocked_run: false,
            exported_samples: samples.len(),
            error: None,
        }
    } else {
        RuntimeMetricsExportOutcome {
            attempted: true,
            blocked_run: false,
            exported_samples: 0,
            error: Some("runtime metrics exporter unavailable".to_owned()),
        }
    }
}

#[must_use]
pub fn runtime_boundary_diagnostics_summary() -> RuntimeBoundaryDiagnosticsSummary {
    let counters = RUNTIME_BOUNDARY_METRICS
        .iter()
        .filter(|entry| entry.kind == RuntimeMetricKind::Counter)
        .count();
    let histograms = RUNTIME_BOUNDARY_METRICS
        .iter()
        .filter(|entry| entry.kind == RuntimeMetricKind::Histogram)
        .count();
    let gauges = RUNTIME_BOUNDARY_METRICS
        .iter()
        .filter(|entry| entry.kind == RuntimeMetricKind::Gauge)
        .count();
    let acp_metrics_present = RUNTIME_BOUNDARY_METRICS
        .iter()
        .any(|entry| entry.name.contains("_acp_") || entry.name.ends_with("_acp_handles"));
    RuntimeBoundaryDiagnosticsSummary {
        schema_version: RUNTIME_BOUNDARY_METRICS_SCHEMA_VERSION,
        counters,
        histograms,
        gauges,
        acp_metrics_present,
        health: if acp_metrics_present { "healthy" } else { "degraded" }.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_catalog_covers_runtime_boundaries() {
        let catalog = runtime_boundary_metric_catalog();

        assert!(catalog.iter().any(|entry| entry.name == "palyra_runtime_harness_selection_total"));
        assert!(catalog.iter().any(|entry| entry.name == "palyra_runtime_hook_invocations_total"));
        assert!(catalog.iter().any(|entry| entry.name == "palyra_runtime_acp_handles"));
        assert!(catalog.iter().any(|entry| entry.kind == RuntimeMetricKind::Histogram));
    }

    #[test]
    fn label_sanitization_blocks_secrets_and_paths() {
        assert_eq!(sanitize_metric_label("api_key", "secret"), "redacted");
        assert_eq!(sanitize_metric_label("runtime", "C:/Users/Palo/project"), "redacted");
        assert_eq!(sanitize_metric_label("provider", "OpenAI Primary"), "openai_primary");
    }

    #[test]
    fn prometheus_text_contains_redacted_labels() {
        let descriptor = RUNTIME_BOUNDARY_METRICS[13];
        let sample = runtime_metric_sample(
            descriptor,
            [("runtime", "native token runtime".to_owned()), ("state", "active".to_owned())],
            2.0,
        );
        let rendered = render_prometheus_text(&[sample]);

        assert!(rendered.contains("runtime=\"redacted\""));
        assert!(rendered.contains("state=\"active\""));
    }

    #[test]
    fn exporter_failure_is_non_blocking() {
        let outcome = export_runtime_metrics_non_blocking(&[], false);

        assert!(outcome.attempted);
        assert!(!outcome.blocked_run);
        assert!(outcome.error.is_some());
    }

    #[test]
    fn diagnostics_summary_reports_acp_metrics() {
        let summary = runtime_boundary_diagnostics_summary();

        assert!(summary.acp_metrics_present);
        assert_eq!(summary.health, "healthy");
        assert!(summary.counters > 0);
        assert!(summary.gauges > 0);
    }
}
