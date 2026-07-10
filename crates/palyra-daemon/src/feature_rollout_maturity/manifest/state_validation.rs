//! Cross-field validation for rollout promotion states.

use super::{
    invalid, ContractAvailability, ExecutionCompleteness, FeatureRolloutPromotion,
    FeatureRolloutPromotionManifestError, PromotionState, RolloutLifecycle,
    ShadowSideEffectPosture, SupportMaturity,
};

/// Enforces the permitted contract, execution, support, and lifecycle posture
/// for every promotion state. Keeping this matrix explicit prevents a rollout
/// label from implying executable maturity that its other dimensions deny.
pub(super) fn validate_promotion_state_matrix(
    path: &str,
    rollout: &FeatureRolloutPromotion,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    match rollout.promotion_state {
        PromotionState::ContractOnly => {
            if rollout.lifecycle == RolloutLifecycle::Active
                && !matches!(
                    rollout.support_maturity,
                    SupportMaturity::Unsupported
                        | SupportMaturity::Experimental
                        | SupportMaturity::Preview
                )
            {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "active contract_only promotion permits unsupported, experimental, or preview support",
                ));
            }
            if rollout.shadow_side_effect_posture == ShadowSideEffectPosture::SideEffectFree {
                return Err(invalid(
                    format!("{path}.shadow_side_effect_posture"),
                    "contract_only promotion cannot claim a qualified shadow execution posture",
                ));
            }
        }
        PromotionState::Shadow => {
            require_active_runtime_execution(path, rollout, "shadow")?;
            if !matches!(
                rollout.support_maturity,
                SupportMaturity::Experimental | SupportMaturity::Preview
            ) {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "shadow promotion requires experimental or preview support",
                ));
            }
            if rollout.shadow_side_effect_posture != ShadowSideEffectPosture::SideEffectFree {
                return Err(invalid(
                    format!("{path}.shadow_side_effect_posture"),
                    "shadow promotion requires a side_effect_free posture",
                ));
            }
        }
        PromotionState::Canary => {
            require_active_runtime_execution(path, rollout, "canary")?;
            if !matches!(
                rollout.support_maturity,
                SupportMaturity::Experimental | SupportMaturity::Preview
            ) {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "canary promotion requires experimental or preview support",
                ));
            }
            require_non_shadow_posture(path, rollout, "canary")?;
        }
        PromotionState::GatedProduction => {
            require_active_runtime_execution(path, rollout, "gated_production")?;
            if rollout.execution_completeness != ExecutionCompleteness::Complete {
                return Err(invalid(
                    format!("{path}.execution_completeness"),
                    "gated_production promotion requires complete execution",
                ));
            }
            if !matches!(
                rollout.support_maturity,
                SupportMaturity::Preview | SupportMaturity::Supported
            ) {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "gated_production promotion requires preview or supported maturity",
                ));
            }
            require_non_shadow_posture(path, rollout, "gated_production")?;
        }
        PromotionState::Stable => {
            require_active_runtime_execution(path, rollout, "stable")?;
            if rollout.execution_completeness != ExecutionCompleteness::Complete {
                return Err(invalid(
                    format!("{path}.execution_completeness"),
                    "stable promotion requires complete execution",
                ));
            }
            if rollout.support_maturity != SupportMaturity::Supported {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "stable promotion requires supported maturity",
                ));
            }
            require_non_shadow_posture(path, rollout, "stable")?;
        }
    }
    Ok(())
}

fn require_active_runtime_execution(
    path: &str,
    rollout: &FeatureRolloutPromotion,
    promotion_state: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if rollout.contract_availability != ContractAvailability::RuntimeAvailable {
        return Err(invalid(
            format!("{path}.contract_availability"),
            format!("{promotion_state} promotion requires runtime availability"),
        ));
    }
    if rollout.execution_completeness == ExecutionCompleteness::NotImplemented {
        return Err(invalid(
            format!("{path}.execution_completeness"),
            format!("{promotion_state} promotion requires partial or complete execution"),
        ));
    }
    if rollout.lifecycle != RolloutLifecycle::Active {
        return Err(invalid(
            format!("{path}.lifecycle"),
            format!("{promotion_state} promotion requires an active lifecycle"),
        ));
    }
    Ok(())
}

fn require_non_shadow_posture(
    path: &str,
    rollout: &FeatureRolloutPromotion,
    promotion_state: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if rollout.shadow_side_effect_posture != ShadowSideEffectPosture::NotApplicable {
        return Err(invalid(
            format!("{path}.shadow_side_effect_posture"),
            format!("{promotion_state} promotion requires a not_applicable shadow posture"),
        ));
    }
    Ok(())
}
