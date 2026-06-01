package io.palyra.mobile.companion

enum class MobileReleaseCapability {
    APPROVALS_INBOX,
    POLLING_NOTIFICATIONS,
    RECENT_SESSIONS,
    SAFE_URL_OPEN,
    VOICE_NOTE,
}

enum class MobileInboxPriority {
    CRITICAL,
    HIGH,
    MEDIUM,
    LOW,
}

data class MobileHandoffTarget(
    val path: String,
    val intent: String? = null,
    val requiresFullConsole: Boolean = false,
)

data class MobileSessionRecap(
    val title: String,
    val preview: String? = null,
    val lastSummary: String? = null,
    val lastIntent: String? = null,
    val lastRunState: String? = null,
    val pendingApprovals: Int = 0,
    val handoffRecommended: Boolean = false,
)

data class MobileApprovalSummary(
    val approvalId: String,
    val title: String,
    val summary: String,
    val priority: MobileInboxPriority,
    val handoffTarget: MobileHandoffTarget,
)

data class MobileBootstrapContract(
    val localeOptions: List<String> = listOf("en", "qps-ploc"),
    val defaultLocale: String = "en",
    val releaseScope: Set<MobileReleaseCapability> = setOf(
        MobileReleaseCapability.APPROVALS_INBOX,
        MobileReleaseCapability.POLLING_NOTIFICATIONS,
        MobileReleaseCapability.RECENT_SESSIONS,
        MobileReleaseCapability.SAFE_URL_OPEN,
        MobileReleaseCapability.VOICE_NOTE,
    ),
    val defaultPollIntervalMs: Long = 45_000,
    val quietHoursSupported: Boolean = true,
    val revokeSupported: Boolean = true,
)
