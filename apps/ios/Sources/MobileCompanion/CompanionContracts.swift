import Foundation

enum MobileReleaseCapability: String, CaseIterable, Codable {
    case approvalsInbox = "approvals_inbox"
    case pollingNotifications = "polling_notifications"
    case recentSessions = "recent_sessions"
    case safeUrlOpen = "safe_url_open"
    case voiceNote = "voice_note"
}

enum MobileInboxPriority: String, Codable {
    case critical
    case high
    case medium
    case low
}

struct MobileHandoffTarget: Codable, Equatable {
    var path: String
    var intent: String?
    var requiresFullConsole: Bool
}

struct MobileSessionRecap: Codable, Equatable {
    var title: String
    var preview: String?
    var lastSummary: String?
    var lastIntent: String?
    var lastRunState: String?
    var pendingApprovals: Int
    var handoffRecommended: Bool
}

struct MobileApprovalSummary: Codable, Equatable {
    var approvalId: String
    var title: String
    var summary: String
    var priority: MobileInboxPriority
    var handoffTarget: MobileHandoffTarget
}

struct MobileBootstrapContract: Codable, Equatable {
    var localeOptions: [String] = ["en", "qps-ploc"]
    var defaultLocale: String = "en"
    var releaseScope: Set<MobileReleaseCapability> = [
        .approvalsInbox,
        .pollingNotifications,
        .recentSessions,
        .safeUrlOpen,
        .voiceNote,
    ]
    var defaultPollIntervalMs: Int = 45_000
    var quietHoursSupported: Bool = true
    var revokeSupported: Bool = true
}
