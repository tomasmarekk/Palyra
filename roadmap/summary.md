# Palyra – syntetická roadmapa dalšího vývoje

Tato roadmapa je složená primárně z těchto tří analýz: `palyra_vs_openclaw_analysis.md`, `palyra_vs_ironclaw_analysis.md`, `palyra_vs_hermes_analysis.md`. Cílem není slepě kopírovat konkurenci, ale produktizovat to, co už je v Palyře silné, a doplnit chybějící workflow vrstvy tam, kde konkurence působí lépe.

Celkem: **60 milestoneů** rozdělených do **10 fází**.

## Hlavní principy roadmapy
- Neoslabovat fail-closed security model, approvals, access/pairing ani auditovatelnost jen kvůli snazšímu onboardingu.
- Nepřepisovat browser relay na managed browser jako default. Pokud někdy přibude managed mode, pouze jako volitelný doplněk.
- Nezplošťovat operator control-plane do čistě consumer/personal-assistant UX. Basic mode má být vrstva nad silným jádrem, ne náhrada jádra.
- Nemíchat deterministic project context (`PALYRA.md` a spol.) s learned memory. Jsou to dvě odlišné vrstvy s jiným trust modelem.
- Voice dělat desktop-first a push-to-talk first. Žádný always-on listening nebo wake-word default.
- Mobile držet úzký: approvals, notifications, handoff, voice note. Nehonit širokou feature parity s desktopem.
- Každý milestone musí být měřitelný přes funnel/telemetrii a vratný přes rollout flags nebo jasný release gate.

## Jak je roadmapa poskládaná
1. Nejprve zafixovat, co se nesmí rozbít, zavést měření a připravit shell, handoffy a lokalizační základ. Bez toho by další UX vrstva jen přidávala chaos.
2. Teprve nad stabilními základy produktizovat onboarding, first success a discoverability, aby Palyra rychleji ukázala hodnotu bez ztráty své operator hloubky.
3. Jakmile nový uživatel umí projít prvním flow, je potřeba zlepšit důvěru a čitelnost oprávnění: per-tool posture, explainability a boj proti approval fatigue.
4. Potom dotáhnout session continuity, protože právě tady se láme rozdíl mezi jednorázovým demem a daily-driver používáním.
5. Na session vrstvu navázat deterministický projektový kontext, který zlepší práci v repozitářích a udrží oddělení project rules od learned memory.
6. Teprve pak rozšířit observabilitu z textového transcriptu na reálné workspace výstupy a bezpečné rollback workflow.
7. Až budou session i workspace pevnější, zviditelnit canvas a sjednotit webové surface tak, aby moderní agent UX stálo na pevných datech a audit stopě.
8. Následně produktizovat desktop ambient mode a voice. V tomto pořadí získáte vyšší vnímanou kvalitu bez rozbití trust modelu.
9. Poté srovnat TUI, aby schopnost terminálu nezaostávala za webem a desktopem v každodenní ergonomii.
10. Nakonec přidat úzký mobile companion, skutečný lokalizační rollout a release hardening, aby šly nové capabilities bezpečně vydat.

## Fáze 1 – Guardraily, měření a společné základy

Nejprve zafixovat, co se nesmí rozbít, zavést měření a připravit shell, handoffy a lokalizační základ. Bez toho by další UX vrstva jen přidávala chaos.

- [x] **M001 – Non-regression kontrakt a guardraily produktu** — Sepsat a vynutit mantinely, aby produktizace nezničila fail-closed security model, approvals, browser relay default a šíři operator control-plane. ([detail](milestones/M001_non_regression_guardrails.md))
- [x] **M002 – Aktivační telemetrie a UX baseline metriky** — Změřit onboarding funnel, approval fatigue, session resume chování, využití desktopu/TUI/voice/canvas a mít před změnami reálnou baseline. ([detail](milestones/M002_activation_telemetry_baseline.md))
- [x] **M003 – Cross-surface handoff a deep-link kontrakt** — Zavést jednotný formát pro předávání kontextu mezi webem, desktopem, TUI a budoucím mobile companionem. ([detail](milestones/M003_cross_surface_handoff_contract.md))
- [x] **M004 – i18n foundation pro web, desktop a TUI** — Začít s lokalizační infrastrukturou hned, dokud UI ještě není přetížené stovkami hardcoded stringů. ([detail](milestones/M004_i18n_foundation.md))
- [x] **M005 – Základ pro Basic mode / Advanced mode a reusable guidance komponenty** — Postavit UI základ pro progressive disclosure, aby onboarding a first-success flow neprobíhaly nad plnou šířkou control-plane menu bez kontextu. ([detail](milestones/M005_basic_advanced_shell_foundation.md))

## Fáze 2 – Onboarding, guided success a discoverability

Teprve nad stabilními základy produktizovat onboarding, first success a discoverability, aby Palyra rychleji ukázala hodnotu bez ztráty své operator hloubky.

- [ ] **M006 – Kanonický onboarding orchestrátor napříč CLI, desktopem a webem** — Sjednotit dnes rozptýlené setup wizardy a onboarding stavy do jednoho sdíleného flow modelu. ([detail](milestones/M006_canonical_onboarding_orchestrator.md))
- [ ] **M007 – Quick Start flow s cílem 'první úspěch do 10 minut'** — Postavit lineární guided flow pro nového uživatele, který chce produkt rychle rozchodit bez studia celé control-plane šíře. ([detail](milestones/M007_quick_start_flow.md))
- [ ] **M008 – Advanced setup flow pro plnou control-plane konfiguraci** — Oddělit expertní konfiguraci od Quick Startu, aniž by se ztratila síla produktu. ([detail](milestones/M008_advanced_setup_flow.md))
- [ ] **M009 – Ověření provideru, modelu a runtime s inline opravami** — Přidat do onboardingu i běžného provozu konkrétní verifikační krok s rychlou opravou portů, stale PIDů, špatných klíčů a nefunkčních modelů. ([detail](milestones/M009_provider_model_runtime_verification.md))
- [ ] **M010 – Guided first-success scénář a automatický handoff do první chat session** — Po setupu uživatele nedovést jen na dashboard, ale do konkrétního scénáře, který ukáže skutečnou hodnotu Palyry. ([detail](milestones/M010_guided_first_success.md))
- [ ] **M011 – Přepsání README a vybudování docs-users skeletonu** — Z externí dokumentace udělat produktový vstup, ne mrtvý placeholder. ([detail](milestones/M011_docs_rewrite.md))
- [ ] **M012 – Showcase, starter prompty a doporučené další kroky uvnitř produktu** — Zvýšit discoverability přímo v produktu místo spoléhat jen na README a externí docs. ([detail](milestones/M012_showcase_starter_prompts_next_actions.md))

## Fáze 3 – Explainable permissions a trust UX

Jakmile nový uživatel umí projít prvním flow, je potřeba zlepšit důvěru a čitelnost oprávnění: per-tool posture, explainability a boj proti approval fatigue.

- [ ] **M013 – Datový model a API pro Tool Posture / Tool Permissions** — Postavit čitelnou vrstvu nad existující policy/approval logikou, která umí pro každý tool vysvětlit jeho efektivní stav. ([detail](milestones/M013_tool_posture_data_model.md))
- [ ] **M014 – Tool Permissions Center UI** — Vytvořit centrální UI plochu, kde operátor jedním pohledem vidí a upraví postoj ke konkrétním nástrojům. ([detail](milestones/M014_tool_permissions_center_ui.md))
- [ ] **M015 – Inline explainability v místech tření: approvals, blokace a disabled tools** — Nahradit obecné chybové hlášky konkrétním vysvětlením, proč je akce blokovaná nebo proč vyžaduje approval. ([detail](milestones/M015_inline_policy_explainability.md))
- [ ] **M016 – Scope hierarchy editor, preset bundles a audit trail pro permissions** — Umožnit bezpečné a srozumitelné změny posture na úrovních global/workspace/agent/session bez chaosu v precedence pravidlech. ([detail](milestones/M016_scope_hierarchy_presets_audit.md))
- [ ] **M017 – Měření approval fatigue a doporučené výchozí posture** — Na základě reálných dat snížit zbytečné approvals, aniž by se oslabila bezpečnostní disciplína. ([detail](milestones/M017_approval_fatigue_recommendations.md))

## Fáze 4 – Session continuity a každodenní ovládání session

Potom dotáhnout session continuity, protože právě tady se láme rozdíl mezi jednorázovým demem a daily-driver používáním.

- [ ] **M018 – Asynchronní semantické názvy session** — Nahradit dnešní first-message auto-title lidsky čitelnějšími, stabilnějšími a rodinově seskupitelnými názvy. ([detail](milestones/M018_semantic_session_titles.md))
- [ ] **M019 – `/title` command a rename affordance napříč surface** — Udělá z přejmenování session první-class akci místo skryté capability. ([detail](milestones/M019_title_command_and_rename_affordances.md))
- [ ] **M020 – Resume recap surface pro web, desktop i TUI** — Z každého návratu do session udělat informovaný restart práce místo skoku do starého transcriptu bez kontextu. ([detail](milestones/M020_resume_recap_surface.md))
- [ ] **M021 – Lineage title families a resume podle názvové rodiny** — Zavést lidsky čitelný model `Incident triage`, `Incident triage #2`, `#3` a umět podle něj session hledat a resumovat. ([detail](milestones/M021_lineage_title_families.md))
- [ ] **M022 – Upgrade session katalogu a hledání** — Rozšířit session rail a katalog tak, aby se daly rychle najít relevantní session podle skutečné práce, ne jen podle času nebo ID. ([detail](milestones/M022_session_catalog_search_upgrades.md))
- [ ] **M023 – Inline quick controls pro session v chat headeru a inspectoru** — Přidat rychlé ovládání agenta, modelu, thinking/trace/verbose a resetu na default bez závislosti na slash commands. ([detail](milestones/M023_session_quick_controls.md))

## Fáze 5 – Deterministický projektový kontext

Na session vrstvu navázat deterministický projektový kontext, který zlepší práci v repozitářích a udrží oddělení project rules od learned memory.

- [ ] **M024 – PALYRA.md / AGENTS.md kompatibilitní vrstva pro projektový kontext** — Zavést deterministický project-context layer vedle stávající memory/reference logiky. ([detail](milestones/M024_project_context_file_compatibility.md))
- [ ] **M025 – Progressive discovery context files při průchodu adresáři** — Načítat další relevantní context files podle toho, kam se práce skutečně přesouvá v repu. ([detail](milestones/M025_progressive_directory_context_discovery.md))
- [ ] **M026 – Active context stack inspector ve webu, desktopu a TUI** — Ukázat uživateli přesně, které deterministické instrukce jsou právě aktivní a v jakém pořadí se aplikují. ([detail](milestones/M026_active_context_stack_inspector.md))
- [ ] **M027 – Injection scanner a risk UX pro project context files** — Přidat přísnější bezpečnostní kontrolu kontextových souborů, než jakou ukazuje konkurence. ([detail](milestones/M027_context_injection_scanner.md))
- [ ] **M028 – Oddělení deterministic context vrstvy od learned memory + authoring templates** — Zabránit míchání projektových pravidel, user/workspace memory a ad-hoc reference vrstev. ([detail](milestones/M028_separate_context_from_memory.md))

## Fáze 6 – Workspace observabilita a rollback safety net

Teprve pak rozšířit observabilitu z textového transcriptu na reálné workspace výstupy a bezpečné rollback workflow.

- [ ] **M029 – Run-scoped workspace artifact index a file API** — Přidat backendovou vrstvu, která z runů a checkpointů udělá prohlížitelné workspace artefakty místo černé skříňky. ([detail](milestones/M029_workspace_artifact_index_api.md))
- [ ] **M030 – Workspace tab v ChatRunDraweru** — Přidat do run inspectoru přímý vstup k souborům a artefaktům, které během běhu vznikly nebo se změnily. ([detail](milestones/M030_workspace_tab_in_run_drawer.md))
- [ ] **M031 – Previewery, changed-files filtr, fulltext search a diff mezi retry/checkpointy** — Dodat workspace exploreru hloubku, aby neukazoval jen seznam souborů, ale skutečný přehled změn a výstupů. ([detail](milestones/M031_workspace_preview_search_diff.md))
- [ ] **M032 – Promote akce: Memory / Support bundle / Canvas / Artifact** — Proměnit důležité výstupy běhu v trvalejší a sdílitelnější entity bez ručního přepisování nebo stahování. ([detail](milestones/M032_artifact_promotion_actions.md))
- [ ] **M033 – Workspace checkpoint subsystem pro file-level bezpečnost** — Zavést samostatnou vrstvu snapshotů souborů, která je odlišená od conversation checkpointů. ([detail](milestones/M033_workspace_checkpoint_subsystem.md))
- [ ] **M034 – `/rollback` UX a diff preview napříč surface** — Z workspace checkpointů udělat skutečně použitelné uživatelské flow místo interní recovery capability. ([detail](milestones/M034_rollback_ux_and_diff_preview.md))
- [ ] **M035 – Restore workspace/souboru a následná session reconciliation** — Po file restore srovnat i mentální model agenta a session metadata, aby systém nepokračoval se zastaralým obrazem workspace. ([detail](milestones/M035_restore_and_session_reconciliation.md))
- [ ] **M036 – Audit, inventory a support integrace pro workspace restore** — Z rollbacku udělat plnohodnotně operovatelnou a dohledatelnou capability, ne jen lokální trik pro coding UX. ([detail](milestones/M036_audit_inventory_support_integration_for_restore.md))

## Fáze 7 – Canvas a moderní web surface

Až budou session i workspace pevnější, zviditelnit canvas a sjednotit webové surface tak, aby moderní agent UX stálo na pevných datech a audit stopě.

- [ ] **M037 – Canvas jako first-class session surface: routing a state model** — Povýšit canvas z experimentu/diagnostiky na běžnou součást session workflow ve webu a desktopu. ([detail](milestones/M037_canvas_first_class_routing.md))
- [ ] **M038 – Chat affordance pro Open/Pin/Reopen canvas a agent-rendered canvas bloky** — Napojit canvas přímo na chat, aby uživatel nemusel hledat experimentální vstupy mimo hlavní workflow. ([detail](milestones/M038_chat_canvas_affordances.md))
- [ ] **M039 – Canvas history, snapshots, rollback a vazba na session/run** — Udělat z canvasu inspectable a vratnou surface místo efemérní vizualizace bez historie. ([detail](milestones/M039_canvas_history_snapshots_rollback.md))
- [ ] **M040 – Unified artifact open targets: web preview, canvas, desktop, browser** — Zavést jednotný model, kam a jak se mají otevírat artefakty napříč produktem. ([detail](milestones/M040_unified_artifact_open_targets.md))
- [ ] **M041 – Konzistenční pass pro webové surface: drawers, shortcuty, empty states, CTA patterny** — Sjednotit každodenní web UX tak, aby onboarding, sessions, workspace, canvas a approvals působily jako jeden produkt. ([detail](milestones/M041_web_surface_consistency_pass.md))

## Fáze 8 – Ambient desktop a voice productizace

Následně produktizovat desktop ambient mode a voice. V tomto pořadí získáte vyšší vnímanou kvalitu bez rozbití trust modelu.

- [ ] **M042 – Tray / menu-bar runtime pro vždy dostupný desktop companion** — Přidat ambient desktop režim, který nebude vyžadovat plné okno control-centra pro každou drobnou interakci. ([detail](milestones/M042_tray_menu_bar_runtime.md))
- [ ] **M043 – Quick companion panel s mini chatem, session pickerem a approval inboxem** — Vytvořit malý desktop panel pro nejčastější denní akce bez nutnosti otevírat celý dashboard. ([detail](milestones/M043_quick_companion_panel.md))
- [ ] **M044 – Globální hotkey, quick invoke a přesné handoffy do session/run/canvas** — Zrychlit vstup do Palyry z desktopu pomocí jedné klávesové zkratky a konzistentního otevření správného kontextu. ([detail](milestones/M044_global_hotkey_and_quick_invoke.md))
- [ ] **M045 – Ambient status, aktivní runy a offline drafts jako first-class desktop surface** — Zpřehlednit, co se právě děje, i když uživatel nemá otevřený plný dashboard. ([detail](milestones/M045_ambient_status_and_offline_drafts.md))
- [ ] **M046 – Voice overlay lifecycle jako strukturovaný desktop workflow** — Povýšit voice z feature-flag experimentu na jasně čitelný workflow s definovanými stavy. ([detail](milestones/M046_voice_overlay_lifecycle.md))
- [ ] **M047 – Push-to-talk, transcript preview a edit-before-send** — Udělá z voice vstupu důvěryhodnou první-class akci, ne jednosměrný záznam bez kontroly. ([detail](milestones/M047_push_to_talk_and_transcript_review.md))
- [ ] **M048 – Mic/TTS permissions, privacy posture, streaming TTS a silence detection** — Dodat voice vrstvě produkční disciplínu kolem oprávnění, soukromí a audio feedbacku. ([detail](milestones/M048_voice_permissions_privacy_tts.md))

## Fáze 9 – TUI jako daily-driver

Poté srovnat TUI, aby schopnost terminálu nezaostávala za webem a desktopem v každodenní ergonomii.

- [ ] **M049 – Plnohodnotný multiline composer v TUI** — Zlepšit terminálový vstup tak, aby se Palyra v CLI používala pohodlně i pro delší a strukturovanější prompty. ([detail](milestones/M049_tui_multiline_composer.md))
- [ ] **M050 – Attachment flow v TUI: cesty, clipboard a základní preview** — Zrušit dnešní stav, kdy `/attach` v TUI fakticky končí hláškou 'web-only'. ([detail](milestones/M050_tui_attachments.md))
- [ ] **M051 – Rich status bar v TUI: context fill, tokeny, cost, duration, approvals, background** — Dodat terminálu průběžnou situational awareness, která dnes často vyžaduje přepnout do webu nebo ručně psát příkazy. ([detail](milestones/M051_tui_rich_status_bar.md))
- [ ] **M052 – Resume recap banner a title-family navigace v TUI** — Přenést zlepšení session continuity i do terminálu, aby TUI nebyla druhotná surface proti webu/desktopu. ([detail](milestones/M052_tui_resume_recap_and_family_navigation.md))
- [ ] **M053 – TUI workspace explorer a rollback commandy** — Dodat terminálovým uživatelům základní inspect/recover workflow bez nutnosti utíkat do webu. ([detail](milestones/M053_tui_workspace_and_rollback_commands.md))
- [ ] **M054 – TUI discoverability polish: /help, palette, shortcuty a volitelný voice keybinding** — Dotáhnout terminál do stavu, kdy jeho schopnosti nejsou schované jen v kódu a slash registry. ([detail](milestones/M054_tui_discoverability_polish.md))

## Fáze 10 – Mobile companion, lokalizace a release hardening

Nakonec přidat úzký mobile companion, skutečný lokalizační rollout a release hardening, aby šly nové capabilities bezpečně vydat.

- [ ] **M055 – Úzká architektura mobile companionu a shared contracts** — Navrhnout mobile jako companion pro approvals, notifications a handoff, ne jako feature-parity závod s desktopem. ([detail](milestones/M055_mobile_companion_architecture.md))
- [ ] **M056 – Mobile approvals inbox a notifikace** — Postavit první skutečně užitečnou mobilní funkci: bezpečné odbavení approval backlogu a run notifikací z telefonu. ([detail](milestones/M056_mobile_approvals_and_notifications.md))
- [ ] **M057 – Mobile session handoff, recent sessions a safe URL/browser handoff** — Umožnit z telefonu navázat na rozpracovanou práci a bezpečně předávat vybrané akce dalším surface. ([detail](milestones/M057_mobile_session_handoff_and_safe_url_open.md))
- [ ] **M058 – Mobile voice note to agent** — Přidat úzkou mobilní voice feature, která dává smysl na cestách, aniž by se z telefonu stal plný voice runtime. ([detail](milestones/M058_mobile_voice_note.md))
- [ ] **M059 – Lokalizační rollout (čeština + angličtina) a translation QA** — Proměnit i18n foundation v reálně použitelnou lokalizaci hlavních flow napříč produktem. ([detail](milestones/M059_localization_rollout_cs_en.md))
- [ ] **M060 – Release hardening, QA matice, rollout gates a finální docs refresh** — Uzavřít roadmapu vydatelným stavem: testy, flagy, docs a měřené rollout rozhodování. ([detail](milestones/M060_release_hardening_qa_rollout_gates.md))
