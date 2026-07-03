//! `vup service …` — self-install as an always-on background service.
//!
//! The intended deployment is a vup daemon permanently alive on every
//! device: it serves scheduled backups (`snap_interval_secs`), watch-mode
//! vaults, FUSE mounts, and friends' sync requests, and it starts with the
//! system. Any vup command auto-spawns a daemon on demand, but that daemon
//! dies with the session; `vup service install` makes it permanent.
//!
//! Platform backends:
//! - **Linux (systemd)** — a *user* unit at
//!   `~/.config/systemd/user/vup.service`, enabled + started via
//!   `systemctl --user`; `loginctl enable-linger` (best-effort) keeps it
//!   alive across logouts and starts it at boot.
//! - **macOS (launchd)** — a LaunchAgent at
//!   `~/Library/LaunchAgents/pro.s5.vup.plist`, loaded via
//!   `launchctl bootstrap` (fallback: `load -w`).
//!
//! The generated unit embeds the CURRENT executable path and the resolved
//! config path — no `$PATH` guessing, no template placeholders (the static
//! files under `packaging/` remain for distro packagers). Re-running
//! `install` after moving/updating the binary rewrites and restarts.
//!
//! These verbs are daemon-less on purpose: installing must not auto-spawn
//! a session daemon. If a loose (session) daemon is running at install
//! time, it is asked to shut down first so the service instance takes over
//! cleanly (the endpoint key + service.lock are exclusive).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Subcommand;

/// Verbs under `vup service …`.
#[derive(Subcommand, Debug)]
pub enum ServiceCmd {
    /// Install vup as an always-on background service (systemd/launchd),
    /// start it now, and enable start-at-boot.
    #[command(alias = "i")]
    Install,
    /// Stop the service and remove it. (Does not touch your data or
    /// config; `vup` commands will auto-start a session daemon again.)
    #[command(alias = "u")]
    Uninstall,
    /// Show whether the service is installed / enabled / running.
    #[command(alias = "s")]
    Status,
}

pub async fn run_service(cmd: &ServiceCmd, config_path: &Path) -> Result<()> {
    match backend()? {
        Backend::Systemd => match cmd {
            ServiceCmd::Install => systemd_install(config_path).await,
            ServiceCmd::Uninstall => systemd_uninstall(),
            ServiceCmd::Status => systemd_status(),
        },
        Backend::Launchd => match cmd {
            ServiceCmd::Install => launchd_install(config_path).await,
            ServiceCmd::Uninstall => launchd_uninstall(),
            ServiceCmd::Status => launchd_status(),
        },
    }
}

enum Backend {
    Systemd,
    Launchd,
}

fn backend() -> Result<Backend> {
    if cfg!(target_os = "macos") {
        return Ok(Backend::Launchd);
    }
    if cfg!(target_os = "linux") {
        // The canonical "is systemd the init" probe.
        if Path::new("/run/systemd/system").exists() {
            return Ok(Backend::Systemd);
        }
        bail!(
            "no systemd detected on this Linux system. Static templates for manual\n\
             installation live in packaging/ (or run `vup _daemon` from your init of choice)."
        );
    }
    bail!(
        "`vup service` supports systemd (Linux) and launchd (macOS). On this platform,\n\
         arrange for `vup _daemon` to run at login (e.g. Windows Task Scheduler); any\n\
         vup command also auto-starts a session daemon on demand."
    );
}

/// The absolute path of the running `vup` binary — what the unit executes.
fn exe_path() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("could not determine own executable path")?;
    // Resolve symlinks (e.g. a `cargo install` shim or /proc self link) so
    // the unit survives PATH changes but still points at a real file.
    Ok(exe.canonicalize().unwrap_or(exe))
}

/// Ask a loose (session) daemon to exit before the service instance
/// starts: both would contend for the iroh endpoint key + service.lock.
/// Best-effort — a dead lock or no daemon is fine.
async fn stop_loose_daemon() {
    let Ok(Some(lock)) = s5_node_api::connect::read_lock() else {
        return;
    };
    if let Ok(client) = s5_node_api::connect::connect_with_lock(&lock).await {
        let _ = client.shutdown().await;
        client.close().await;
    }
    if let Some(pid) = lock.pid
        && !crate::node::wait_for_pid_exit(pid, Duration::from_secs(15)).await
    {
        eprintln!(
            "note: a previous daemon (pid {pid}) is still exiting; the service may take over shortly"
        );
    }
}

/// Run a command, returning (success, combined output) — the callers report
/// failures honestly instead of bailing (uninstall stays idempotent).
fn run(cmd: &str, args: &[&str]) -> (bool, String) {
    match Command::new(cmd).args(args).output() {
        Ok(out) => {
            let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
            s.push_str(&String::from_utf8_lossy(&out.stderr));
            (out.status.success(), s.trim().to_string())
        }
        Err(e) => (false, format!("{cmd}: {e}")),
    }
}

/// A compact, non-printing summary of the OS service state — the one-line
/// row `vup doctor` shows. `vup service status` stays the verbose view; this
/// shares the same probes (unit/plist existence + `systemctl`/`launchctl`).
pub struct ServiceHealth {
    /// Whether the unit/plist file exists on disk.
    pub installed: bool,
    /// Whether the service is currently running/loaded.
    pub active: bool,
    /// Short human summary (e.g. `enabled, active` / `loaded` /
    /// `not installed` / `unsupported platform`).
    pub detail: String,
}

/// Inspect the OS service manager without printing, for `vup doctor`.
pub fn service_health() -> ServiceHealth {
    match backend() {
        Ok(Backend::Systemd) => systemd_health(),
        Ok(Backend::Launchd) => launchd_health(),
        Err(_) => ServiceHealth {
            installed: false,
            active: false,
            detail: "unsupported platform (no systemd/launchd)".to_string(),
        },
    }
}

fn systemd_health() -> ServiceHealth {
    let installed = systemd_unit_path().map(|p| p.exists()).unwrap_or(false);
    if !installed {
        return ServiceHealth {
            installed: false,
            active: false,
            detail: "not installed".to_string(),
        };
    }
    let (_, enabled) = run("systemctl", &["--user", "is-enabled", "vup.service"]);
    let (active_ok, active) = run("systemctl", &["--user", "is-active", "vup.service"]);
    ServiceHealth {
        installed: true,
        active: active_ok && active == "active",
        detail: format!("{enabled}, {active}"),
    }
}

fn launchd_health() -> ServiceHealth {
    let installed = launchd_plist_path().map(|p| p.exists()).unwrap_or(false);
    if !installed {
        return ServiceHealth {
            installed: false,
            active: false,
            detail: "not installed".to_string(),
        };
    }
    let uid = launchd_uid();
    let target = format!("gui/{uid}/{LAUNCHD_LABEL}");
    let (ok, _out) = run("launchctl", &["print", &target]);
    ServiceHealth {
        installed: true,
        active: ok,
        detail: if ok { "loaded" } else { "not loaded" }.to_string(),
    }
}

// ---------------------------------------------------------------------------
// systemd (Linux, user unit)
// ---------------------------------------------------------------------------

fn systemd_unit_path() -> Result<PathBuf> {
    let config = dirs::config_dir().context("could not determine ~/.config")?;
    Ok(config.join("systemd").join("user").join("vup.service"))
}

fn systemd_unit(exe: &Path, config_path: &Path) -> String {
    format!(
        "# Generated by `vup service install` — re-run it after moving/updating vup.\n\
         [Unit]\n\
         Description=vup vault daemon (S5 node)\n\
         Documentation=https://docs.s5.pro\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart=\"{exe}\" _daemon --config \"{config}\"\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         # The daemon deadlines its own shutdown (staged-upload drain <=45s +\n\
         # teardown); give it a little more before SIGKILL.\n\
         TimeoutStopSec=70\n\
         NoNewPrivileges=true\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exe = exe.display(),
        config = config_path.display(),
    )
}

async fn systemd_install(config_path: &Path) -> Result<()> {
    let exe = exe_path()?;
    let unit_path = systemd_unit_path()?;
    std::fs::create_dir_all(unit_path.parent().expect("unit path has a parent"))?;
    std::fs::write(&unit_path, systemd_unit(&exe, config_path))
        .with_context(|| format!("writing {}", unit_path.display()))?;
    println!("✓ Wrote {}", unit_path.display());

    stop_loose_daemon().await;

    let (ok, out) = run("systemctl", &["--user", "daemon-reload"]);
    if !ok {
        bail!("systemctl --user daemon-reload failed: {out}");
    }
    let (ok, out) = run("systemctl", &["--user", "enable", "--now", "vup.service"]);
    if !ok {
        bail!("systemctl --user enable --now vup.service failed: {out}");
    }
    println!("✓ Service enabled and started (systemd user unit)");

    // Without linger the user manager — and vup with it — dies at logout
    // and only returns at next login. Best-effort: polkit may prompt/deny.
    let user = std::env::var("USER").unwrap_or_default();
    let (ok, _) = run("loginctl", &["enable-linger", &user]);
    if ok {
        println!("✓ Lingering enabled — vup starts at boot and survives logout");
    } else {
        println!(
            "⚠ Could not enable lingering; vup will run while you are logged in.\n\
             \x20 For boot-time start: loginctl enable-linger {user}"
        );
    }

    println!();
    println!("  Status:    vup service status   (or: systemctl --user status vup)");
    println!("  Uninstall: vup service uninstall");
    Ok(())
}

fn systemd_uninstall() -> Result<()> {
    let unit_path = systemd_unit_path()?;
    let (ok, out) = run("systemctl", &["--user", "disable", "--now", "vup.service"]);
    if !ok && unit_path.exists() {
        eprintln!("note: systemctl disable --now: {out}");
    }
    if unit_path.exists() {
        std::fs::remove_file(&unit_path)
            .with_context(|| format!("removing {}", unit_path.display()))?;
        let _ = run("systemctl", &["--user", "daemon-reload"]);
        println!("✓ Service stopped and removed ({})", unit_path.display());
    } else {
        println!(
            "Nothing to remove — no service installed at {}",
            unit_path.display()
        );
    }
    println!("  Reinstall anytime: vup service install");
    Ok(())
}

fn systemd_status() -> Result<()> {
    let unit_path = systemd_unit_path()?;
    if !unit_path.exists() {
        println!(
            "Not installed (no {}). Install: vup service install",
            unit_path.display()
        );
        return Ok(());
    }
    let (_, enabled) = run("systemctl", &["--user", "is-enabled", "vup.service"]);
    let (_, active) = run("systemctl", &["--user", "is-active", "vup.service"]);
    println!("Installed: {}", unit_path.display());
    println!("Enabled:   {enabled}");
    println!("Active:    {active}");
    Ok(())
}

// ---------------------------------------------------------------------------
// launchd (macOS, LaunchAgent)
// ---------------------------------------------------------------------------

const LAUNCHD_LABEL: &str = "pro.s5.vup";

fn launchd_plist_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("could not determine home directory")?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{LAUNCHD_LABEL}.plist")))
}

fn launchd_plist(exe: &Path, config_path: &Path) -> String {
    let stderr_log = dirs::cache_dir()
        .map(|d| d.join("s5").join("logs").join("daemon-spawn.log"))
        .unwrap_or_else(|| PathBuf::from("/tmp/vup-daemon-spawn.log"));
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Generated by `vup service install` - re-run it after moving/updating vup. -->
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LAUNCHD_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>_daemon</string>
        <string>--config</string>
        <string>{config}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardErrorPath</key>
    <string>{stderr}</string>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
"#,
        exe = exe.display(),
        config = config_path.display(),
        stderr = stderr_log.display(),
    )
}

fn launchd_uid() -> String {
    let (_, uid) = run("id", &["-u"]);
    uid
}

async fn launchd_install(config_path: &Path) -> Result<()> {
    let exe = exe_path()?;
    let plist_path = launchd_plist_path()?;
    std::fs::create_dir_all(plist_path.parent().expect("plist path has a parent"))?;
    std::fs::write(&plist_path, launchd_plist(&exe, config_path))
        .with_context(|| format!("writing {}", plist_path.display()))?;
    println!("✓ Wrote {}", plist_path.display());

    stop_loose_daemon().await;

    let uid = launchd_uid();
    let domain = format!("gui/{uid}");
    // `bootstrap` is the modern verb; older macOS falls back to `load -w`.
    let plist = plist_path.to_string_lossy();
    let (ok, out) = run("launchctl", &["bootstrap", &domain, &plist]);
    if !ok {
        let (ok2, out2) = run("launchctl", &["load", "-w", &plist]);
        if !ok2 {
            bail!("launchctl bootstrap failed ({out}); load -w also failed ({out2})");
        }
    }
    println!("✓ Service loaded (LaunchAgent) — starts at login, restarts on failure");
    println!();
    println!("  Status:    vup service status");
    println!("  Uninstall: vup service uninstall");
    Ok(())
}

fn launchd_uninstall() -> Result<()> {
    let plist_path = launchd_plist_path()?;
    let uid = launchd_uid();
    let target = format!("gui/{uid}/{LAUNCHD_LABEL}");
    let (ok, out) = run("launchctl", &["bootout", &target]);
    if !ok && plist_path.exists() {
        let plist = plist_path.to_string_lossy().into_owned();
        let (ok2, _) = run("launchctl", &["unload", "-w", &plist]);
        if !ok2 {
            eprintln!("note: launchctl bootout: {out}");
        }
    }
    if plist_path.exists() {
        std::fs::remove_file(&plist_path)
            .with_context(|| format!("removing {}", plist_path.display()))?;
        println!("✓ Service unloaded and removed ({})", plist_path.display());
    } else {
        println!(
            "Nothing to remove — no LaunchAgent at {}",
            plist_path.display()
        );
    }
    println!("  Reinstall anytime: vup service install");
    Ok(())
}

fn launchd_status() -> Result<()> {
    let plist_path = launchd_plist_path()?;
    if !plist_path.exists() {
        println!(
            "Not installed (no {}). Install: vup service install",
            plist_path.display()
        );
        return Ok(());
    }
    let uid = launchd_uid();
    let target = format!("gui/{uid}/{LAUNCHD_LABEL}");
    let (ok, out) = run("launchctl", &["print", &target]);
    println!("Installed: {}", plist_path.display());
    if ok {
        // `launchctl print` is verbose; surface just the state line.
        let state = out
            .lines()
            .find(|l| l.trim_start().starts_with("state ="))
            .map(|l| l.trim().to_string())
            .unwrap_or_else(|| "state = (loaded)".to_string());
        println!("Loaded:    yes ({state})");
    } else {
        println!("Loaded:    no — load with: vup service install");
    }
    Ok(())
}

/// Onboarding hook: offer the always-on install, and print the
/// install/uninstall commands either way so the choice is reversible from
/// memory. Never fails onboarding — a service problem is reported and
/// skipped (the session daemon keeps everything working).
pub async fn offer_install_during_onboarding(config_path: &Path) {
    println!("vup works best with its daemon permanently alive on every device:");
    println!("scheduled + watch-mode backups run unattended, and the daemon starts");
    println!("with the system.");
    match crate::interact::confirm(
        "Install vup as an always-on background service now (recommended)",
        true,
    ) {
        Ok(true) => {
            if let Err(e) = run_service(&ServiceCmd::Install, config_path).await {
                eprintln!("⚠ Service install failed (onboarding continues): {e:#}");
                eprintln!("  Retry later with: vup service install");
            }
        }
        Ok(false) => {
            println!("Skipped. Any vup command still auto-starts a session daemon.");
            println!("  Enable later:  vup service install");
            println!("  (and undo:     vup service uninstall)");
        }
        // Non-interactive onboard (scripted): don't block, just point at it.
        Err(_) => {
            println!("  Always-on service: vup service install  /  vup service uninstall");
        }
    }
}
