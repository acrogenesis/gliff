//! Spawn `gliff-server --stdio` over ssh and expose its stdio as a stream.

use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// How to reach the server host over ssh.
#[derive(Debug, Clone)]
pub struct SshTarget {
    /// `user@host` or an ssh config alias.
    pub host: String,
    /// Remote `gliff-server` path (default: `gliff-server` on PATH).
    pub server_bin: String,
    /// Extra args passed to the server after `--stdio`.
    pub server_args: Vec<String>,
    /// Extra ssh options (e.g. `-J jump`).
    pub ssh_args: Vec<String>,
    /// Optional native GUI prompt helper (macOS); terminal clients leave unset.
    pub askpass: Option<std::path::PathBuf>,
}

impl SshTarget {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            server_bin: "gliff-server".into(),
            server_args: Vec::new(),
            ssh_args: Vec::new(),
            askpass: None,
        }
    }
}

/// The spawned ssh process and its piped stdio, joined into one duplex stream.
pub struct SshStream {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

/// Spawn ssh. Compression is off (video is already compressed), the socket
/// asks for low-latency queuing, and unknown host keys are accepted unless
/// ssh config says otherwise. stderr is inherited so server logs reach the
/// client's terminal.
pub fn spawn_ssh(target: &SshTarget) -> std::io::Result<SshStream> {
    let mut cmd = ssh_command(target);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::inherit());
    cmd.kill_on_drop(true);
    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    Ok(SshStream {
        child,
        stdin,
        stdout,
    })
}

fn ssh_command(target: &SshTarget) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.arg("-T")
        .arg("-o")
        .arg("Compression=no")
        .arg("-o")
        .arg("IPQoS=lowdelay")
        .args(&target.ssh_args);
    // No terminal to answer the host-key prompt: trust a host on first
    // connect, as typing "yes" would. A changed key still fails. Only when
    // the user's ssh config leaves the default, since -o would override it.
    if host_key_checking_is_default(target) {
        cmd.arg("-o").arg("StrictHostKeyChecking=accept-new");
    }
    cmd.arg("--").arg(&target.host).arg(remote_command(target));
    if let Some(path) = &target.askpass {
        cmd.env("SSH_ASKPASS", path)
            .env("SSH_ASKPASS_REQUIRE", "force")
            .env("GLIFF_ASKPASS", "1");
    }
    cmd
}

/// OpenSSH joins remote argv through the remote user's shell. Keep custom
/// server commands (including `env ...`) intact, but quote each extra argument.
fn remote_command(target: &SshTarget) -> String {
    let args = std::iter::once("--stdio".to_string())
        .chain(target.server_args.iter().map(|s| shell_quote(s)))
        .collect::<Vec<_>>()
        .join(" ");
    if target.server_bin == "gliff-server" {
        // The login shell may be fish or csh. Only a simple exec reaches it;
        // the fallback's POSIX syntax is parsed by /bin/sh explicitly.
        let script = "if command -v gliff-server >/dev/null 2>&1; then exec gliff-server \"$@\"; elif [ -x \"$HOME/Applications/Gliff.app/Contents/MacOS/gliff-server\" ]; then exec \"$HOME/Applications/Gliff.app/Contents/MacOS/gliff-server\" \"$@\"; else exec \"$HOME/.local/bin/gliff-server\" \"$@\"; fi";
        format!(
            "exec /bin/sh -c {} gliff-server {args}",
            shell_quote(script)
        )
    } else {
        format!("{} {args}", target.server_bin)
    }
}

fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "'\\''"))
}

/// Whether ssh's effective StrictHostKeyChecking for this host is the default
/// `ask`, i.e. neither ssh config nor ssh_args set it.
fn host_key_checking_is_default(target: &SshTarget) -> bool {
    let Ok(out) = std::process::Command::new("ssh")
        .arg("-G")
        .args(&target.ssh_args)
        .arg("--")
        .arg(&target.host)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
    else {
        return false;
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|l| l == "stricthostkeychecking ask")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_cannot_supply_ssh_options() {
        let target = SshTarget::new("-oProxyCommand=touch /tmp/unwanted");
        let cmd = ssh_command(&target);
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_str().unwrap())
            .collect();
        let host = args.iter().position(|a| *a == target.host).unwrap();
        assert_eq!(args[host - 1], "--");
    }

    #[test]
    fn target_cannot_add_options_and_gui_prompts_are_opt_in() {
        let mut target = SshTarget::new("-oProxyCommand=untrusted");
        target.ssh_args = vec!["-p".into(), "2222".into()];
        let cmd = ssh_command(&target);
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_str().unwrap())
            .collect();
        assert!(args
            .windows(2)
            .any(|a| a == ["--", "-oProxyCommand=untrusted"]));
        assert!(args.windows(2).any(|a| a == ["-p", "2222"]));
        assert_eq!(cmd.as_std().get_envs().count(), 0);
        target.askpass = Some("/native/helper".into());
        let cmd = ssh_command(&target);
        let vars: std::collections::BTreeMap<_, _> = cmd
            .as_std()
            .get_envs()
            .map(|(k, v)| (k.to_str().unwrap(), v.unwrap().to_str().unwrap()))
            .collect();
        assert_eq!(vars["SSH_ASKPASS"], "/native/helper");
        assert_eq!(vars["SSH_ASKPASS_REQUIRE"], "force");
        assert_eq!(vars["GLIFF_ASKPASS"], "1");
    }

    #[test]
    fn remote_arguments_are_literal_and_custom_prefixes_work() {
        let mut t = SshTarget::new("host");
        t.server_bin = "env GLIFF_VIDEO=cpu /path/to/gliff-server".into();
        t.server_args = vec!["--output".into(), "a'b; $(touch /tmp/no)".into()];
        assert_eq!(remote_command(&t), "env GLIFF_VIDEO=cpu /path/to/gliff-server --stdio '--output' 'a'\\''b; $(touch /tmp/no)'");
    }

    #[test]
    fn default_finds_native_app_when_ssh_path_is_minimal() {
        let command = remote_command(&SshTarget::new("host"));
        assert!(command.contains("command -v gliff-server"));
        assert!(command.contains("$HOME/Applications/Gliff.app/Contents/MacOS/gliff-server"));
        assert!(command.contains("$HOME/.local/bin/gliff-server"));
    }

    #[cfg(unix)]
    #[test]
    fn default_fallback_executes_in_login_shells_with_literal_arguments() {
        use std::os::unix::fs::PermissionsExt;
        let home = tempfile::tempdir().unwrap();
        let bin = home.path().join("bin");
        let bundle = home.path().join("Applications/Gliff.app/Contents/MacOS");
        let local = home.path().join(".local/bin");
        for dir in [&bin, &bundle, &local] {
            std::fs::create_dir_all(dir).unwrap();
            let server = dir.join("gliff-server");
            std::fs::write(&server, "#!/bin/sh\nprintf '%s\\0' \"$@\"\n").unwrap();
            std::fs::set_permissions(server, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut target = SshTarget::new("host");
        target.server_args = vec!["--output".into(), "a'b; $(touch /tmp/no) \\\" $HOME".into()];
        let expected = format!("--stdio\0--output\0{}\0", target.server_args[1]);
        for stage in 0..3 {
            if stage == 1 {
                std::fs::remove_file(bin.join("gliff-server")).unwrap();
            }
            if stage == 2 {
                std::fs::remove_file(bundle.join("gliff-server")).unwrap();
            }
            for shell in [
                "/bin/sh",
                "/bin/bash",
                "/bin/zsh",
                "/usr/bin/fish",
                "/bin/csh",
            ] {
                if !std::path::Path::new(shell).exists() {
                    continue;
                }
                let output = std::process::Command::new(shell)
                    .args(["-c", &remote_command(&target)])
                    .env("HOME", home.path())
                    .env("PATH", &bin)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{shell} fallback {stage}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(
                    output.stdout,
                    expected.as_bytes(),
                    "{shell} fallback {stage}"
                );
            }
        }
    }
}
