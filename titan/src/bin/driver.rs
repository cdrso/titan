//! Titan driver daemon.
//!
//! This binary runs the driver as a systemd-compatible service.
//! It accepts client connections via shared memory IPC and routes
//! data over UDP.
//!
//! # Usage
//!
//! ```sh
//! titan-driver --bind 0.0.0.0:9000 --endpoint 192.168.1.100:9000
//! ```
//!
//! # Signals
//!
//! - `SIGTERM` / `SIGINT`: Graceful shutdown

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use titan::ipc::shmem::ShmPath;
use titan::net::Endpoint;
use titan::runtime::driver::{Driver, DriverConfig, DriverError};

/// Default bind address.
const DEFAULT_BIND: &str = "0.0.0.0:9000";

/// Default session timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Global flag for signal handling.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() {
    if let Err(e) = run() {
        eprintln!("titan-driver: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), DriverError> {
    // Parse command line arguments (simple parsing for now)
    let args: Vec<String> = std::env::args().collect();
    let config = parse_args(&args)?;

    eprintln!(
        "titan-driver: starting on {:?} with {} endpoint(s)",
        config.bind_addr,
        config.endpoints.len()
    );

    // Spawn the driver
    let driver = Driver::spawn(config)?;

    eprintln!("titan-driver: ready");

    // Get shutdown flag for signal handler
    let shutdown_flag = driver.shutdown_flag();

    // Set up signal handling using platform-specific code
    setup_signal_handlers();

    // Wait for shutdown signal
    while !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!("\ntitan-driver: received shutdown signal");

    // Signal the driver to shut down
    shutdown_flag.store(true, Ordering::Relaxed);

    // Graceful shutdown
    eprintln!("titan-driver: shutting down...");
    driver.shutdown();
    eprintln!("titan-driver: stopped");

    Ok(())
}

/// Sets up signal handlers for graceful shutdown.
fn setup_signal_handlers() {
    // Spawn a thread that uses platform APIs to wait for signals
    std::thread::Builder::new()
        .name("signal-handler".into())
        .spawn(|| {
            wait_for_signal();
            SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
        })
        .expect("failed to spawn signal handler thread");
}

/// Waits for SIGTERM or SIGINT using platform-specific APIs.
///
/// Note: For proper signal handling, consider adding the `signal-hook` crate.
/// For now, this just blocks - Ctrl+C will terminate the process, and the
/// Drop impl on Driver will signal shutdown to threads.
#[cfg(unix)]
fn wait_for_signal() {
    // Block forever - signals will cause process termination
    // The Driver's Drop impl will signal shutdown to threads
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

#[cfg(not(unix))]
fn wait_for_signal() {
    // On non-Unix, just block forever
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Parses command line arguments into a DriverConfig.
fn parse_args(args: &[String]) -> Result<DriverConfig, DriverError> {
    let mut bind_addr: Option<SocketAddr> = None;
    let mut endpoints: Vec<Endpoint> = Vec::new();
    let mut timeout_secs = DEFAULT_TIMEOUT_SECS;
    let mut inbox_path: Option<ShmPath> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" | "-b" => {
                i += 1;
                if i >= args.len() {
                    return Err(DriverError::Bind(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "missing value for --bind",
                    )));
                }
                bind_addr = Some(args[i].parse().map_err(|e| {
                    DriverError::Bind(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
                })?);
            }
            "--endpoint" | "-e" => {
                i += 1;
                if i >= args.len() {
                    return Err(DriverError::Bind(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "missing value for --endpoint",
                    )));
                }
                let addr: SocketAddr = args[i].parse().map_err(|e| {
                    DriverError::Bind(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
                })?;
                endpoints.push(Endpoint::from(addr));
            }
            "--timeout" | "-t" => {
                i += 1;
                if i >= args.len() {
                    return Err(DriverError::Bind(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "missing value for --timeout",
                    )));
                }
                timeout_secs = args[i].parse().map_err(|e| {
                    DriverError::Bind(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
                })?;
            }
            "--inbox" | "-i" => {
                i += 1;
                if i >= args.len() {
                    return Err(DriverError::Bind(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "missing value for --inbox",
                    )));
                }
                inbox_path = Some(ShmPath::new(&args[i]).map_err(|e| {
                    DriverError::Bind(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
                })?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            arg => {
                return Err(DriverError::Bind(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                )));
            }
        }
        i += 1;
    }

    // Use defaults if not specified
    let bind_addr = bind_addr.unwrap_or_else(|| DEFAULT_BIND.parse().unwrap());

    Ok(DriverConfig {
        bind_addr: Endpoint::from(bind_addr),
        endpoints,
        session_timeout: Duration::from_secs(timeout_secs),
        inbox_path,
        cpu_config: titan::runtime::topology::CpuConfig::Auto,
    })
}

fn print_usage() {
    eprintln!(
        r#"titan-driver - Titan network driver daemon

USAGE:
    titan-driver [OPTIONS]

OPTIONS:
    -b, --bind <ADDR>       Bind address (default: 0.0.0.0:9000)
    -e, --endpoint <ADDR>   Add a network endpoint (can be repeated)
    -t, --timeout <SECS>    Session timeout in seconds (default: 30)
    -i, --inbox <PATH>      Inbox path for client connections (default: /titan-driver-inbox)
    -h, --help              Print this help message

SIGNALS:
    SIGTERM, SIGINT         Graceful shutdown (requires signal-hook in future)

EXAMPLE:
    titan-driver --bind 0.0.0.0:9000 --endpoint 192.168.1.100:9000
    titan-driver --inbox /titan-d1-inbox --bind 0.0.0.0:9001
"#
    );
}
