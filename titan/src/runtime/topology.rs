//! CPU topology detection and thread placement for driver threads.
//!
//! This module detects the CPU topology at runtime and determines optimal
//! placement for the driver's three threads (RX, TX, Control) to minimize
//! cache contention and maximize throughput.
//!
//! # Placement Strategy
//!
//! - **RX and TX** should be on separate physical cores (not SMT siblings)
//!   to avoid L1/L2 cache thrashing. They share L3 which is beneficial for
//!   the RxToTxEvent queue.
//! - **Control** is cold path (1ms sleeps), so it can share a physical core
//!   with TX via SMT, or run on a dedicated core if available.
//!
//! # Detection
//!
//! Uses `num_cpus` for physical/logical core counts and `core_affinity` for
//! pinning. On most systems, core IDs 0..N map to separate physical cores
//! before SMT siblings are enumerated.

use core_affinity::CoreId;

/// CPU topology information detected at runtime.
#[derive(Debug, Clone)]
pub struct CpuTopology {
    /// Total logical cores (including SMT/hyperthreads).
    pub logical_cores: usize,
    /// Total physical cores.
    pub physical_cores: usize,
    /// Whether SMT (hyperthreading) is enabled.
    pub has_smt: bool,
    /// Available core IDs for pinning.
    pub available_cores: Vec<usize>,
}

impl CpuTopology {
    /// Detects the CPU topology of the current system.
    #[must_use]
    pub fn detect() -> Self {
        let logical_cores = num_cpus::get();
        let physical_cores = num_cpus::get_physical();
        let has_smt = logical_cores > physical_cores;

        // Get available core IDs from core_affinity
        let available_cores = core_affinity::get_core_ids()
            .map(|ids| ids.into_iter().map(|id| id.id).collect())
            .unwrap_or_else(|| (0..logical_cores).collect());

        Self {
            logical_cores,
            physical_cores,
            has_smt,
            available_cores,
        }
    }

    /// Selects optimal thread placement based on detected topology.
    #[must_use]
    pub fn select_placement(&self) -> ThreadPlacement {
        let physical = self.physical_cores;
        let available = &self.available_cores;

        // Determine strategy based on available physical cores
        let strategy = if physical >= 3 {
            PlacementStrategy::Dedicated3
        } else if physical == 2 && self.has_smt {
            PlacementStrategy::SmtShare
        } else if physical == 2 {
            PlacementStrategy::UnpinnedControl
        } else if physical == 1 && self.has_smt {
            PlacementStrategy::SingleCoreSmt
        } else {
            PlacementStrategy::NoPin
        };

        // Select cores based on strategy
        // Heuristic: On most systems, lower core IDs are separate physical cores
        let (rx_core, tx_core, control_core) = match strategy {
            PlacementStrategy::Dedicated3 => {
                // 3+ physical cores: each thread gets its own
                let rx = available.first().copied();
                let tx = available.get(1).copied();
                let ctrl = available.get(2).copied();
                (rx, tx, ctrl)
            }
            PlacementStrategy::SmtShare => {
                // 2 physical + SMT: RX dedicated, TX+Control share
                // Core 0 for RX, Core 1 for TX, Control unpinned (OS will use SMT)
                let rx = available.first().copied();
                let tx = available.get(1).copied();
                (rx, tx, None)
            }
            PlacementStrategy::UnpinnedControl => {
                // 2 physical, no SMT: RX and TX pinned, Control unpinned
                let rx = available.first().copied();
                let tx = available.get(1).copied();
                (rx, tx, None)
            }
            PlacementStrategy::SingleCoreSmt => {
                // 1 physical + SMT: RX on logical 0, TX on logical 1
                let rx = available.first().copied();
                let tx = available.get(1).copied();
                (rx, tx, None)
            }
            PlacementStrategy::NoPin => {
                // No pinning - let OS handle it
                (None, None, None)
            }
            PlacementStrategy::Manual => {
                // Manual is only set by CpuConfig::Manual, not by select_placement
                unreachable!("Manual strategy should not be returned by select_placement")
            }
        };

        ThreadPlacement {
            rx_core,
            tx_core,
            control_core,
            strategy,
        }
    }
}

/// Thread placement decisions for driver threads.
#[derive(Debug, Clone)]
pub struct ThreadPlacement {
    /// Core ID for RX thread (None = unpinned).
    pub rx_core: Option<usize>,
    /// Core ID for TX thread (None = unpinned).
    pub tx_core: Option<usize>,
    /// Core ID for Control thread (None = unpinned).
    pub control_core: Option<usize>,
    /// Strategy used for placement.
    pub strategy: PlacementStrategy,
}

impl ThreadPlacement {
    /// Creates a placement with all threads unpinned.
    #[must_use]
    pub fn unpinned() -> Self {
        Self {
            rx_core: None,
            tx_core: None,
            control_core: None,
            strategy: PlacementStrategy::NoPin,
        }
    }

    /// Creates a manual placement with explicit core assignments.
    #[must_use]
    pub fn manual(rx_core: Option<usize>, tx_core: Option<usize>, control_core: Option<usize>) -> Self {
        Self {
            rx_core,
            tx_core,
            control_core,
            strategy: PlacementStrategy::Manual,
        }
    }
}

/// Strategy used for thread placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementStrategy {
    /// 3+ physical cores: RX, TX, Control each on dedicated core.
    Dedicated3,
    /// 2 physical cores + SMT: RX dedicated, TX+Control share via SMT.
    SmtShare,
    /// 2 physical cores, no SMT: RX and TX pinned, Control unpinned.
    UnpinnedControl,
    /// 1 physical core + SMT: RX and TX on SMT siblings.
    SingleCoreSmt,
    /// No pinning (fallback).
    NoPin,
    /// Manual assignment by user.
    Manual,
}

impl std::fmt::Display for PlacementStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dedicated3 => write!(f, "dedicated-3"),
            Self::SmtShare => write!(f, "smt-share"),
            Self::UnpinnedControl => write!(f, "unpinned-control"),
            Self::SingleCoreSmt => write!(f, "single-core-smt"),
            Self::NoPin => write!(f, "no-pin"),
            Self::Manual => write!(f, "manual"),
        }
    }
}

/// Configuration for CPU pinning behavior.
#[derive(Debug, Clone, Default)]
pub enum CpuConfig {
    /// Auto-detect topology and choose optimal placement.
    #[default]
    Auto,
    /// User-specified core assignments.
    Manual {
        /// Core for RX thread (None = unpinned).
        rx_core: Option<usize>,
        /// Core for TX thread (None = unpinned).
        tx_core: Option<usize>,
        /// Core for Control thread (None = unpinned).
        control_core: Option<usize>,
    },
    /// Disable CPU pinning entirely.
    Disabled,
}

impl CpuConfig {
    /// Resolves the config to a concrete thread placement.
    #[must_use]
    pub fn resolve(&self) -> ThreadPlacement {
        match self {
            Self::Auto => {
                let topology = CpuTopology::detect();
                topology.select_placement()
            }
            Self::Manual { rx_core, tx_core, control_core } => {
                ThreadPlacement::manual(*rx_core, *tx_core, *control_core)
            }
            Self::Disabled => ThreadPlacement::unpinned(),
        }
    }
}

/// Pins the current thread to the specified core.
///
/// Returns `true` if pinning succeeded, `false` otherwise.
/// Pinning may fail if the core ID is invalid or the OS denies the request.
pub fn pin_to_core(core_id: usize) -> bool {
    let core = CoreId { id: core_id };
    core_affinity::set_for_current(core)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_detection_returns_valid_counts() {
        let topo = CpuTopology::detect();

        assert!(topo.logical_cores > 0, "should have at least 1 logical core");
        assert!(topo.physical_cores > 0, "should have at least 1 physical core");
        assert!(
            topo.logical_cores >= topo.physical_cores,
            "logical >= physical"
        );
        assert!(!topo.available_cores.is_empty(), "should have available cores");
    }

    #[test]
    fn topology_detects_smt_correctly() {
        let topo = CpuTopology::detect();

        if topo.logical_cores > topo.physical_cores {
            assert!(topo.has_smt, "should detect SMT when logical > physical");
        } else {
            assert!(!topo.has_smt, "should not detect SMT when logical == physical");
        }
    }

    #[test]
    fn placement_returns_valid_cores() {
        let topo = CpuTopology::detect();
        let placement = topo.select_placement();

        // If cores are assigned, they should be in the available set
        if let Some(rx) = placement.rx_core {
            assert!(
                topo.available_cores.contains(&rx),
                "rx_core should be in available set"
            );
        }
        if let Some(tx) = placement.tx_core {
            assert!(
                topo.available_cores.contains(&tx),
                "tx_core should be in available set"
            );
        }
        if let Some(ctrl) = placement.control_core {
            assert!(
                topo.available_cores.contains(&ctrl),
                "control_core should be in available set"
            );
        }
    }

    #[test]
    fn rx_and_tx_on_different_cores_when_possible() {
        let topo = CpuTopology::detect();
        let placement = topo.select_placement();

        if topo.physical_cores >= 2 {
            // With 2+ physical cores, RX and TX should be on different cores
            assert_ne!(
                placement.rx_core, placement.tx_core,
                "RX and TX should be on different cores"
            );
        }
    }

    #[test]
    fn cpu_config_auto_resolves() {
        let config = CpuConfig::Auto;
        let placement = config.resolve();

        // Should return some placement (strategy depends on system)
        assert!(
            !matches!(placement.strategy, PlacementStrategy::Manual),
            "auto should not return manual strategy"
        );
    }

    #[test]
    fn cpu_config_disabled_returns_unpinned() {
        let config = CpuConfig::Disabled;
        let placement = config.resolve();

        assert!(placement.rx_core.is_none());
        assert!(placement.tx_core.is_none());
        assert!(placement.control_core.is_none());
        assert_eq!(placement.strategy, PlacementStrategy::NoPin);
    }

    #[test]
    fn cpu_config_manual_uses_specified_cores() {
        let config = CpuConfig::Manual {
            rx_core: Some(5),
            tx_core: Some(6),
            control_core: Some(7),
        };
        let placement = config.resolve();

        assert_eq!(placement.rx_core, Some(5));
        assert_eq!(placement.tx_core, Some(6));
        assert_eq!(placement.control_core, Some(7));
        assert_eq!(placement.strategy, PlacementStrategy::Manual);
    }
}
