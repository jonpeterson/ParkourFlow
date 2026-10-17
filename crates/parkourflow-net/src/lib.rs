//! Network nodes for ParkourFlow, built on libpcap (Npcap on Windows).
//!
//! The node types' descriptors live in `parkourflow_core::builtin_nodes`;
//! this crate provides the code they run and lists capture interfaces. Call
//! [`register`] to add it to a [`NodeRegistry`].

pub mod interface;

use parkourflow_core::NodeRegistry;

pub use interface::{list_interfaces, InterfaceSource};

/// Registers this crate's runnable node types and device lists.
pub fn register(registry: &mut NodeRegistry) {
    registry
        .register(interface::SOURCE_TYPE, InterfaceSource::build)
        .register_devices(interface::DEVICE_CLASS, list_interfaces);
}
