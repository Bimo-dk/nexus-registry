pub mod entities;
pub mod sqlite;

pub use entities::{
    delete_gate, delete_host, get_gate, get_gate_by_domain, get_host, host_exists, insert_gate, insert_host,
    list_gates, list_hosts, toggle_gate, toggle_host, update_gate, update_host, DeleteHostOutcome,
};
pub use sqlite::{delete, get, init, insert, list, list_for_host, toggle, update, Db, StoreError};
