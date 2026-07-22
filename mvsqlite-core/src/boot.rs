use std::sync::OnceLock;

use foundationdb::{api::NetworkAutoStop, options::NetworkOption};

static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();

/// Boots the FDB client network exactly once per process. Must be called
/// before the first `Core::open`. Idempotent - subsequent calls are no-ops.
///
/// IMPORTANT: FoundationDB's C client does not support being used after
/// `fork()` once the network thread has started - a forked child inherits
/// this process's memory (including the fact that boot already ran) but not
/// the actual network thread, so FDB calls in the child will hang or fail
/// silently. If a process needs to both fork and use FDB, it must fork
/// before ever calling this function (or `Core::open`), so each child
/// performs its own first-time boot independently. There is no supported way
/// to "rebuild" the FDB connection after a fork that already touched FDB,
/// unlike a plain HTTP client.
pub fn boot() {
    boot_with_buggify(false);
}

/// Same as `boot()`, but optionally enables FDB client buggify (fault
/// injection) before booting the network. DO NOT USE IN PRODUCTION! Only the
/// first caller's `enable_buggify` value takes effect, per the same
/// once-per-process semantics as `boot()`.
pub fn boot_with_buggify(enable_buggify: bool) {
    NETWORK.get_or_init(|| {
        let mut network_builder = foundationdb::api::FdbApiBuilder::default()
            .build()
            .expect("fdb api initialization failed");
        if enable_buggify {
            network_builder = network_builder
                .set_option(NetworkOption::ClientBuggifyEnable)
                .unwrap()
                .set_option(NetworkOption::ClientBuggifySectionActivatedProbability(
                    100,
                ))
                .unwrap();
        }
        unsafe { network_builder.boot() }.expect("fdb network initialization failed")
    });
}
