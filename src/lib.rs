#![deny(clippy::all)]

pub mod data_frame;
pub mod session_context;

pub use data_frame::SedonaDataFrame;
pub use session_context::{ContextBuilder, QueryResult, SessionContext};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[allow(dead_code)]
fn configure_tg_allocator() {
  use libmimalloc_sys::{mi_free, mi_malloc, mi_realloc};
  use sedona_tg::tg::set_allocator;

  // Configure tg to use mimalloc
  unsafe { set_allocator(mi_malloc, mi_realloc, mi_free) }.expect("Failed to set tg allocator");
}
