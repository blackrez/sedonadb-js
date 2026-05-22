#![deny(clippy::all)]

use datafusion::prelude::col;
use napi_derive::napi;
use napi::{Error, Result}; // Import napi error types
use sedona::context::{SedonaContext, SedonaDataFrame};


pub mod data_frame;
pub mod session_context;

#[napi]
pub async fn demo(_sql: String) -> Result<String> {
    // Convert any DataFusionError to napi::Error
    let ctx = SedonaContext::new_local_interactive()
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?;

    let url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet";
    let df = ctx
        .read_parquet(url, Default::default())
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?;

    let output = df
        .sort_by(vec![col("name")])
        .map_err(|e| Error::from_reason(e.to_string()))?
        .show_sedona(&ctx, Some(5), Default::default())
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?;

    // Remove unnecessary parentheses
    Ok(output.into())
}
