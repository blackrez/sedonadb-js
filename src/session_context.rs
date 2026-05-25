use napi_derive::napi;
use napi::Result;

use crate::data_frame::SedonaDataFrame;

/// A SedonaDB session context — the entry point for all spatial data operations.
///
/// Wraps SedonaContext (which itself extends DataFusion's SessionContext) with
/// all spatial functions, GeoParquet support, and object store configuration.
#[napi(object)]
#[derive(Clone)]
pub struct QueryResult {
    /// Column names in result order
    pub columns: Vec<String>,
    /// Row-oriented data — each inner vec is one row's values as strings
    pub rows: Vec<Vec<String>>,
    /// Number of rows returned
    pub num_rows: i64,
}

/// A builder for constructing SedonaDB session contexts with custom
/// runtime configuration (memory limits, spill directories, pool type).
///
/// The builder defaults to an interactive context (local filesystem access,
/// all spatial functions registered). Use `.localInteractive(false)` for
/// a lightweight in-memory-only context.
#[napi]
pub struct ContextBuilder {
    memory_limit: Option<String>,
    temp_dir: Option<String>,
    pool_type: Option<String>,
    unspillable_reserve_ratio: Option<f64>,
    interactive: bool,
}

#[napi]
impl ContextBuilder {
    /// Create a new builder with default settings.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            memory_limit: None,
            temp_dir: None,
            pool_type: None,
            unspillable_reserve_ratio: None,
            interactive: true,
        }
    }

    /// Set the memory limit as a human-readable string (e.g. "4gb", "512m", "1.5g")
    /// or a plain byte count. Pass "unlimited" to use the default (75% of RAM).
    #[napi]
    pub fn memory_limit(&self, limit: String) -> Result<Self> {
        let mut copy = Self::default_for_copy();
        copy.memory_limit = Some(limit);
        copy.temp_dir = self.temp_dir.clone();
        copy.pool_type = self.pool_type.clone();
        copy.unspillable_reserve_ratio = self.unspillable_reserve_ratio;
        copy.interactive = self.interactive;
        Ok(copy)
    }

    /// Use the default memory limit (75% of total physical memory).
    #[napi]
    pub fn without_memory_limit(&self) -> Self {
        let mut copy = Self::default_for_copy();
        copy.memory_limit = Some("unlimited".to_string());
        copy.temp_dir = self.temp_dir.clone();
        copy.pool_type = self.pool_type.clone();
        copy.unspillable_reserve_ratio = self.unspillable_reserve_ratio;
        copy.interactive = self.interactive;
        copy
    }

    /// Set the directory for temporary/spill files.
    #[napi]
    pub fn temp_dir(&self, dir: String) -> Self {
        let mut copy = Self::default_for_copy();
        copy.memory_limit = self.memory_limit.clone();
        copy.temp_dir = Some(dir);
        copy.pool_type = self.pool_type.clone();
        copy.unspillable_reserve_ratio = self.unspillable_reserve_ratio;
        copy.interactive = self.interactive;
        copy
    }

    /// Set the memory pool type: "fair" or "greedy".
    #[napi]
    pub fn pool_type(&self, pool_type: String) -> Result<Self> {
        let mut copy = Self::default_for_copy();
        copy.memory_limit = self.memory_limit.clone();
        copy.temp_dir = self.temp_dir.clone();
        copy.pool_type = Some(pool_type);
        copy.unspillable_reserve_ratio = self.unspillable_reserve_ratio;
        copy.interactive = self.interactive;
        Ok(copy)
    }

    /// Set the fraction of memory reserved for unspillable consumers (0.0 to 1.0).
    #[napi]
    pub fn unspillable_reserve_ratio(&self, ratio: f64) -> Result<Self> {
        if !(0.0..=1.0).contains(&ratio) {
            return Err(napi::Error::from_reason(
                "unspillable_reserve_ratio must be between 0.0 and 1.0".to_string(),
            ));
        }
        let mut copy = Self::default_for_copy();
        copy.memory_limit = self.memory_limit.clone();
        copy.temp_dir = self.temp_dir.clone();
        copy.pool_type = self.pool_type.clone();
        copy.unspillable_reserve_ratio = Some(ratio);
        copy.interactive = self.interactive;
        Ok(copy)
    }

    /// Set whether to create an interactive context with local filesystem access.
    #[napi]
    pub fn local_interactive(&self, interactive: bool) -> Self {
        let mut copy = Self::default_for_copy();
        copy.memory_limit = self.memory_limit.clone();
        copy.temp_dir = self.temp_dir.clone();
        copy.pool_type = self.pool_type.clone();
        copy.unspillable_reserve_ratio = self.unspillable_reserve_ratio;
        copy.interactive = interactive;
        copy
    }

    /// Build the session context from the current configuration.
    #[napi]
    pub async fn build(&self) -> Result<SessionContext> {
        if self.interactive {
            let ctx = build_interactive_context(
                &self.memory_limit,
                &self.temp_dir,
                &self.pool_type,
                self.unspillable_reserve_ratio,
            )
            .await?;
            Ok(SessionContext { inner: ctx })
        } else {
            let ctx = sedona::context::SedonaContext::new();
            Ok(SessionContext { inner: ctx })
        }
    }
}

// Non-napi helpers for ContextBuilder
impl ContextBuilder {
    pub(crate) fn default_for_copy() -> Self {
        Self {
            memory_limit: None,
            temp_dir: None,
            pool_type: None,
            unspillable_reserve_ratio: None,
            interactive: true,
        }
    }
}

// Free function: build an interactive context from builder options
async fn build_interactive_context(
    memory_limit: &Option<String>,
    temp_dir: &Option<String>,
    pool_type: &Option<String>,
    unspillable_reserve_ratio: Option<f64>,
) -> Result<sedona::context::SedonaContext> {
    use sedona::context_builder::SedonaContextBuilder;
    use sedona::pool_type::PoolType;

    let mut builder = SedonaContextBuilder::new();

    if let Some(ref limit) = memory_limit {
        if !limit.eq_ignore_ascii_case("unlimited") {
            let bytes = sedona::size_parser::parse_size_string(limit)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            builder = builder.with_memory_limit(bytes);
        }
    }

    if let Some(ref dir) = temp_dir {
        builder = builder.with_temp_dir(dir.clone());
    }

    if let Some(ref pt) = pool_type {
        let parsed: PoolType = pt
            .parse()
            .map_err(|e: String| napi::Error::from_reason(e))?;
        builder = builder.with_pool_type(parsed);
    }

    if let Some(ratio) = unspillable_reserve_ratio {
        builder = builder
            .with_unspillable_reserve_ratio(ratio)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    }

    builder
        .build()
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))
}

#[napi]
pub struct SessionContext {
    pub(crate) inner: sedona::context::SedonaContext,
}

#[napi]
impl SessionContext {
    /// Create a new interactive session with default settings.
    ///
    /// Equivalent to `new ContextBuilder().build()` — uses 75% of physical
    /// memory, fair pool, local filesystem access, and all spatial functions.
    #[napi(factory)]
    pub async fn new() -> Result<Self> {
        ContextBuilder::default_for_copy().build().await
    }

    /// Create a new local interactive session with default spatial functions
    /// and local filesystem access.
    ///
    /// This is equivalent to `SessionContext.new()`.
    #[napi(factory)]
    pub async fn new_local_interactive() -> Result<Self> {
        Self::new().await
    }

    /// Execute a SQL query and return the results as a structured object.
    #[napi]
    pub async fn sql(&self, query: String) -> Result<QueryResult> {
        let df = self
            .inner
            .sql(&query)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        let columns = if batches.is_empty() {
            vec![]
        } else {
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };

        let mut rows = Vec::new();
        for batch in &batches {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();
            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
                    let array = batch.column(col_idx);
                    if array.is_null(row_idx) {
                        row.push(String::new());
                    } else {
                        let s = arrow::util::display::array_value_to_string(array.as_ref(), row_idx)
                            .unwrap_or_else(|_| String::new());
                        row.push(s);
                    }
                }
                rows.push(row);
            }
        }

        let num_rows = rows.len() as i64;

        Ok(QueryResult {
            columns,
            rows,
            num_rows,
        })
    }

    /// Register a DataFrame as a temporary view for SQL queries.
    #[napi]
    pub fn register_table(&self, name: String, df: &SedonaDataFrame) -> Result<()> {
        let provider = df.inner.clone().into_temporary_view();
        self.inner
            .ctx
            .register_table(name, provider)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(())
    }

    /// Read a GeoParquet file and return a DataFrame for further processing.
    #[napi]
    pub async fn read_parquet(&self, path: String) -> Result<SedonaDataFrame> {
        let df = self
            .inner
            .read_parquet(&path, Default::default())
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(SedonaDataFrame { inner: df })
    }
}
