use std::sync::Mutex;
use std::sync::Arc;
use std::num::NonZeroUsize;
use std::io::Cursor;
use lru::LruCache;
use napi_derive::napi;
use napi::Result;
use napi::bindgen_prelude::{ReadableStream, spawn, block_on};
use napi::Env;
use futures::StreamExt;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::DataFrame;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType;
use arrow::array::{ArrayRef, StringArray};

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
    /// Number of rows returned
    pub num_rows: i64,
    /// Row-oriented data — each inner vec is one row's values as native JS types.
    ///
    /// Values preserve their Arrow types:
    /// - numbers → JS number
    /// - strings → JS string
    /// - booleans → JS boolean
    /// - null   → JS null
    /// - complex types (geometries, lists, etc.) → JS string via Arrow Display
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Arrow IPC stream buffer (Apache Arrow streaming format).
    ///
    /// Contains the same data as a columnar binary representation, preserving
    /// full type fidelity including null bitmaps. Use with `@apache-arrow`:
    ///
    /// ```js
    /// import { tableFromIPC } from '@apache-arrow';
    /// const table = tableFromIPC(new Uint8Array(result.arrowIpc));
    /// ```
    pub arrow_ipc: Vec<u8>,
}

/// Decode an Arrow IPC buffer back to string-formatted rows.
///
/// This is a backward-compatibility helper — prefer using `@apache-arrow`
/// directly for type-safe access. The returned rows match the old
/// `QueryResult.rows` format (nulls → empty strings).
#[napi]
pub fn ipc_to_rows(arrow_ipc: Vec<u8>) -> Result<Vec<Vec<String>>> {
    if arrow_ipc.is_empty() {
        return Ok(Vec::new());
    }

    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(&arrow_ipc);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| napi::Error::from_reason(format!("Arrow IPC read: {e}")))?;

    let mut rows = Vec::new();
    while let Some(batch_result) = reader.next() {
        let batch = batch_result
            .map_err(|e| napi::Error::from_reason(format!("Arrow IPC batch: {e}")))?;
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

    Ok(rows)
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
            Ok(SessionContext {
                inner: ctx,
                plan_cache: Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())),
            })
        } else {
            let ctx = sedona::context::SedonaContext::new();
            Ok(SessionContext {
                inner: ctx,
                plan_cache: Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())),
            })
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

/// Convert a serde_json::Value (from JS) to a DataFusion ScalarValue.
///
/// Type inference:
/// - JS null        → Null
/// - JS boolean     → Boolean
/// - JS number      → Float64 (DataFusion will coerce to Int as needed)
/// - JS string      → Utf8
/// - JS array/object→ Null (unsupported)
fn json_value_to_scalar(v: &serde_json::Value) -> ScalarValue {
    match v {
        serde_json::Value::Null => ScalarValue::Null,
        serde_json::Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                ScalarValue::Float64(Some(f))
            } else {
                ScalarValue::Null
            }
        }
        serde_json::Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        _ => ScalarValue::Null,
    }
}

/// Build DataFusion ParamValues from a serde_json::Value.
///
/// - Array  → positional params (e.g. `$1`, `$2`)
/// - Object → named params (e.g. `$name`)
fn build_param_values(params: &serde_json::Value) -> Result<ParamValues> {
    match params {
        serde_json::Value::Array(arr) => {
            let scalars: Vec<ScalarValue> = arr.iter().map(json_value_to_scalar).collect();
            Ok(ParamValues::from(scalars))
        }
        serde_json::Value::Object(map) => {
            let pairs: Vec<(String, ScalarValue)> = map
                .iter()
                .map(|(k, v)| (k.clone(), json_value_to_scalar(v)))
                .collect();
            Ok(ParamValues::from(pairs))
        }
        _ => Err(napi::Error::from_reason(
            "params must be a JSON array (positional) or object (named)".to_string(),
        )),
    }
}

/// A pre-planned SQL statement with optional parameter bindings.
///
/// Created via [`SessionContext::prepare`]. Use [`execute`](#method.execute) to run
/// the same query repeatedly with different parameters without re-parsing the SQL.
///
/// # Performance
///
/// `prepare`+`execute` is significantly faster than calling `sql()` repeatedly with
/// the same query text because:
/// - SQL parsing + schema resolution happens once in `prepare`
/// - Each `execute` only does plan cloning + parameter substitution + execution
#[napi]
pub struct PreparedStatement {
    plan: LogicalPlan,
    state: SessionState,
}

#[napi]
impl PreparedStatement {
    /// Execute the prepared statement with the given parameter bindings.
    ///
    /// - An array for positional params (`$1`, `$2` in SQL)
    /// - An object for named params (`$name` in SQL)
    ///
    /// Value types are inferred from JS: number → Float64, string → Utf8,
    /// boolean → Boolean, null → Null.
    #[napi]
    pub async fn execute(&self, params: Option<serde_json::Value>) -> Result<QueryResult> {
        let plan = if let Some(ref p) = params {
            let param_values = build_param_values(p)?;
            self.plan
                .clone()
                .with_param_values(param_values)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?
        } else {
            self.plan.clone()
        };

        let state = self.state.clone();
        let df = DataFrame::new(state, plan);
        collect_to_query_result(df).await
    }
}

/// Serialize RecordBatches to an Arrow IPC stream buffer.
///
/// Uses Arrow's binary streaming format instead of stringifying every value,
/// which is significantly faster and preserves type/null information.
pub(crate) fn record_batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let buffer: Vec<u8> = Vec::new();
    let cursor = Cursor::new(buffer);

    let mut writer = StreamWriter::try_new(cursor, &schema)
        .map_err(|e| napi::Error::from_reason(format!("Arrow IPC writer init: {e}")))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| napi::Error::from_reason(format!("Arrow IPC write: {e}")))?;
    }

    let cursor = writer
        .into_inner()
        .map_err(|e| napi::Error::from_reason(format!("Arrow IPC finish: {e}")))?;

    Ok(cursor.into_inner())
}

/// Convert Binary / BinaryView / LargeBinary / FixedSizeBinary / Utf8View
/// columns in a RecordBatch to their string representations (Utf8), so they
/// can be processed by serde_arrow without "invalid type: byte array" errors.
///
/// A new schema is built to match the converted column types. Columns that
/// are not in the conversion list are left untouched.
pub(crate) fn binary_cols_to_strings(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let original_fields = schema.fields();
    let mut new_fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(batch.num_columns());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (i, field) in original_fields.iter().enumerate() {
        let dt = field.data_type();
        let needs_conversion = matches!(
            dt,
            DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::BinaryView
                | DataType::Utf8View
        );

        if needs_conversion {
            let array = batch.column(i);
            let strings: Vec<String> = (0..array.len())
                .map(|j| {
                    if array.is_null(j) {
                        String::new()
                    } else {
                        arrow::util::display::array_value_to_string(array.as_ref(), j)
                            .unwrap_or_default()
                    }
                })
                .collect();
            new_fields.push(arrow::datatypes::Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            ));
            new_columns.push(Arc::new(StringArray::from(strings)) as ArrayRef);
        } else {
            new_fields.push((*field.as_ref()).clone());
            new_columns.push(batch.column(i).clone());
        }
    }

    let new_schema = arrow::datatypes::Schema::new(new_fields);
    RecordBatch::try_new(Arc::new(new_schema), new_columns)
        .map_err(|e| napi::Error::from_reason(format!("rebuild batch: {e}")))
}

/// Execute a DataFrame, collect all batches, and return as a [`QueryResult`]
/// with typed JS rows and an Arrow IPC stream buffer.
async fn collect_to_query_result(df: DataFrame) -> Result<QueryResult> {
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

    let num_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    // Build Arrow IPC buffer for @apache-arrow users
    let arrow_ipc = record_batches_to_ipc(&batches)?;

    // Build native JS-typed rows using serde_arrow.
    // Binary columns (geometry WKB) are converted to strings first.
    let mut rows = Vec::with_capacity(num_rows as usize);
    for batch in &batches {
        let safe = binary_cols_to_strings(batch)?;
        let items: Vec<serde_json::Value> = serde_arrow::from_record_batch(&safe)
            .map_err(|e| napi::Error::from_reason(format!("serde_arrow: {e}")))?;

        for item in items {
            if let serde_json::Value::Object(map) = item {
                let row: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|c| map.get(c).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                rows.push(row);
            } else {
                // Single-value result (unexpected for a multi-column query)
                rows.push(vec![item]);
            }
        }
    }

    Ok(QueryResult {
        columns,
        num_rows,
        rows,
        arrow_ipc,
    })
}

/// Execute a DataFrame and return only the row-oriented results (no Arrow IPC).
/// Avoids the cost of Arrow IPC serialization.
async fn collect_to_rows_only(df: DataFrame) -> Result<QueryResult> {
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

    let num_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    // Build native JS-typed rows using serde_arrow.
    // Binary columns (geometry WKB) are converted to strings first.
    let mut rows = Vec::with_capacity(num_rows as usize);
    for batch in &batches {
        let safe = binary_cols_to_strings(batch)?;
        let items: Vec<serde_json::Value> = serde_arrow::from_record_batch(&safe)
            .map_err(|e| napi::Error::from_reason(format!("serde_arrow: {e}")))?;

        for item in items {
            if let serde_json::Value::Object(map) = item {
                let row: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|c| map.get(c).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                rows.push(row);
            } else {
                // Single-value result (unexpected for a multi-column query)
                rows.push(vec![item]);
            }
        }
    }

    Ok(QueryResult {
        columns,
        num_rows,
        rows,
        arrow_ipc: vec![],
    })
}

/// Execute a DataFrame and return only the Arrow IPC stream buffer (no row conversion).
/// Avoids the cost of serde_arrow row serialization.
/// Returns the raw IPC bytes — use with `@apache-arrow`'s `tableFromIPC()`.
async fn collect_to_arrow_only(df: DataFrame) -> Result<Vec<u8>> {
    let batches = df
        .collect()
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    record_batches_to_ipc(&batches)
}

#[napi]
pub struct SessionContext {
    pub(crate) inner: sedona::context::SedonaContext,
    pub(crate) plan_cache: Mutex<LruCache<String, LogicalPlan>>,
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
    ///
    /// Optionally accepts parameter bindings:
    /// - An array for positional params (`$1`, `$2` in SQL)
    /// - An object for named params (`$name` in SQL)
    ///
    /// Value types are inferred from JS: number → Float64, string → Utf8,
    /// boolean → Boolean, null → Null.
    ///
    /// # Performance
    ///
    /// When called repeatedly with the same SQL but different params, the plan
    /// is cached internally (LRU, 100 entries) so SQL parsing + schema resolution
    /// are skipped on cache hits. For maximal control, use [`prepare`](#method.prepare)
    /// + [`PreparedStatement::execute`].
    #[napi]
    pub async fn sql(&self, query: String, params: Option<serde_json::Value>) -> Result<QueryResult> {
        if params.is_some() {
            // Try the LRU plan cache for parameterized queries
            let plan = {
                let mut cache = self.plan_cache.lock().unwrap();
                cache.get(&query).cloned()
            };

            if let Some(cached_plan) = plan {
                // Cache hit: clone plan + apply params + collect
                let param_values = build_param_values(params.as_ref().unwrap())?;
                let plan = cached_plan
                    .with_param_values(param_values)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                let state = self.inner.ctx.state();
                let df = DataFrame::new(state, plan);
                return collect_to_query_result(df).await;
            }
        }

        // Cache miss (or no params): full planning path
        let mut df = self
            .inner
            .sql(&query)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        if let Some(ref p) = params {
            let param_values = build_param_values(p)?;
            df = df
                .with_param_values(param_values)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;

            // Cache the plan for future calls
            let plan = self
                .inner
                .ctx
                .state()
                .create_logical_plan(&query)
                .await
                .ok();
            if let Some(plan) = plan {
                let mut cache = self.plan_cache.lock().unwrap();
                cache.put(query.clone(), plan);
            }
        }

        collect_to_query_result(df).await
    }

    /// Prepare a SQL query for repeated execution with different parameters.
    ///
    /// Parses and plans the SQL once. The returned [`PreparedStatement`] can be
    /// executed many times with different params via
    /// [`PreparedStatement::execute`], avoiding repeated SQL parsing + schema
    /// resolution.
    ///
    /// # Example (JavaScript)
    ///
    /// ```js
    /// const stmt = ctx.prepare("SELECT * FROM table WHERE id = $1");
    /// const r1 = await stmt.execute([42]);
    /// const r2 = await stmt.execute([99]);
    /// ```
    #[napi]
    pub async fn prepare(&self, query: String) -> Result<PreparedStatement> {
        let state = self.inner.ctx.state();
        let plan = state
            .create_logical_plan(&query)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(PreparedStatement { plan, state })
    }

    /// Execute a SQL query and return only the row-oriented results (no Arrow IPC buffer).
    ///
    /// Avoids the cost of Arrow IPC serialization compared to [`sql`](#method.sql).
    /// Returns the same [`QueryResult`] with columns, numRows, and rows, but arrowIpc
    /// will be empty.
    ///
    /// Optionally accepts parameter bindings — same semantics as [`sql`](#method.sql).
    #[napi]
    pub async fn sql_rows(&self, query: String, params: Option<serde_json::Value>) -> Result<QueryResult> {
        // Try the LRU plan cache for parameterized queries
        if params.is_some() {
            let plan = {
                let mut cache = self.plan_cache.lock().unwrap();
                cache.get(&query).cloned()
            };

            if let Some(cached_plan) = plan {
                let param_values = build_param_values(params.as_ref().unwrap())?;
                let plan = cached_plan
                    .with_param_values(param_values)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                let state = self.inner.ctx.state();
                let df = DataFrame::new(state, plan);
                return collect_to_rows_only(df).await;
            }
        }

        let mut df = self
            .inner
            .sql(&query)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        if let Some(ref p) = params {
            let param_values = build_param_values(p)?;
            df = df
                .with_param_values(param_values)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;

            let plan = self
                .inner
                .ctx
                .state()
                .create_logical_plan(&query)
                .await
                .ok();
            if let Some(plan) = plan {
                let mut cache = self.plan_cache.lock().unwrap();
                cache.put(query.clone(), plan);
            }
        }

        collect_to_rows_only(df).await
    }

    /// Execute a SQL query and return only the Arrow IPC stream buffer (no row conversion).
    ///
    /// Avoids the cost of serde_arrow row serialization compared to [`sql`](#method.sql).
    /// Returns a raw binary buffer usable with `@apache-arrow`'s `tableFromIPC()`:
    ///
    /// ```js
    /// import { tableFromIPC } from '@apache-arrow';
    /// const table = tableFromIPC(new Uint8Array(buffer));
    /// ```
    ///
    /// Optionally accepts parameter bindings — same semantics as [`sql`](#method.sql).
    #[napi]
    pub async fn sql_arrow(&self, query: String, params: Option<serde_json::Value>) -> Result<Vec<u8>> {
        // Try the LRU plan cache for parameterized queries
        if params.is_some() {
            let plan = {
                let mut cache = self.plan_cache.lock().unwrap();
                cache.get(&query).cloned()
            };

            if let Some(cached_plan) = plan {
                let param_values = build_param_values(params.as_ref().unwrap())?;
                let plan = cached_plan
                    .with_param_values(param_values)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                let state = self.inner.ctx.state();
                let df = DataFrame::new(state, plan);
                return collect_to_arrow_only(df).await;
            }
        }

        let mut df = self
            .inner
            .sql(&query)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        if let Some(ref p) = params {
            let param_values = build_param_values(p)?;
            df = df
                .with_param_values(param_values)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;

            let plan = self
                .inner
                .ctx
                .state()
                .create_logical_plan(&query)
                .await
                .ok();
            if let Some(plan) = plan {
                let mut cache = self.plan_cache.lock().unwrap();
                cache.put(query.clone(), plan);
            }
        }

        collect_to_arrow_only(df).await
    }

    /// Execute a SQL query and return the results as a ReadableStream of rows.
    ///
    /// Each row is yielded as an array of strings, using Arrow's Display
    /// formatting. Null values are returned as empty strings.
    /// The stream lazily evaluates the query — rows are produced as the
    /// consumer reads them, so large result sets can be processed without
    /// buffering everything in memory.
    ///
    /// Optionally accepts parameter bindings:
    /// - An array for positional params (`$1`, `$2` in SQL)
    /// - An object for named params (`$name` in SQL)
    #[napi]
    pub fn stream_sql<'env>(&self, env: Env, query: String, params: Option<serde_json::Value>) -> Result<ReadableStream<'env, Vec<String>>> {
        // Block on SQL planning to produce a DataFrame (query is planned, not executed yet)
        let mut df = match block_on(self.inner.sql(&query)) {
            Ok(df) => df,
            Err(e) => return Err(napi::Error::from_reason(e.to_string())),
        };

        if let Some(ref p) = params {
            let param_values = match build_param_values(p) {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            df = match df.with_param_values(param_values) {
                Ok(d) => d,
                Err(e) => return Err(napi::Error::from_reason(e.to_string())),
            };
        }

        let (mut tx, rx) = futures::channel::mpsc::channel::<Vec<String>>(64);

        spawn(async move {
            let mut stream = match df.execute_stream().await {
                Ok(s) => s,
                Err(_) => {
                    let _ = tx.try_send(Vec::new());
                    return;
                }
            };

            while let Some(batch_result) = stream.next().await {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(_) => {
                        let _ = tx.try_send(Vec::new());
                        break;
                    }
                };
                let num_rows = batch.num_rows();
                let num_cols = batch.num_columns();
                for row_idx in 0..num_rows {
                    let mut row = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        let array = batch.column(col_idx);
                        if array.is_null(row_idx) {
                            row.push(String::new());
                        } else {
                            let s = arrow::util::display::array_value_to_string(
                                array.as_ref(),
                                row_idx,
                            )
                            .unwrap_or_else(|_| String::new());
                            row.push(s);
                        }
                    }
                    if tx.try_send(row).is_err() {
                        return; // stream canceled (receiver dropped)
                    }
                }
            }
        });

        ReadableStream::new(&env, rx.map(Ok))
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

    /// Read a GeoParquet file and immediately register it as a view.
    ///
    /// This avoids returning a DataFrame across the NAPI-RS async boundary,
    /// which can crash with large datasets. The table is registered with the
    /// given `name` and can be queried via SQL.
    #[napi]
    pub async fn register_parquet_table(
        &self,
        name: String,
        path: String,
        overwrite: Option<bool>,
    ) -> Result<()> {
        let df = self
            .inner
            .read_parquet(&path, Default::default())
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        let provider = df.into_view();
        let overwrite = overwrite.unwrap_or(false);

        if overwrite {
            let _ = self.inner.ctx.deregister_table(&name);
        }

        self.inner
            .ctx
            .register_table(&name, provider)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(())
    }
}
