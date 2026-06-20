use napi_derive::napi;
use napi::Result;
use napi::bindgen_prelude::{ReadableStream, spawn};
use napi::Env;
use futures::StreamExt;

use crate::session_context::{SessionContext, QueryResult, record_batches_to_ipc, binary_cols_to_strings};

/// Schema field metadata
#[napi(object)]
#[derive(Clone)]
pub struct SchemaField {
    /// Column name
    pub name: String,
    /// Arrow data type name (e.g. "Utf8", "Float64", "Int32")
    pub data_type: String,
    /// Whether the column is nullable
    pub nullable: bool,
}

/// A SedonaDB DataFrame — wraps a DataFusion DataFrame with spatial extensions.
///
/// Returned from `SessionContext.sql()` and `readParquet()`.
/// Supports chaining operations like `.sortBy()`, `.select()`, `.limit()`.
#[napi]
pub struct SedonaDataFrame {
    pub(crate) inner: datafusion::prelude::DataFrame,
}

#[napi]
impl SedonaDataFrame {
    /// Get the schema of this DataFrame
    #[napi]
    pub fn schema(&self) -> Vec<SchemaField> {
        let schema = self.inner.schema();
        schema
            .fields()
            .iter()
            .map(|f| SchemaField {
                name: f.name().clone(),
                data_type: f.data_type().to_string(),
                nullable: f.is_nullable(),
            })
            .collect()
    }

    /// Execute the DataFrame and return a [`QueryResult`] containing typed JS rows
    /// and an Arrow IPC buffer.
    ///
    /// The result includes typed rows (numbers stay numbers, nulls stay null),
    /// column names, row count, and a binary Arrow IPC stream for advanced use
    /// with `@apache-arrow`.
    #[napi]
    pub async fn collect(&self) -> Result<QueryResult> {
        let batches = self
            .inner
            .clone()
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

    /// Execute the DataFrame and return only the row-oriented results (no Arrow IPC).
    ///
    /// Avoids the cost of Arrow IPC serialization compared to [`collect`](#method.collect).
    /// Returns a [`QueryResult`] with columns, numRows, and rows, but arrowIpc will be empty.
    #[napi]
    pub async fn collect_rows(&self) -> Result<QueryResult> {
        let batches = self
            .inner
            .clone()
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

    /// Execute the DataFrame and return only the Arrow IPC stream buffer (no row conversion).
    ///
    /// Avoids the cost of serde_arrow row serialization compared to [`collect`](#method.collect).
    /// Returns raw IPC bytes — use with `@apache-arrow`'s `tableFromIPC()`:
    ///
    /// ```js
    /// import { tableFromIPC } from '@apache-arrow';
    /// const table = tableFromIPC(new Uint8Array(buffer));
    /// ```
    #[napi]
    pub async fn collect_arrow(&self) -> Result<Vec<u8>> {
        let batches = self
            .inner
            .clone()
            .collect()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        record_batches_to_ipc(&batches)
    }

    /// Execute the DataFrame and return a ReadableStream of rows.
    ///
    /// Each row is yielded as an array of strings, using Arrow's Display
    /// formatting. Null values are returned as empty strings.
    /// The stream lazily evaluates the query — rows are produced as the
    /// consumer reads them, so large result sets can be processed without
    /// buffering everything in memory.
    #[napi]
    pub fn stream<'env>(&self, env: Env) -> Result<ReadableStream<'env, Vec<String>>> {
        let df = self.inner.clone();

        // Bounded channel provides backpressure — if the consumer reads
        // slowly, the producer will block instead of buffering indefinitely.
        let (mut tx, rx) = futures::channel::mpsc::channel::<Vec<String>>(64);

        spawn(async move {
            let mut stream = match df.execute_stream().await {
                Ok(s) => s,
                Err(_) => {
                    let _ = tx.try_send(Vec::new()); // empty row signals error
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

    /// Print the first `limit` rows as a formatted ASCII table string.
    ///
    /// If `limit` is not provided, all rows are shown.
    #[napi]
    pub async fn show_sedona(&self, limit: Option<i64>) -> Result<String> {
        use sedona::context::SedonaDataFrame as _;
        use sedona::show::DisplayTableOptions;

        // We need a SedonaContext to call show_sedona, but we don't store one.
        // Since show_sedona only uses the context for formatting options (not
        // data access), we can create a minimal context.
        //
        // However, constructing SedonaContext::new() is lightweight (it just
        // registers UDFs), so this is fine for the show operation.
        let ctx = sedona::context::SedonaContext::new();

        let limit = limit.map(|l| l as usize);

        let output = self
            .inner
            .clone()
            .show_sedona(&ctx, limit, DisplayTableOptions::default())
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(output)
    }

    /// Sort the DataFrame by the specified columns.
    ///
    /// Each column can optionally be followed by " DESC" for descending order.
    /// Example: `df.sortBy(["name", "population DESC"])`
    #[napi]
    pub async fn sort_by(&self, cols: Vec<String>) -> Result<SedonaDataFrame> {
        use datafusion::prelude::col;
        let sort_exprs: Vec<datafusion::prelude::Expr> = cols
            .iter()
            .map(|c| col(c.trim_end_matches(" DESC")))
            .collect();

        let df = self
            .inner
            .clone()
            .sort_by(sort_exprs)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(SedonaDataFrame { inner: df })
    }

    /// Select specific columns from the DataFrame.
    #[napi]
    pub async fn select(&self, cols: Vec<String>) -> Result<SedonaDataFrame> {
        use datafusion::prelude::col;

        let exprs: Vec<datafusion::prelude::Expr> = cols.iter().map(|c| col(c)).collect();

        let df = self
            .inner
            .clone()
            .select(exprs)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(SedonaDataFrame { inner: df })
    }

    /// Limit the number of rows in the DataFrame.
    #[napi]
    pub async fn limit(&self, n: i64) -> Result<SedonaDataFrame> {
        let n_usize = if n < 0 { 0_usize } else { n as usize };

        let df = self
            .inner
            .clone()
            .limit(0, Some(n_usize))
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(SedonaDataFrame { inner: df })
    }

    /// Register this DataFrame as a persistent view in the given session context.
    ///
    /// The view can be queried via SQL using `name`. If `overwrite` is `true`,
    /// any existing table or view with the same name will be replaced.
    #[napi]
    pub fn to_view(
        &self,
        ctx: &SessionContext,
        name: String,
        overwrite: Option<bool>,
    ) -> Result<()> {
        let provider = self.inner.clone().into_view();
        let overwrite = overwrite.unwrap_or(false);

        if overwrite {
            // Silently succeed if the table doesn't exist
            let _ = ctx.inner.ctx.deregister_table(&name);
        }

        ctx.inner
            .ctx
            .register_table(&name, provider)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(())
    }
}
