use napi_derive::napi;
use napi::Result;

use crate::session_context::SessionContext;

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

    /// Execute the DataFrame and return all rows as arrays of strings.
    ///
    /// Each value is formatted using Arrow's Display implementation.
    /// Null values are returned as empty strings.
    #[napi]
    pub async fn collect(&self) -> Result<Vec<Vec<String>>> {
        let batches = self
            .inner
            .clone()
            .collect()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

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
                        let s =
                            arrow::util::display::array_value_to_string(array.as_ref(), row_idx)
                                .unwrap_or_else(|_| String::new());
                        row.push(s);
                    }
                }
                rows.push(row);
            }
        }

        Ok(rows)
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
