use std::collections::HashMap;
use optimizer_core::ir::types::{ColumnId, ColumnRef, TableId};

/// Bidirectional mapping between ColumnId and PG Var attributes.
pub struct ColumnMapping {
    columns: HashMap<ColumnId, ColumnRef>,
    var_to_colid: HashMap<(u32, i16), ColumnId>,
    next_id: u32,
}

impl ColumnMapping {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            var_to_colid: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn register_column(
        &mut self,
        table_id: TableId,
        name: &str,
        varno: u32,
        varattno: i16,
        vartype: u32,
        vartypmod: i32,
        varcollid: u32,
    ) -> ColumnId {
        // Check if already registered
        if let Some(&cid) = self.var_to_colid.get(&(varno, varattno)) {
            return cid;
        }

        let id = ColumnId(self.next_id);
        self.next_id += 1;

        let col_ref = ColumnRef {
            id,
            table_id,
            name: name.to_string(),
            pg_varno: varno,
            pg_varattno: varattno,
            pg_vartype: vartype,
            pg_vartypmod: vartypmod,
            pg_varcollid: varcollid,
        };

        self.columns.insert(id, col_ref);
        self.var_to_colid.insert((varno, varattno), id);
        id
    }

    pub fn lookup_var(&self, varno: u32, varattno: i16) -> Option<ColumnId> {
        self.var_to_colid.get(&(varno, varattno)).copied()
    }

    pub fn get_column_ref(&self, id: ColumnId) -> Option<&ColumnRef> {
        self.columns.get(&id)
    }

    pub fn all_columns(&self) -> impl Iterator<Item = &ColumnRef> {
        self.columns.values()
    }
}
