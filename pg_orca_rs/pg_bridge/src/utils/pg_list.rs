use pgrx::pg_sys;

/// Get the length of a PG List (or 0 if null).
pub unsafe fn list_length(list: *mut pg_sys::List) -> i32 {
    if list.is_null() { 0 } else { (*list).length }
}

/// Iterate over a PG List, yielding `*mut T` pointers.
pub unsafe fn list_iter<T>(list: *mut pg_sys::List) -> Vec<*mut T> {
    let mut result = Vec::new();
    if list.is_null() {
        return result;
    }
    let len = (*list).length as usize;
    let elements = (*list).elements;
    for i in 0..len {
        let cell = *elements.add(i);
        result.push(cell.ptr_value as *mut T);
    }
    result
}

/// Append a pointer to a PG List.
pub unsafe fn lappend(list: *mut pg_sys::List, datum: *mut std::ffi::c_void) -> *mut pg_sys::List {
    pg_sys::lappend(list, datum)
}

/// Create an OID list and append an OID.
pub unsafe fn lappend_oid(list: *mut pg_sys::List, oid: pg_sys::Oid) -> *mut pg_sys::List {
    pg_sys::lappend_oid(list, oid)
}
