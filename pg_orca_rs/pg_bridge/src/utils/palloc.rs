use pgrx::pg_sys;

/// Allocate a zeroed PG node of type T and set its NodeTag.
/// Equivalent to PG's makeNode() macro.
pub unsafe fn palloc_node<T>(tag: pg_sys::NodeTag) -> *mut T {
    let size = std::mem::size_of::<T>();
    let ptr = pg_sys::palloc0(size) as *mut T;
    // The first field of every Node is `type_: NodeTag`
    let node_ptr = ptr as *mut pg_sys::Node;
    (*node_ptr).type_ = tag;
    ptr
}
