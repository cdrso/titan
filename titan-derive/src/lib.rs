use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Fields, GenericArgument, Ident, PathArguments, ReturnType,
    Type, TypePath, parse_macro_input, spanned::Spanned,
};

/// Derive macro for the `SharedMemorySafe` trait.
///
/// This macro generates a safe implementation of `SharedMemorySafe` by verifying
/// at compile time that a type meets the requirements for cross-process shared memory.
///
/// # Compile-Time Checks
///
/// The macro enforces the following requirements:
///
/// 1. **Stable layout**: The type must have `#[repr(C)]`, `#[repr(transparent)]`,
///    or for enums `#[repr(u8)]`/`#[repr(i32)]`/etc. This ensures consistent memory
///    layout across compilation units and compiler versions.
///
/// 2. **No pointer types**: Fields cannot contain types that hold pointers, as virtual
///    addresses are process-specific. Forbidden types include:
///    - Heap allocations: `Vec`, `Box`, `String`, `PathBuf`, `OsString`, `CString`
///    - Reference counting: `Rc`, `Arc`
///    - References: `&T`, `&mut T`
///    - Raw pointers: `*const T`, `*mut T`
///    - Process-local synchronization: `Mutex`, `RwLock`, `Condvar`, `Barrier`
///
/// 3. **Recursive safety**: All fields must themselves implement `SharedMemorySafe`.
///    This is enforced via generated where clauses.
///
/// # Safety
///
/// This macro generates an `unsafe impl SharedMemorySafe` because the trait has
/// safety requirements that cannot be fully verified at compile time:
///
/// - The type must be safe to access concurrently from multiple processes
/// - The type must not rely on `Drop` for safety invariants (crashes bypass destructors)
/// - The type should use atomic operations for any mutable shared state
///
/// The macro provides compile-time validation for layout and pointer-freedom,
/// but the user is responsible for ensuring concurrent access safety (typically
/// by using atomic types for shared mutable state).
///
/// # Example
///
/// ```
/// # use titan::SharedMemorySafe;
/// use std::sync::atomic::AtomicUsize;
///
/// #[derive(SharedMemorySafe)]
/// #[repr(C)]
/// struct RingBuffer {
///     head: AtomicUsize,
///     tail: AtomicUsize,
///     data: [u8; 4096],
/// }
/// ```
///
/// # Compile Errors
///
/// The macro produces helpful error messages for common mistakes:
///
/// ```compile_fail
/// # use titan::SharedMemorySafe;
/// #[derive(SharedMemorySafe)]
/// struct MissingRepr {  // Error: requires #[repr(C)]
///     x: u32,
/// }
/// ```
///
/// ```compile_fail
/// # use titan::SharedMemorySafe;
/// #[derive(SharedMemorySafe)]
/// #[repr(C)]
/// struct HasPointer {
///     data: Vec<u8>,  // Error: Vec contains heap allocation
/// }
/// ```
#[proc_macro_derive(SharedMemorySafe)]
pub fn derive_shared_memory_safe(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match derive_shared_memory_safe_impl(input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error().into(),
    }
}

fn get_crate_path() -> proc_macro2::TokenStream {
    match crate_name("titan") {
        Ok(FoundCrate::Itself) => {
            quote!(::titan)
        }
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => {
            quote!(::titan)
        }
    }
}

fn derive_shared_memory_safe_impl(input: DeriveInput) -> syn::Result<TokenStream> {
    check_repr(&input)?;

    let field_types = get_field_types(&input.data)?;
    field_types.iter().try_for_each(check_types)?;

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let crate_path = get_crate_path();

    let mut where_predicates = where_clause
        .map(|w| w.predicates.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    for ty in &field_types {
        where_predicates.push(syn::parse_quote! {
            #ty: #crate_path::__SharedMemorySafePrivate
        });
    }

    let expanded = if where_predicates.is_empty() {
        quote! {
            unsafe impl #impl_generics #crate_path::__SharedMemorySafePrivate for #name #ty_generics #where_clause {}
        }
    } else {
        quote! {
            unsafe impl #impl_generics #crate_path::__SharedMemorySafePrivate for #name #ty_generics
            where
                #(#where_predicates),*
            {}
        }
    };

    Ok(expanded.into())
}

fn is_valid_repr_ident(ident: &syn::Ident) -> bool {
    ident == "C"
        || ident == "transparent"
        || ident == "u8"
        || ident == "u16"
        || ident == "u32"
        || ident == "u64"
        || ident == "u128"
        || ident == "usize"
        || ident == "i8"
        || ident == "i16"
        || ident == "i32"
        || ident == "i64"
        || ident == "i128"
        || ident == "isize"
}

fn has_valid_repr(attr: &Attribute) -> syn::Result<bool> {
    if !attr.path().is_ident("repr") {
        return Ok(false);
    }

    let mut valid = false;

    attr.parse_nested_meta(|meta| {
        if let Some(ident) = meta.path.get_ident()
            && is_valid_repr_ident(ident)
        {
            valid = true;
        }
        Ok(())
    })?;

    Ok(valid)
}

fn check_repr(input: &DeriveInput) -> syn::Result<()> {
    let has_valid =
        input.attrs.iter().try_fold(
            false,
            |acc, attr| {
                if acc { Ok(true) } else { has_valid_repr(attr) }
            },
        )?;

    if !has_valid {
        let help_msg = if matches!(input.data, Data::Enum(_)) {
            "SharedMemorySafe requires #[repr(C)], #[repr(transparent)], \
             or #[repr(u8/i8/etc)] for enums\n\
             help: add #[repr(C)] or #[repr(u8)] above this item"
        } else {
            "SharedMemorySafe requires #[repr(C)] or #[repr(transparent)]\n\
             help: add #[repr(C)] above this item"
        };
        return Err(Error::new(input.span(), help_msg));
    }
    Ok(())
}

fn get_field_types(data: &Data) -> syn::Result<Vec<Type>> {
    fn extract_field_types(fields: &Fields) -> Vec<Type> {
        match fields {
            Fields::Named(fields) => fields.named.iter().map(|f| f.ty.clone()).collect(),
            Fields::Unnamed(fields) => fields.unnamed.iter().map(|f| f.ty.clone()).collect(),
            Fields::Unit => Vec::new(),
        }
    }

    match data {
        Data::Struct(data_struct) => Ok(extract_field_types(&data_struct.fields)),

        Data::Enum(data_enum) => Ok(data_enum
            .variants
            .iter()
            .flat_map(|variant| extract_field_types(&variant.fields))
            .collect()),

        Data::Union(u) => Err(Error::new(
            u.union_token.span,
            "SharedMemorySafe cannot be derived for unions",
        )),
    }
}

fn check_types(field_ty: &Type) -> syn::Result<()> {
    // Inner recursive walker that has access to the original field type.
    fn walk(ty: &Type, field_ty: &Type) -> syn::Result<()> {
        match ty {
            Type::Path(TypePath { path, .. }) => {
                for segment in &path.segments {
                    check_forbidden_type(&segment.ident, field_ty, segment.ident.span())?;

                    match &segment.arguments {
                        PathArguments::AngleBracketed(args) => {
                            for arg in &args.args {
                                if let GenericArgument::Type(inner_ty) = arg {
                                    walk(inner_ty, field_ty)?;
                                }
                            }
                        }
                        PathArguments::Parenthesized(args) => {
                            for input in &args.inputs {
                                walk(input, field_ty)?;
                            }
                            if let ReturnType::Type(_, ret_ty) = &args.output {
                                walk(ret_ty, field_ty)?;
                            }
                        }
                        PathArguments::None => {}
                    }
                }
            }

            Type::Reference(type_ref) => {
                return Err(Error::new(
                    type_ref.span(),
                    format!(
                        "Field type `{}` contains a reference (`&` or `&mut`).\n\
                         References are process-specific and cannot be shared across processes.\n\
                         help: use inline data or atomics instead",
                        quote!(#field_ty),
                    ),
                ));
            }

            Type::Ptr(type_ptr) => {
                return Err(Error::new(
                    type_ptr.span(),
                    format!(
                        "Field type `{}` contains a raw pointer (`*const` or `*mut`).\n\
                         Pointers are process-specific and cannot be shared across processes.\n\
                         help: use inline data or atomics instead",
                        quote!(#field_ty),
                    ),
                ));
            }

            Type::Tuple(tuple) => {
                for elem in &tuple.elems {
                    walk(elem, field_ty)?;
                }
            }

            Type::Array(array) => {
                walk(&array.elem, field_ty)?;
            }

            Type::Slice(slice) => {
                walk(&slice.elem, field_ty)?;
            }

            Type::Group(group) => {
                walk(&group.elem, field_ty)?;
            }

            Type::Paren(paren) => {
                walk(&paren.elem, field_ty)?;
            }

            // Other variants (Never, Infer, Macro, TraitObject, ImplTrait, Verbatim, etc.)
            // either can't appear as struct fields or don't contain type parameters we care about.
            _ => {}
        }

        Ok(())
    }

    walk(field_ty, field_ty)
}

fn check_forbidden_type(
    ident: &Ident,
    field_ty: &Type,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    enum ForbiddenType {
        Heap,
        RefCounted,
        ProcessLocal,
    }

    fn classify_forbidden(ident: &Ident) -> Option<ForbiddenType> {
        const HEAP_TYPES: &[&str] = &["Vec", "Box", "String", "PathBuf", "OsString", "CString"];
        const RC_TYPES: &[&str] = &["Rc", "Arc"];
        const SYNC_TYPES: &[&str] = &["Mutex", "RwLock", "Condvar", "Barrier"];

        if HEAP_TYPES.iter().any(|&name| ident == name) {
            Some(ForbiddenType::Heap)
        } else if RC_TYPES.iter().any(|&name| ident == name) {
            Some(ForbiddenType::RefCounted)
        } else if SYNC_TYPES.iter().any(|&name| ident == name) {
            Some(ForbiddenType::ProcessLocal)
        } else {
            None
        }
    }

    if let Some(category) = classify_forbidden(ident) {
        let msg = match category {
            ForbiddenType::Heap => format!(
                "Field type `{}` contains `{}` which has heap allocation.\n\
                 Shared memory cannot contain pointer types.\n\
                 help: use inline data like `[T; N]` instead of `Vec<T>`, or primitive types",
                quote!(#field_ty),
                ident,
            ),
            ForbiddenType::RefCounted => format!(
                "Field type `{}` contains `{}` which uses reference counting.\n\
                 Shared memory cannot contain pointer types.\n\
                 help: use inline data or atomic types instead",
                quote!(#field_ty),
                ident,
            ),
            ForbiddenType::ProcessLocal => format!(
                "Field type `{}` contains `{}` which is process-local.\n\
                 `std::sync::{}` does not work across process boundaries.\n\
                 help: use atomic types (AtomicU64, AtomicBool, etc.) for cross-process synchronization",
                quote!(#field_ty),
                ident,
                ident,
            ),
        };

        return Err(Error::new(span, msg));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_check_repr_accepts_repr_c() {
        let input: DeriveInput = parse_quote! {
            #[repr(C)]
            struct Foo {
                x: u32,
            }
        };
        assert!(check_repr(&input).is_ok());
    }

    #[test]
    fn test_check_repr_accepts_repr_transparent() {
        let input: DeriveInput = parse_quote! {
            #[repr(transparent)]
            struct Foo(u32);
        };
        assert!(check_repr(&input).is_ok());
    }

    #[test]
    fn test_check_repr_accepts_repr_u8_enum() {
        let input: DeriveInput = parse_quote! {
            #[repr(u8)]
            enum Foo {
                A,
                B,
            }
        };
        assert!(check_repr(&input).is_ok());
    }

    #[test]
    fn test_check_repr_rejects_no_repr() {
        let input: DeriveInput = parse_quote! {
            struct Foo {
                x: u32,
            }
        };
        assert!(check_repr(&input).is_err());
    }

    #[test]
    fn test_check_types_accepts_primitives() {
        let ty: Type = parse_quote!(u32);
        assert!(check_types(&ty).is_ok());
    }

    #[test]
    fn test_check_types_accepts_atomics() {
        let ty: Type = parse_quote!(AtomicU64);
        assert!(check_types(&ty).is_ok());
    }

    #[test]
    fn test_check_types_rejects_vec() {
        let ty: Type = parse_quote!(Vec<u8>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_box() {
        let ty: Type = parse_quote!(Box<u32>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_string() {
        let ty: Type = parse_quote!(String);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_rc() {
        let ty: Type = parse_quote!(Rc<u32>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_arc() {
        let ty: Type = parse_quote!(Arc<u32>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_reference() {
        let ty: Type = parse_quote!(&u32);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_mut_reference() {
        let ty: Type = parse_quote!(&mut u32);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_raw_pointer() {
        let ty: Type = parse_quote!(*const u32);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_mut_raw_pointer() {
        let ty: Type = parse_quote!(*mut u32);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_mutex() {
        let ty: Type = parse_quote!(Mutex<u32>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_rwlock() {
        let ty: Type = parse_quote!(RwLock<u32>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_condvar() {
        let ty: Type = parse_quote!(Condvar);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_barrier() {
        let ty: Type = parse_quote!(Barrier);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_pathbuf() {
        let ty: Type = parse_quote!(PathBuf);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_get_field_types_struct() {
        let input: DeriveInput = parse_quote! {
            struct Foo {
                x: u32,
                y: u64,
            }
        };
        let types = get_field_types(&input.data).unwrap();
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_get_field_types_tuple_struct() {
        let input: DeriveInput = parse_quote! {
            struct Foo(u32, u64);
        };
        let types = get_field_types(&input.data).unwrap();
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_get_field_types_unit_struct() {
        let input: DeriveInput = parse_quote! {
            struct Foo;
        };
        let types = get_field_types(&input.data).unwrap();
        assert_eq!(types.len(), 0);
    }

    #[test]
    fn test_get_field_types_enum() {
        let input: DeriveInput = parse_quote! {
            enum Foo {
                A(u32),
                B { x: u64 },
                C,
            }
        };
        let types = get_field_types(&input.data).unwrap();
        assert_eq!(types.len(), 2); // u32 and u64
    }

    #[test]
    fn test_get_field_types_rejects_union() {
        let input: DeriveInput = parse_quote! {
            union Foo {
                x: u32,
                y: f32,
            }
        };
        assert!(get_field_types(&input.data).is_err());
    }

    // Deep pointer detection tests
    #[test]
    fn test_check_types_rejects_option_vec() {
        let ty: Type = parse_quote!(Option<Vec<u8>>);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_result_arc() {
        let ty: Type = parse_quote!(Result<Arc<u32>, String>);
        let result = check_types(&ty);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_types_rejects_tuple_with_box() {
        let ty: Type = parse_quote!((u32, Box<u64>));
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_rejects_array_of_mutex() {
        let ty: Type = parse_quote!([Mutex<u64>; 4]);
        assert!(check_types(&ty).is_err());
    }

    #[test]
    fn test_check_types_accepts_option_primitive() {
        let ty: Type = parse_quote!(Option<u32>);
        assert!(check_types(&ty).is_ok());
    }

    #[test]
    fn test_check_types_accepts_result_primitives() {
        let ty: Type = parse_quote!(Result<u32, i32>);
        assert!(check_types(&ty).is_ok());
    }
}
