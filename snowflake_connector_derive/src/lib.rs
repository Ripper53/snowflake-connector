extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{self, Data, DeriveInput, Fields, parse_macro_input};

/// Implements `SnowflakeDeserialize` for struct.
///
/// Creates an error enum named: `{name}DeserializeError`,
/// and generates a variant for each unique type that must implement `DeserializeFromStr`.
///
/// Use the attribute `snowflake_deserialize_error` for a custom error name. Ex. `#[snowflake_deserialize_error(CustomErrorName)]`
#[proc_macro_derive(SnowflakeDeserialize, attributes(snowflake_deserialize_error))]
pub fn snowflake_deserialize_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse_macro_input!(input);
    impl_snowflake_deserialize(&ast)
}

fn impl_snowflake_deserialize(ast: &DeriveInput) -> TokenStream {
    let custom_error = if let Some(custom_error) = ast
        .attrs
        .iter()
        .find(|f| f.path().is_ident("snowflake_deserialize_error"))
    {
        custom_error.parse_args().unwrap()
    } else {
        syn::Ident::new(
            &format!("{}DeserializeError", ast.ident.to_string()),
            ast.ident.span(),
        )
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let (t_name, t_str, t_index, t_ty, t_variant, unique_name, unique_ty) = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(data) => {
                let count = data.named.len();
                let mut t_name = Vec::with_capacity(count);
                let mut t_str = Vec::with_capacity(count);
                let mut t_index = Vec::with_capacity(count);
                let mut t_ty = Vec::with_capacity(count);
                let mut unique_name = std::collections::HashSet::with_capacity(count);
                for (i, field) in data.named.iter().enumerate() {
                    let name = field.ident.as_ref().unwrap();
                    let ty = &field.ty;
                    t_name.push(name);
                    t_str.push(syn::LitStr::new(&name.to_string(), name.span()));
                    t_index.push(i);
                    if let syn::Type::Path(path) = ty
                        && let Some(seg) = path.path.segments.first()
                    {
                        let s = seg.ident.to_string();
                        let name = syn::Ident::new(&to_upper_camel(&s), seg.ident.span());
                        t_ty.push((ty, name));
                        unique_name
                            .insert((syn::Ident::new(&to_upper_camel(&s), seg.ident.span()), ty));
                    }
                }
                let (t_ty, t_variant): (Vec<_>, Vec<_>) = t_ty.into_iter().unzip();
                let (unique_name, unique_ty): (Vec<_>, Vec<_>) = unique_name.into_iter().unzip();
                (
                    t_name,
                    t_str,
                    t_index,
                    t_ty,
                    t_variant,
                    unique_name,
                    unique_ty,
                )
            }
            _ => panic!("Named fields only!"),
        },
        Data::Enum(_) => panic!("This macro can only be derived in a struct, not enum."),
        Data::Union(_) => panic!("This macro can only be derived in a struct, not union."),
    };
    let generated_code = quote! {
        impl #impl_generics ::snowflake_connector::SnowflakeDeserialize for #name #ty_generics #where_clause {
            type Error = #custom_error;
            fn snowflake_deserialize(
                response: ::snowflake_connector::SnowflakeSQLResponse,
            ) -> Result<::snowflake_connector::SnowflakeSQLResult<Self>, Self::Error> {
                let count = response.result_set_meta_data.num_rows;
                let mut results = ::std::vec::Vec::with_capacity(count);
                for data in response.data {
                    results.push(
                        #name #ty_generics {
                            #(#t_name: <#t_ty>::deserialize_from_str(&data[#t_index]).map_err(|error| {
                                #custom_error::#t_variant {
                                    field_name: #t_str,
                                    actual_value: data[#t_index].clone(),
                                    error,
                                }
                        })?),*
                    });
                }
                Ok(::snowflake_connector::SnowflakeSQLResult {
                    data: results,
                })
            }
        }
        #[derive(Debug)]
        pub enum #custom_error {
            #(
                #unique_name {
                    field_name: &'static str,
                    actual_value: ::std::string::String,
                    error: <#unique_ty as ::snowflake_connector::DeserializeFromStr>::Error,
                },
            )*
        }
        impl #custom_error {
            pub const fn field_name(&self) -> &'static str {
                match self {
                    #(
                        Self::#unique_name { field_name, .. } => field_name,
                    )*
                }
            }
        }
        // TODO: figure out why commented out code implements From<#custom_error> for #custom_error
        /*#(
            impl ::std::convert::From<<#unique_ty as ::snowflake_connector::DeserializeFromStr>::Error> for #custom_error {
                fn from(value: <#unique_ty as ::snowflake_connector::DeserializeFromStr>::Error) -> Self {
                    Self::#unique_name(value)
                }
            }
        )**/
    };
    generated_code.into()
}

fn to_upper_camel(s: &str) -> String {
    s.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut c = part.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        })
        .collect()
}
