extern crate proc_macro;

use std::io::{BufRead, BufReader, Read, Write};

use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro::{Span, TokenStream};
use quote::quote;
use syn::{self, Data, DeriveInput, Fields, LitStr, parse::Parse, parse_macro_input};

/// Implements `SnowflakeDeserialize` for struct.
///
/// Creates an error enum named: `{name}DeserializeError`,
/// and generates a variant for each unique type that must implement `DeserializeFromStr`.
///
/// Use the attribute `snowflake_deserialize_error` for a custom error name. Ex. `#[snowflake_deserialize_error(CustomErrorName)]`
#[proc_macro_derive(
    SnowflakeDeserialize,
    attributes(snowflake_deserialize_error, snowflake)
)]
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

    let (conversion_generation, unique_name, unique_ty, unique_error) = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(data) => {
                let count = data.named.len();
                let mut conversion_generation = Vec::with_capacity(count);
                let mut unique_name = std::collections::HashMap::with_capacity(count);
                let mut added_uni = 0;
                for (i, field) in data.named.iter().enumerate() {
                    let name = field.ident.as_ref().unwrap();
                    let t_str = syn::LitStr::new(&name.to_string(), name.span());
                    let ty = &field.ty;
                    let (variant_key, t_variant) = if let syn::Type::Path(path) = ty
                        && let Some(seg) = path.path.segments.last()
                    {
                        let r = if seg.arguments.is_empty() {
                            seg.ident.to_string()
                        } else {
                            match &seg.arguments {
                                syn::PathArguments::AngleBracketed(angle) => {
                                    let generic_argument = angle.args.first().unwrap();
                                    match generic_argument {
                                        syn::GenericArgument::Type(ty) => match ty {
                                            syn::Type::Path(path) => {
                                                path.path.segments.last().unwrap().ident.to_string()
                                            }
                                            _ => todo!("Type handling"),
                                        },
                                        _ => todo!("Generic type handling"),
                                    }
                                }
                                _ => todo!("Path arguments handling"),
                            }
                        };
                        let name = syn::Ident::new(&r.to_upper_camel_case(), seg.ident.span());
                        (r, name)
                    } else {
                        todo!();
                    };
                    let (conversion_code, error) = if let Some(f) =
                        field.attrs.iter().find(|f| f.path().is_ident("snowflake"))
                        && let syn::Meta::Path(list) = f.parse_args().unwrap()
                        && let Some(first) = list.segments.first()
                        && first.ident.to_string() == "json"
                    {
                        (
                            quote! {
                                #name: ::snowflake_connector::serde_json::de::from_str::<#ty>(&data[#i]).map_err(|error| {
                                    #custom_error::#t_variant {
                                        field_name: #t_str,
                                        actual_value: data[#i].clone(),
                                        error,
                                    }
                                })?
                            },
                            quote!(::snowflake_connector::serde_json::Error),
                        )
                    } else {
                        (
                            quote! {
                                #name: <#ty as ::snowflake_connector::DeserializeFromStr>::deserialize_from_str(&data[#i]).map_err(|error| {
                                    #custom_error::#t_variant {
                                        field_name: #t_str,
                                        actual_value: data[#i].clone(),
                                        error,
                                    }
                                })?
                            },
                            quote!(<#ty as ::snowflake_connector::DeserializeFromStr>::Error),
                        )
                    };
                    conversion_generation.push(conversion_code);
                    unique_name.insert(variant_key, (t_variant, (ty, error)));
                }
                let (_, (unique_name, (unique_ty, unique_error))): (
                    Vec<_>,
                    (Vec<_>, (Vec<_>, Vec<_>)),
                ) = unique_name.into_iter().unzip();
                (conversion_generation, unique_name, unique_ty, unique_error)
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
                            #(#conversion_generation,)*
                        }
                    );
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
                    //error: <#unique_ty as ::snowflake_connector::DeserializeFromStr>::Error,
                    error: #unique_error,
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
    /*eprintln!(
        "Generated code from SnowflakeDeserialize macro:\n{}",
        generated_code
    );*/
    generated_code.into()
}
