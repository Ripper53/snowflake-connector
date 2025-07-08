extern crate proc_macro;

use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use quote::quote;
use syn::{self, Data, DeriveInput, Fields, MetaList, MetaNameValue, parse_macro_input};

/// Implements `SnowflakeDeserialize` for struct.
///
/// Creates an error enum named: `{name}DeserializeError`,
/// and generates a variant for each unique type that must implement `DeserializeFromStr`.
///
/// Use the attribute `snowflake_deserialize_error` for a custom error name. Ex. `#[snowflake_deserialize_error(CustomErrorName)]`
#[proc_macro_derive(
    SnowflakeDeserialize,
    attributes(
        snowflake,
        snowflake_deserialize_error,
        snowflake_deserialize_error_name
    )
)]
pub fn snowflake_deserialize_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse_macro_input!(input);
    if let Some(custom_error) = ast
        .attrs
        .iter()
        .find(|f| f.path().is_ident("snowflake_deserialize_error"))
    {
        let name: proc_macro2::Ident = custom_error.parse_args().unwrap();
        impl_snowflake_deserialize_custom_error(&ast, name)
    } else {
        impl_snowflake_deserialize(&ast)
    }
}

fn impl_snowflake_deserialize(ast: &DeriveInput) -> TokenStream {
    let custom_error = if let Some(custom_error) = ast
        .attrs
        .iter()
        .find(|f| f.path().is_ident("snowflake_deserialize_error_name"))
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

    let (conversion_generation, error_variants, errors) = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(data) => {
                let count = data.named.len();
                let mut conversion_generation = Vec::with_capacity(count);
                let mut names = Vec::with_capacity(count);
                for (i, field) in data.named.iter().enumerate() {
                    let name = field.ident.as_ref().unwrap();
                    let name_str = name.to_string();
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
                        let variant_name =
                            syn::Ident::new(&name_str.to_upper_camel_case(), seg.ident.span());
                        (r, variant_name)
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
                                        actual_value: data[#i].clone(),
                                        error,
                                    }
                                })?
                            },
                            quote!(<#ty as ::snowflake_connector::DeserializeFromStr>::Error),
                        )
                    };
                    conversion_generation.push(conversion_code);
                    names.push((variant_key, (t_variant, error)));
                }
                let (_, (names, errors)): (Vec<_>, (Vec<_>, Vec<_>)) = names.into_iter().unzip();
                (conversion_generation, names, errors)
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
                #error_variants {
                    actual_value: ::std::string::String,
                    error: #errors,
                },
            )*
        }
        impl #custom_error {
            pub fn actual_value(&self) -> &str {
                match self {
                    #(
                        Self::#error_variants { actual_value, .. } => &actual_value,
                    )*
                }
            }
        }
    };
    generated_code.into()
}

fn impl_snowflake_deserialize_custom_error(
    ast: &DeriveInput,
    custom_error: proc_macro2::Ident,
) -> TokenStream {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let conversion_generation = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(data) => {
                let count = data.named.len();
                let mut conversion_generation = Vec::with_capacity(count);
                for (i, field) in data.named.iter().enumerate() {
                    let field_name = field.ident.as_ref().unwrap();
                    let Some(error_expr) = field.attrs.iter().find_map(|f| {
                        if !f.meta.path().is_ident("snowflake") {
                            return None;
                        }
                        let Ok(metadata) = f.parse_args::<MetaNameValue>() else {
                            return None;
                        };
                        if metadata.path.is_ident("error") {
                            Some(metadata.value)
                        } else {
                            None
                        }
                    }) else {
                        let e = format!(
                            "Failed to find attribute `snowflake(error = ...)` for field `{}.{}`, usage: #[snowflake(error = {}::Variant {{ error }})]",
                            name, field_name, custom_error,
                        );
                        return quote!(compile_error!(#e);).into();
                    };
                    let is_json = field.attrs.iter().any(|f| {
                        if f.meta.path().is_ident("snowflake") {
                            return false;
                        }
                        let Ok(metadata) = f.parse_args::<MetaList>() else {
                            return false;
                        };
                        metadata.path.is_ident("json")
                    });
                    let Some(ref ident) = field.ident else {
                        return quote!(compile_error!("Field not named");).into();
                    };
                    conversion_generation.push((
                        ident.clone(),
                        field.ty.clone(),
                        error_expr,
                        is_json,
                    ));
                }
                conversion_generation
            }
            _ => return quote!(compile_error!("Named fields only!");).into(),
        },
        Data::Enum(_) => {
            return quote!(compile_error!(
                "This macro can only be derived in a struct, not enum."
            );)
            .into();
        }
        Data::Union(_) => {
            return quote!(compile_error!(
                "This macro can only be derived in a struct, not union."
            );)
            .into();
        }
    };
    let mut converted_code = Vec::with_capacity(conversion_generation.len());
    for (i, (field_name, ty, error_expr, is_json)) in conversion_generation.into_iter().enumerate()
    {
        let code = if is_json {
            quote! {
                #field_name: ::snowflake_connector::serde_json::de::from_str::<#ty>(&data[#i]).map_err(|error| {
                    #error_expr
                })?
            }
        } else {
            quote! {
                #field_name: <#ty as ::snowflake_connector::DeserializeFromStr>::deserialize_from_str(&data[#i]).map_err(|error| {
                    #error_expr
                })?
            }
        };
        converted_code.push(code);
    }
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
                            #(
                                #converted_code,
                            )*
                        }
                    );
                }
                Ok(::snowflake_connector::SnowflakeSQLResult {
                    data: results,
                })
            }
        }
    };
    generated_code.into()
}
