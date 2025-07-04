extern crate proc_macro;

use std::io::{BufRead, BufReader, Read};

use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro::TokenStream;
use quote::quote;
use syn::{self, Data, DeriveInput, Fields, LitStr, parse_macro_input};

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

/* ---------------- */

#[proc_macro_derive(SnowflakeTable, attributes(snowflake_path))]
pub fn snowflake_table_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse_macro_input!(input);
    impl_snowflake_table(&ast)
}

fn impl_snowflake_table(ast: &DeriveInput) -> TokenStream {
    let mut path = None;
    for attr in ast.attrs.iter() {
        let p = attr.path();
        if p.is_ident("snowflake_path") {
            path = Some(attr.parse_args::<LitStr>().unwrap());
            break;
        }
    }
    let path = if let Some(path) = path {
        std::path::Path::new(&path.value()).to_path_buf()
    } else {
        std::path::Path::new(env!("OUT_DIR")).join("snowflake-data.json")
    };
    let file = std::fs::File::open(path).unwrap();
    let mut buf = BufReader::new(file);
    let mut read = String::new();
    let mut numbers = String::new();
    let mut read = Vec::new();
    let mut structs = Vec::new();
    while let Ok(v) = buf.read_line(&mut numbers)
        && v != 0
    {
        let sliced_number = numbers.trim();
        let len = sliced_number.parse::<usize>();
        if let Err(_) = len {
            //panic!("STRUCTS: {}", structs.len());
            break;
        }
        let len = len.unwrap();
        numbers.clear();
        read.clear();
        for _ in 0..len {
            read.push(0);
        }
        let slice = read.as_mut_slice();
        if buf.read_exact(slice).is_err() {
            break;
        }
        let value: serde_json::Value = serde_json::from_slice(slice).expect(&format!(
            "Failed to parse query: {}",
            str::from_utf8(slice).unwrap()
        ));
        let metadata = value
            .get("resultSetMetaData")
            .expect("Could not find metadata");
        let row_types = metadata
            .get("rowType")
            .unwrap()
            .as_array()
            .expect("Failed to find `rowType`");
        let mut names = Vec::with_capacity(row_types.len());
        let mut types = Vec::with_capacity(row_types.len());
        let mut tables = Vec::new();
        for row_type in row_types {
            let name = row_type
                .get("name")
                .unwrap()
                .as_str()
                .unwrap()
                .to_snake_case();
            names.push(syn::Ident::new(&name, ast.ident.span()));
            let nullable = row_type.get("nullable").unwrap().as_bool().unwrap();
            let ty = row_type.get("type").unwrap().as_str().unwrap();
            let ty = match ty {
                "fixed" => quote!(usize),
                "text" | "variant" => quote!(::std::string::String),
                unknown_type => panic!("unknown type: {unknown_type}"),
            };
            if nullable {
                types.push(quote!(::std::option::Option<#ty>));
            } else {
                types.push(ty);
            }
            let table = row_type
                .get("table")
                .unwrap()
                .as_str()
                .unwrap()
                .to_upper_camel_case();
            tables.push(syn::Ident::new(&table, ast.ident.span()));
        }
        tables.dedup();
        if tables.is_empty() {
            panic!("No tables found for query");
        } else if tables.len() == 1 {
            let table = tables.pop().unwrap();
            structs.push(quote! {
                #[derive(::snowflake_connector::SnowflakeDeserialize, Debug)]
                pub struct #table {
                    #(
                        #names: #types,
                    )*
                }
            });
        } else {
            todo!("Unhandled multiple table query! Amount: {}", tables.len());
        }
    }
    let generated = quote! {
        #(
            #structs
        )*
    };
    //eprintln!("Generated code from SnowflakeTable macro:\n{}", generated);
    generated.into()
}
