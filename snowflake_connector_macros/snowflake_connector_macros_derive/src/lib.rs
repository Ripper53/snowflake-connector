extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input, DeriveInput, Data, Fields};

#[proc_macro_derive(SnowflakeDeserialize)]
pub fn snowflake_deserialize_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse_macro_input!(input);
    impl_snowflake_deserialize(&ast)
}

fn impl_snowflake_deserialize(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let (t_name, t_index, t_ty) = match &ast.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(data) => {
                    let count = data.named.len();
                    let mut t_name = Vec::with_capacity(count);
                    let mut t_index = Vec::with_capacity(count);
                    let mut t_ty = Vec::with_capacity(count);
                    for (i, field) in data.named.iter().enumerate() {
                        let name = field.ident.as_ref().unwrap();
                        let ty = &field.ty;
                        t_name.push(name);
                        t_index.push(i);
                        t_ty.push(ty);
                    }
                    (t_name, t_index, t_ty)
                },
                _ => panic!("Named fields only!"),
            }
        },
        Data::Enum(_) => panic!("This macro can only be derived in a struct, not enum."),
        Data::Union(_) => panic!("This macro can only be derived in a struct, not union."),
    };
    let gen = quote! {
        impl #impl_generics SnowflakeDeserialize for #name #ty_generics #where_clause {
            fn snowflake_deserialize(
                response: SnowflakeSQLResponse,
            ) -> Result<SnowflakeSQLResult<Self>, anyhow::Error> {
                let count = response.result_set_meta_data.num_rows;
                let mut results = Vec::with_capacity(count);
                for data in response.data {
                    results.push(#name #ty_generics {
                        #(#t_name: <#t_ty>::deserialize_from_str(&data[#t_index])?),*
                    });
                }
                Ok(SnowflakeSQLResult {
                    data: results,
                })
            }
        }
    };
    gen.into()
}
