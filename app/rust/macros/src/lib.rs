use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, punctuated::Punctuated, Data, DeriveInput, Ident, Token};

#[proc_macro_derive(SerializeJson, attributes(serjson))]
pub fn serialize_json_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let Data::Struct(data_struct) = input.data else {
        panic!("data struct expected")
    };

    let struct_name = input.ident;

    let template = 'b: {
        if data_struct.fields.is_empty() {
            break 'b vec!["{}".to_owned()];
        }
        let mut res: Vec<String> = vec![];
        for field in &data_struct.fields {
            let Some(ident) = field.ident.as_ref() else {
                panic!("tuple struct is not supported")
            };
            let mut this = format!("\"{ident}\":");
            if !res.is_empty() {
                this.insert(0, ',');
            }
            res.push(this);
        }
        res.first_mut().unwrap().insert(0, '{');
        res.push("}".to_owned());
        res
    };
    let template_bytes: usize = template.iter().map(|x| x.len()).sum();
    let template_len = template.len();

    // ---

    let mut json_template: Punctuated<TokenStream2, Token![,]> = Punctuated::new();
    for t in &template {
        json_template.push(format!("r#\"{t}\"#").parse().unwrap());
    }

    let mut child_size: Punctuated<TokenStream2, Token![+]> = Punctuated::new();
    for field in &data_struct.fields {
        let Some(ident) = field.ident.as_ref() else {
            panic!("tuple struct is not supported")
        };
        let ty = &field.ty;
        child_size.push(quote!(<#ty as crate::fw::SerializeJson>::size_est(&self.#ident)));
    }

    let mut lines: Vec<TokenStream2> = vec![quote!(buf.push_str(Self::JSON_TEMPLATE[0]);)];
    for (i, field) in data_struct.fields.iter().enumerate() {
        let Some(ident) = field.ident.as_ref() else {
            panic!("tuple struct is not supported")
        };
        let ty = &field.ty;
        let template_idx = i + 1;
        lines.push(quote! { <#ty as crate::fw::SerializeJson>::ser(&self.#ident, buf); });
        lines.push(quote! { buf.push_str(Self::JSON_TEMPLATE[#template_idx]); });

        let skip = field.attrs.iter().any(|x| {
            x.path().is_ident("serjson") && x.parse_args::<Ident>().unwrap() == "skip_if_none"
        });

        if skip {
            let len = data_struct.fields.len();
            assert!(i != 0, "skip_if_none on first field is not supported");
            assert!(i + 1 != len, "skip_if_none on last field is not supported");

            let next_header = lines.pop().unwrap();
            let this_ser = lines.pop().unwrap();
            let this_header = lines.pop().unwrap();

            lines.push(quote! {
                if self.#ident.is_some() {
                    #this_header
                    #this_ser
                }
                #next_header
            });
        }
    }

    let mut ser = quote!();
    for l in &lines {
        ser = quote!(#ser #l);
    }

    let expanded = quote! {
        impl #struct_name {
            const JSON_TEMPLATE: [&str; #template_len] = [ #json_template ];
            const JSON_TEMPLATE_SIZE: usize = #template_bytes;
        }
        impl crate::fw::SerializeJson for #struct_name {
            #[inline(always)]
            fn size_est(&self) -> usize {
                Self::JSON_TEMPLATE_SIZE + #child_size
            }
            fn ser(&self, buf: &mut String) {
                #ser
            }
        }
    };

    let ts = TokenStream::from(expanded);
    // eprintln!("{ts}");
    // for (i, t) in template.iter().enumerate() {
    //     eprintln!("{i:2}: {t}");
    // }
    // let t = prettyplease::unparse(&syn::parse_file(&format!("{ts}")).unwrap());
    // eprintln!("{t}");
    #[allow(clippy::let_and_return)]
    ts
}
