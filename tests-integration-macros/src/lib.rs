// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeSet;

use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Ident;
use syn::ItemFn;
use syn::Meta;
use syn::Token;
use syn::parse::Parser;
use syn::parse_macro_input;
use syn::punctuated::Punctuated;

#[derive(Clone, Copy)]
struct RuntimeSpec {
    name: &'static str,
    type_name: &'static str,
    capabilities: &'static [&'static str],
}

const RUNTIMES: &[RuntimeSpec] = &[
    RuntimeSpec {
        name: "tokio",
        type_name: "Tokio",
        capabilities: &["time"],
    },
    RuntimeSpec {
        name: "smol",
        type_name: "Smol",
        capabilities: &["time"],
    },
    RuntimeSpec {
        name: "compio",
        type_name: "Compio",
        capabilities: &["time"],
    },
];

#[derive(Default)]
struct RuntimeTestArgs {
    only: Option<BTreeSet<String>>,
    required: BTreeSet<String>,
}

/// Runs an async test on every configured runtime which has the requested
/// capabilities.
///
/// Supported forms are `#[runtime_test]`,
/// `#[runtime_test(require(time))]`, and
/// `#[runtime_test(only(compio))]`.
///
/// Each generated test body receives a `runtime` type alias whose operations
/// are statically bound to that wrapper's runtime. The annotated function does
/// not need a runtime generic parameter.
#[proc_macro_attribute]
pub fn runtime_test(attribute: TokenStream, item: TokenStream) -> TokenStream {
    let args = match parse_args(attribute) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    let function = parse_macro_input!(item as ItemFn);

    expand_runtime_test(args, function)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn parse_args(attribute: TokenStream) -> syn::Result<RuntimeTestArgs> {
    let attributes = Punctuated::<Meta, Token![,]>::parse_terminated.parse(attribute)?;
    let mut args = RuntimeTestArgs::default();

    for attribute in attributes {
        let Meta::List(list) = attribute else {
            return Err(syn::Error::new_spanned(
                attribute,
                "expected `require(...)` or `only(...)`",
            ));
        };
        let values = list.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;

        if list.path.is_ident("require") {
            for value in values {
                let capability = value.to_string();
                if capability != "time" {
                    return Err(syn::Error::new_spanned(value, "unknown runtime capability"));
                }
                args.required.insert(capability);
            }
        } else if list.path.is_ident("only") {
            if args.only.is_some() {
                return Err(syn::Error::new_spanned(list, "duplicate `only(...)`"));
            }
            let mut runtimes = BTreeSet::new();
            for value in values {
                let runtime = value.to_string();
                if !RUNTIMES.iter().any(|spec| spec.name == runtime) {
                    return Err(syn::Error::new_spanned(value, "unknown runtime"));
                }
                runtimes.insert(runtime);
            }
            args.only = Some(runtimes);
        } else {
            return Err(syn::Error::new_spanned(
                list.path,
                "expected `require(...)` or `only(...)`",
            ));
        }
    }

    Ok(args)
}

fn expand_runtime_test(
    args: RuntimeTestArgs,
    function: ItemFn,
) -> syn::Result<proc_macro2::TokenStream> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "`runtime_test` requires an async function",
        ));
    }
    if !function.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "`runtime_test` functions cannot take arguments",
        ));
    }

    if !function.sig.generics.params.is_empty() || function.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &function.sig.generics,
            "`runtime_test` functions cannot declare generics",
        ));
    }

    let selected = RUNTIMES
        .iter()
        .copied()
        .filter(|runtime| {
            args.only
                .as_ref()
                .is_none_or(|only| only.contains(runtime.name))
        })
        .filter(|runtime| {
            args.required
                .iter()
                .all(|required| runtime.capabilities.contains(&required.as_str()))
        })
        .collect::<Vec<_>>();

    if selected.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "no configured runtime satisfies this test",
        ));
    }

    let attributes = &function.attrs;
    let body = &function.block;
    let function_name = &function.sig.ident;
    let wrappers = selected.into_iter().map(|runtime| {
        let runtime_type = format_ident!("{}", runtime.type_name);
        let wrapper_name = format_ident!("{}_{}", function_name, runtime.name);

        quote! {
            #(#attributes)*
            #[test]
            fn #wrapper_name() {
                ::tests_integration::runtime::run::<
                    ::tests_integration::runtime::#runtime_type,
                    _,
                    _,
                >(|| async move {
                    #[allow(non_camel_case_types)]
                    type runtime = ::tests_integration::runtime::RuntimeOps<
                        ::tests_integration::runtime::#runtime_type
                    >;

                    #body
                });
            }
        }
    });

    Ok(quote! {
        #(#wrappers)*
    })
}
