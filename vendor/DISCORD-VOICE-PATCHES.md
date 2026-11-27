# Discord voice dependency backports

These crates preserve the published interfaces used by Mercury's Discord voice
adapter and update vulnerable transitive dependencies. The sources and licenses
come from the listed crates.io releases and their recorded upstream revisions.

| Crate | Published source | Mercury patch |
| --- | --- | --- |
| Songbird 0.6.1 | [957e17f](https://github.com/serenity-rs/songbird/tree/957e17fcbc687cd88979c90b76bdf79017803b09) | Apply the ringbuf upgrade from [Songbird #312](https://github.com/serenity-rs/songbird/pull/312). Select the Serenity TLS dependency only when the Serenity feature is enabled. Mercury supplies its own Discord gateway. |
| Davey 0.1.5 | [73bcec2](https://github.com/Snazzah/davey/tree/73bcec27b7190ce47088c831e0aa72600cc657e7) | Use the current P-256 key generation API with OS randomness. Keep the existing private key and SEC1 public key encodings. |
| OpenMLS Basic Credential 0.5.0 | [6b85f0e](https://github.com/openmls/openmls/tree/6b85f0edc560b4fe0f5b9266092947a774614f3f/basic_credential) | Backport the signing and key generation updates from [OpenMLS #2249](https://github.com/openmls/openmls/pull/2249). |
| OpenMLS Rust Crypto 0.5.1 | [47dbede](https://github.com/openmls/openmls/tree/47dbedecad0c1fd8eb5368d582250ebfcc1e1ce6/openmls_rust_crypto) | Backport the signing updates from [OpenMLS #2249](https://github.com/openmls/openmls/pull/2249) and the HPKE 0.8 update from [OpenMLS #2261](https://github.com/openmls/openmls/pull/2261). Keep the OpenMLS 0.5 provider traits and storage API used by Davey. |

Remove these overrides as compatible upstream releases pick up the fixes.
