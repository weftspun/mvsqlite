import Lake
open Lake DSL

package «mvsqlite-verify» where

require «plausible-witness-dag» from git
  "https://github.com/fire/plausible-witness-dag" @ "main"

@[default_target] lean_exe «mvsqlite-verify» where
  root := `MvsqliteVerify
  moreLinkArgs := #[
    "../target/release/libmvsqlite_verify_ffi.a",
    "-lpthread", "-ldl", "-lm", "-lrt"
  ]
