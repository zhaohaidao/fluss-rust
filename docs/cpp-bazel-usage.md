# Fluss C++ Bazel Usage Guide

This guide is for both:

- C++ application teams consuming Fluss C++ bindings.
- Maintainers publishing and evolving the Bazel module.

The current version focuses on **source dependency** with bzlmod.
Prebuilt static/shared dependency modes are listed as TODOs.

## Scope

- Dependency model: **root module mode**.
- Consumer dependency target: `@red-fluss-rust//:fluss_cpp`.
- Version governance: explicit version pinning in `MODULE.bazel`.

## Source Dependency (Recommended)

### 1. Pin version in consumer `MODULE.bazel`

```starlark
module(name = "ris")

bazel_dep(name = "red-fluss-rust", version = "0.1.0")
```

For local development only, you can temporarily use:

```starlark
local_path_override(
    module_name = "red-fluss-rust",
    path = "/path/to/fluss-rust",
)
```

Do not keep local overrides in long-lived mainline branches.

### 2. Depend on Fluss C++ target in consumer `BUILD.bazel`

```starlark
cc_binary(
    name = "ris_reader",
    srcs = ["reader.cc"],
    deps = ["@red-fluss-rust//:fluss_cpp"],
)
```

### 3. Build and run

```bash
bazel build //:ris_reader
bazel run //:ris_reader
```

## Upgrade Procedure

Use this fixed flow to reduce dependency conflict troubleshooting cost:

1. Update `bazel_dep(name = "red-fluss-rust", version = "...")`.
2. Run `bazel mod tidy`.
3. Commit both `MODULE.bazel` and `MODULE.bazel.lock`.
4. Run consumer build + integration tests.
5. Verify dependency graph:

```bash
bazel mod graph | rg "red-fluss-rust@"
```

## Maintainer Notes

For root module mode:

- Keep root `MODULE.bazel` as the release source of truth.
- Keep root `BUILD.bazel` aliases stable for consumers (`//:fluss_cpp`).
- Avoid breaking consumer labels without deprecation/migration guidance.

## TODO

- Document prebuilt static dependency mode (`libfluss_cpp.a`).
- Document prebuilt shared dependency mode (`libfluss_cpp.so` / `.dylib`).
