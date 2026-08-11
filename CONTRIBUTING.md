# Contributing to Caminus CDC Engine

Thank you for your interest in contributing to **Caminus**! We welcome bug reports, feature proposals, documentation improvements, and pull requests.

---

## Development Setup

### Prerequisites
- **Rust Toolchain**: Rust 1.80+ (`rustup default stable`)
- **C++ Compiler & Build Tools**: `clang`, `pkg-config`, `libssl-dev` (required for static linking `librocksdb-sys`)

### Building & Running Tests
```bash
# Clone the repository
git clone https://github.com/ichbingautam/caminus.git
cd caminus

# Run the complete unit test and integration benchmark suite
cargo test

# Check code formatting & clippy warnings
cargo fmt --check
cargo clippy --all-targets --all-features
```

---

## Pull Request Guidelines

1. **Branch Naming**: Use prefixed branch names:
   - `feat/feature-description`
   - `fix/bug-description`
   - `docs/documentation-update`
2. **Commit Conventions**: Format commit messages using standard prefixes:
   - `[Feat]: Implemented WASM SMT transformation memory limit`
   - `[Fix]: Resolved replication slot failover lease race condition`
   - `[Chore]: Updated dependency versions`
3. **Automated Test Coverage**: Every pull request introducing new features or bug fixes must include unit or integration tests in `src/` or `tests/`.
4. **Branch Protection**: All PRs target the `main` branch and require at least 1 approving code review before merging.

---

## Code of Conduct

We are committed to providing a welcoming, respectful environment for all contributors. Please ensure conversations and reviews remain polite, objective, and professional.
