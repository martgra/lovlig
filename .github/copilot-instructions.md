# Copilot Instructions for Lovdata Processing

## Project Context

This is an **MVP (Minimum Viable Product)** for processing Lovdata legal datasets. We prioritize:

- **Speed over perfection** - Ship working features fast
- **No over-engineering** - YAGNI (You Aren't Gonna Need It)
- **No backward compatibility** - We're building fresh, not maintaining legacy
- **No migration code** - No transition periods, just implement the new way
- **Simple solutions** - Choose the straightforward approach

## Tech Stack

- **Python 3.11+** with type hints
- **uv** for dependency management (NOT pip, NOT poetry)
- **Pydantic** for models and validation
- **xxHash** for file change detection (fast, non-cryptographic)
- **Rich** for terminal UI
- **Typer** for CLI
- **pytest** for testing

## Development Workflow

### Package Management

```bash
# Install dependencies
uv sync

# Add new dependency
uv add package-name

# Run commands
uv run pytest
uv run lov <command>
```

**NEVER use pip or virtualenv directly** - always use `uv`.

## Coding Standards

### General Principles

1. **Keep it simple** - Prefer 10 lines of clear code over 100 lines of "clever" code
2. **No premature optimization** - Make it work first, optimize if needed
3. **No backward compatibility** - Change things if the new way is better
4. **No deprecation warnings** - Just remove old code
5. **No migration helpers** - Update code directly to new patterns

### Python Style

- Use **type hints** everywhere
- Prefer **Pydantic models** over dicts
- Use **Path** objects, not strings for file paths
- Keep functions **under 30 lines** when possible
- **No `# type: ignore`** comments - fix the types

### Naming Conventions

- `snake_case` for variables, functions, methods
- `PascalCase` for classes
- `SCREAMING_SNAKE_CASE` for constants
- Prefix private methods with `_`

### Module Structure

```python
# 1. Docstring
"""Module description."""

# 2. Imports (stdlib → third-party → local)
import os
from pathlib import Path

import xxhash
from pydantic import BaseModel

from lovlig.domain import FileMetadata

# 3. __all__ export (define public API)
__all__ = ["public_function", "PublicClass"]

# 4. Code
```

### Error Handling

```python
# Good: Specific, actionable errors
if not file_path.exists():
    raise FileNotFoundError(f"File not found: {file_path}")

# Bad: Generic errors
if not file_path.exists():
    raise Exception("Error!")
```

### Configuration

- Use **pydantic-settings** with environment variables
- Prefix env vars with `LOVDATA_`
- Always provide sensible defaults
- No complex validation - keep it simple

```python
# Good
class Settings(BaseSettings):
    api_timeout: int = 30

# Bad - over-engineered
class Settings(BaseSettings):
    api_timeout: int = Field(
        default=30,
        ge=1,
        le=300,
        description="API timeout in seconds with...",
        json_schema_extra={"examples": [30, 60]}
    )
```

## Testing

### Test File Naming

- **MUST use `*_test.py` pattern** - enforced by pre-commit hooks
- Examples: `hash_algorithms_test.py`, `config_test.py`, `services_test.py`
- **NOT** `test_*.py` - this will fail pre-commit

### Test Structure

```python
class TestFeatureName:
    """Test feature behavior."""

    def test_happy_path(self):
        """Test normal usage."""
        ...

    def test_edge_case(self):
        """Test boundary condition."""
        ...
```

### Test Guidelines

- Use **descriptive test names** - `test_xxhash_faster_than_sha256`
- Use **tmp_path** fixture for file operations
- Keep tests **focused** - one thing per test
- **Don't mock everything** - use real objects when simple
- Test **behavior**, not implementation

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test
uv run pytest tests/unit/config_test.py::TestClass::test_method -v

# With coverage
uv run pytest --cov=lovlig
```

## Performance

### File Hashing

- **Always use xxHash** (`compute_file_hash` with default algorithm)
- xxHash is 10-27x faster than SHA256
- SHA256 is ONLY for backward compatibility (deprecated)

```python
# Good - fast
hash_value = compute_file_hash(file_path)  # Uses xxh128

# Bad - slow (only for legacy files)
hash_value = compute_file_hash(file_path, algorithm="sha256")
```

### Concurrency

- Use **ThreadPoolExecutor** for I/O-bound operations
- Default workers: `min(32, os.cpu_count())`
- Keep it simple - no async unless really needed

## Documentation

### Code Comments

```python
# Good: Explain WHY, not WHAT
# Use xxHash instead of SHA256 for 10x speedup
hash_value = compute_file_hash(path)

# Bad: States the obvious
# Compute the hash
hash_value = compute_file_hash(path)
```

### Docstrings

```python
def process_file(path: Path) -> FileMetadata:
    """Process file and compute hash.

    Args:
        path: File to process

    Returns:
        File metadata with xxHash
    """
```

Keep docstrings **short and useful**. No need for novels.

## Anti-Patterns to Avoid

### ❌ Over-Engineering

```python
# Bad - over-engineered
class AbstractFileHasherFactory:
    def create_hasher(self, algorithm: str) -> IHasher:
        ...

# Good - simple
def compute_file_hash(path: Path, algorithm: str = "xxh128") -> str:
    ...
```

### ❌ Premature Abstraction

```python
# Bad - unnecessary abstraction
class ConfigurationRepository:
    def get_config(self) -> Settings:
        return self._config_store.retrieve()

# Good - direct
def get_config() -> Settings:
    return Settings()
```

### ❌ Backward Compatibility

```python
# Bad - unnecessary compatibility layer
class PipelineConfig(Settings):
    def __init__(self, **data):
        warnings.warn("Use Settings instead", DeprecationWarning)
        super().__init__(**data)

# Good - just use the new way
class Settings(BaseSettings):
    ...
```

### ❌ Migration Code

```python
# Bad - migration logic
if previous_meta.hash_algorithm == "sha256":
    old_hash = compute_file_hash(path, algorithm="sha256")
    is_modified = previous_meta.sha256 != old_hash
else:
    is_modified = previous_meta.sha256 != new_hash

# Good - just use new format
new_hash = compute_file_hash(path)
is_modified = previous_meta.sha256 != new_hash
```

## Remember

> "Make it work, make it right, make it fast - in that order."

This is an **MVP**. We ship working code fast. No need for:

- Complex abstractions
- Migration strategies
- Backward compatibility
- Future-proofing
- Enterprise patterns

**Just make it work, keep it simple, and ship it.**
