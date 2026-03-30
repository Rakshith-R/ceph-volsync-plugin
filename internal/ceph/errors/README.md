# errors

Ceph-specific error types.

## Overview

Defines sentinel errors for common Ceph-related failure conditions that may require special handling.

## Error Types

- **ErrKeyNotFound**: Returned when a requested key is not found
- **ErrPoolNotFound**: Returned when a Ceph pool does not exist

## Usage

These errors can be checked using `errors.Is()`:

```go
if errors.Is(err, cepherr.ErrPoolNotFound) {
    // Handle pool not found
}
```
