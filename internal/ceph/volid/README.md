# volid

CSI volume ID encoding and decoding.

## Overview

This package implements the encoding scheme for CSI volume identifiers used by the Ceph-CSI plugin. It provides reversible encoding/decoding of volume metadata into CSI-compliant identifier strings.

## Key Types

### CSIIdentifier

Structure containing all metadata needed to locate a Ceph volume:

**Fields**:
- `LocationID`: Pool ID (RBD) or filesystem ID (CephFS)
- `ClusterID`: Unique cluster identifier
- `ObjectUUID`: On-disk UUID of the volume/snapshot
- `encodingVersion`: Encoding scheme version (currently v1)

## Encoding Format (Version 1)

CSI IDs are composed as follows (max 128 bytes per CSI spec):

```
[version:4 hex] + [-] + [clusterID length:4 hex] + [-] + [clusterID:variable] +
[-] + [locationID:16 hex] + [-] + [objectUUID:36]
```

Example: `0001-0024-rook-ceph-cluster-1234-00000000000003e8-f47ac10b-58cc-4372-a567-0e02b2c3d479`

## Key Methods

- **ComposeCSIID()**: Encodes CSIIdentifier into string format
- **DecomposeCSIID(composedID)**: Decodes string back into CSIIdentifier structure

## Usage

```go
id := volid.CSIIdentifier{
    LocationID: 1000,
    ClusterID:  "my-cluster",
    ObjectUUID: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
}
csiID, err := id.ComposeCSIID()

// Later, decode it back
var decoded volid.CSIIdentifier
err = decoded.DecomposeCSIID(csiID)
```

## Constraints

- Maximum CSI ID length: 128 bytes
- UUID must be exactly 36 characters
- ClusterID length is dynamically encoded to optimize space usage
