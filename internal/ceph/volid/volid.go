/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volid

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidVolID = errors.New("invalid volume ID")
)

// CSIIdentifier represents a decoded CSI volume or snapshot identifier.
// The format is: <clusterID>-<locationID>-<objectUUID>
type CSIIdentifier struct {
	ClusterID  string
	LocationID int64
	ObjectUUID string
}

// DecomposeCSIID decodes a CSI volume or snapshot handle into its components.
// Format: <clusterID>-<poolID>-<objectUUID> where objectUUID is hex-encoded.
func (ci *CSIIdentifier) DecomposeCSIID(csiID string) error {
	parts := strings.Split(csiID, "-")
	if len(parts) < 3 {
		return fmt.Errorf("%w: expected at least 3 parts, got %d", ErrInvalidVolID, len(parts))
	}

	ci.ClusterID = parts[0]

	var locationID int64
	_, err := fmt.Sscanf(parts[1], "%d", &locationID)
	if err != nil {
		return fmt.Errorf("%w: failed to parse locationID: %v", ErrInvalidVolID, err)
	}
	ci.LocationID = locationID

	objectUUID := strings.Join(parts[2:], "-")
	if _, err := hex.DecodeString(objectUUID); err != nil {
		return fmt.Errorf("%w: invalid hex-encoded objectUUID: %v", ErrInvalidVolID, err)
	}
	ci.ObjectUUID = objectUUID

	return nil
}
