/*
Copyright 2026.

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

package common

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// EphemeralCert holds a self-signed certificate and
// its private key for direct TLS connections.
type EphemeralCert struct {
	CertPEM     []byte
	TLSCert     tls.Certificate
	Fingerprint [sha256.Size]byte
}

// GenerateEphemeralCert creates a short-lived
// self-signed ECDSA P-256 certificate valid for 1 hour.
func GenerateEphemeralCert() (*EphemeralCert, error) {
	key, err := ecdsa.GenerateKey(
		elliptic.P256(), rand.Reader,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"generate ECDSA key: %w", err,
		)
	}

	serial, err := rand.Int(
		rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"generate serial: %w", err,
		)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "ceph-volsync-ephemeral",
		},
		NotBefore:             now,
		NotAfter:              now.Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(
		rand.Reader, template, template, &key.PublicKey, key,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create certificate: %w", err,
		)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: certDER,
	})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal key: %w", err,
		)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type: "EC PRIVATE KEY", Bytes: keyDER,
	})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf(
			"parse key pair: %w", err,
		)
	}

	return &EphemeralCert{
		CertPEM:     certPEM,
		TLSCert:     tlsCert,
		Fingerprint: sha256.Sum256(certDER),
	}, nil
}

// PeerFingerprint computes the SHA-256 fingerprint of
// a PEM-encoded certificate.
func PeerFingerprint(
	certPEM []byte,
) ([sha256.Size]byte, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return [sha256.Size]byte{}, fmt.Errorf(
			"failed to decode PEM certificate",
		)
	}
	return sha256.Sum256(block.Bytes), nil
}

// fingerprintVerifier returns a VerifyPeerCertificate
// callback that checks the peer's raw certificate
// fingerprint matches the expected value.
// Used with InsecureSkipVerify=true — this is NOT
// insecure because fingerprint pinning replaces
// hostname/SAN verification (needed since the client
// dials via K8s Service IP which won't match the
// pod's cert SAN).
func fingerprintVerifier(
	expected [sha256.Size]byte,
) func([][]byte, [][]*x509.Certificate) error {
	return func(
		rawCerts [][]byte,
		_ [][]*x509.Certificate,
	) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("no peer certificate")
		}
		actual := sha256.Sum256(rawCerts[0])
		if actual != expected {
			return fmt.Errorf(
				"certificate fingerprint mismatch",
			)
		}
		return nil
	}
}

// NewServerTLSConfig creates a TLS config for the
// direct TLS listener with mTLS and fingerprint
// pinning of the client certificate.
func NewServerTLSConfig(
	serverCert *EphemeralCert, clientCertPEM []byte,
) (*tls.Config, error) {
	peerFP, err := PeerFingerprint(clientCertPEM)
	if err != nil {
		return nil, fmt.Errorf(
			"compute client fingerprint: %w", err,
		)
	}
	return &tls.Config{
		Certificates:          []tls.Certificate{serverCert.TLSCert},
		ClientAuth:            tls.RequireAnyClientCert,
		InsecureSkipVerify:    true, //nolint:gosec // G402: fingerprint pinning replaces SAN verification
		VerifyPeerCertificate: fingerprintVerifier(peerFP),
		MinVersion:            tls.VersionTLS13,
	}, nil
}

// NewClientTLSConfig creates a TLS config for direct
// TLS connections with mTLS and fingerprint pinning
// of the server certificate.
func NewClientTLSConfig(
	clientCert *EphemeralCert, serverCertPEM []byte,
) (*tls.Config, error) {
	peerFP, err := PeerFingerprint(serverCertPEM)
	if err != nil {
		return nil, fmt.Errorf(
			"compute server fingerprint: %w", err,
		)
	}
	return &tls.Config{
		Certificates:          []tls.Certificate{clientCert.TLSCert},
		InsecureSkipVerify:    true, //nolint:gosec // G402: fingerprint pinning replaces SAN verification
		VerifyPeerCertificate: fingerprintVerifier(peerFP),
		MinVersion:            tls.VersionTLS13,
	}, nil
}
