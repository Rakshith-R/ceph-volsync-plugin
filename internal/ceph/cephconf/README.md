# cephconf

Ceph configuration file generation utilities.

## Overview

This package provides functions to create a basic Ceph configuration file (`/etc/ceph/ceph.conf`) and keyring file, making it easy to use Ceph-related CLI tools within the plugin's runtime environment.

## Key Functions

- **WriteCephConfig()**: Creates `/etc/ceph/ceph.conf` with basic cephx authentication settings and an empty keyring file at `/etc/ceph/keyring`

## Usage

The configuration file is automatically created when establishing connections to Ceph clusters. It enables proper authentication for all Ceph CLI operations.

## Constants

- `CephConfigPath`: Path to the Ceph configuration file (`/etc/ceph/ceph.conf`)
