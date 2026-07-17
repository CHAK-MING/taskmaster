# Persistent State Backup and Restore

## Scope

DAGForge file storage is a single-writer state directory containing the Plan catalog, Run checkpoints, Evidence log, Artifact data and metadata, and `.dagforge.lock`. A backup is valid only when it captures the complete directory at one quiescent point. Do not combine subdirectories or individual files from different backups.

The current pre-release reader accepts only explicit version-1 storage envelopes. Unversioned development state and unsupported future versions fail closed. Keep the matching DAGForge release archive and configuration with every backup.

## Stop and verify ownership

Stop the DAGForge process cleanly and wait for it to exit. Then verify that the state lock is available before copying. Replace `/var/lib/dagforge/state` with the configured `storage.directory`.

```bash
state=/var/lib/dagforge/state
exec 9<>"$state/.dagforge.lock"
flock -n 9 || { echo "DAGForge still owns the state directory" >&2; exit 1; }
```

Do not copy a live state directory. The advisory lock prevents a second DAGForge Application from opening the directory, but ordinary backup tools do not automatically participate in that lock.

## Create a backup

Preserve the entire directory tree, permissions, ownership, sparse files, and timestamps. The example creates a deterministic compressed archive next to a configuration copy and checksum.

```bash
state=/var/lib/dagforge/state
backup=/var/backups/dagforge
stamp=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup"
tar --numeric-owner --acls --xattrs --sparse -C "$(dirname "$state")" -czf "$backup/state-$stamp.tar.gz" "$(basename "$state")"
install -m 0600 /etc/dagforge/system_config.json "$backup/system_config-$stamp.json"
sha256sum "$backup/state-$stamp.tar.gz" "$backup/system_config-$stamp.json" > "$backup/sha256sums-$stamp.txt"
```

Record the DAGForge release version, Minijail revision, host architecture, filesystem type, and backup command output with the archive. Store backups outside the state filesystem and apply the same confidentiality controls as the Workflow inputs and outputs they contain.

## Validate a backup

Verify checksums before extraction. Extract into an isolated staging directory, require one expected state root, reject symlinks or group/world-writable paths, and inspect the envelope files before using the copy for a recovery drill.

```bash
backup=/var/backups/dagforge
stamp=YYYYMMDDTHHMMSSZ
stage=$(mktemp -d)
cd "$backup"
sha256sum -c "sha256sums-$stamp.txt"
tar --no-same-owner --no-overwrite-dir -xzf "state-$stamp.tar.gz" -C "$stage"
find "$stage" -type l -print -quit | grep -q . && { echo "backup contains a symlink" >&2; exit 1; }
find "$stage" -perm /022 -print -quit | grep -q . && { echo "backup contains group/world-writable state" >&2; exit 1; }
```

For a full recovery drill, use a copy of the extracted state, configure API listeners on isolated ports, start the exact DAGForge release that created the backup, confirm startup recovery and Artifact reconciliation, exercise read-only Plan/Run/Artifact queries, then discard the drill copy. Startup may repair one incomplete final Evidence fragment or compact retained Evidence, so never perform a validation drill against the only backup copy.

## Restore

Stop DAGForge, move the current state directory aside as one rollback unit, extract the backup into an empty parent directory, and restore the service owner and restrictive permissions. The persistent lock file may contain a stale PID string; it is safe to retain because ownership is determined by the live advisory lock and the PID is overwritten on acquisition.

```bash
state=/var/lib/dagforge/state
backup=/var/backups/dagforge/state-YYYYMMDDTHHMMSSZ.tar.gz
service_user=dagforge
service_group=dagforge
mv "$state" "$state.before-restore"
mkdir -p "$(dirname "$state")"
tar --numeric-owner --acls --xattrs --sparse -C "$(dirname "$state")" -xzf "$backup"
chown -R "$service_user:$service_group" "$state"
chmod 0700 "$state"
chmod 0600 "$state/.dagforge.lock"
```

Start DAGForge with the matching configuration. Startup must acquire `.dagforge.lock`, validate every managed catalog entry, open Evidence, reconcile Artifact pairs, restore Plans before Checkpoints, and fail closed on corruption, unsupported storage versions, unsafe permissions, or oversized files. Treat any startup error as a restore failure; do not delete the rollback directory until Workflow and Artifact checks pass.

## Rollback

Stop the failed restored instance, move the restored directory aside, and rename `state.before-restore` back to the configured path. Never merge a failed restore with the previous state. If both copies must be retained for investigation, keep them read-only and outside the active path.

## Retention and testing

Maintain multiple dated backups and test restoration regularly on an isolated host. A successful archive command is not evidence of recoverability; the acceptance criterion is a clean startup with the matching binary, expected Plan and Run inventory, readable retained Artifacts, and no unexplained reconciliation debt.
