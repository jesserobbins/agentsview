package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// mergeSessionIDTables lists every table that stores a session id outside the
// sessions row itself. A machine merge must rewrite the id in each of them so
// renamed sessions keep their messages, tool calls, usage, and queue state.
// Trigger-maintained journals (session_project_identity_snapshot_changes,
// session_deletion_changes) are deliberately absent: revision triggers on the
// tables above record the merge, and the deletion ledger is written explicitly
// for renamed ids.
var mergeSessionIDTables = []string{
	"messages",
	"artifact_export_queue",
	"artifact_publications",
	"usage_events",
	"tool_calls",
	"tool_result_events",
	"recall_evidence",
	"recall_extract_progress",
	"pinned_messages",
	"starred_sessions",
	"local_session_source_baselines",
	"session_project_assignments",
	"session_project_identity_snapshots",
	"subagent_parent_repair_queue",
	"subagent_parent_cleanup_queue",
	"secret_findings",
	"tool_call_occurrence_agent_state",
	"parser_checkpoints",
	"parser_checkpoint_blobs",
	"session_signal_state",
	"conversation_messages",
	"conversation_session_changes",
	"project_identity_observations",
}

// mergeMachineTables lists tables that key rows by machine outside sessions.
// Change-log tables (project_identity_observation_changes,
// worktree_project_mapping_changes,
// session_project_identity_snapshot_changes) are deliberately absent: they
// are fed by revision triggers on the tables above, which record the merge
// as new change rows for downstream mirrors.
var mergeMachineTables = []string{
	"local_session_source_baselines",
	"worktree_project_mappings",
	"project_identity_observations",
	"session_project_identity_snapshots",
}

// mergeSessionRefColumns lists sessions/tool_calls columns whose values are
// session ids that may carry the merged machine's id prefix and must be
// rewritten alongside the id sweep.
var mergeSessionRefColumns = []struct {
	table  string
	column string
}{
	{"sessions", "parent_session_id"},
	{"sessions", "parser_parent_session_id"},
	{"sessions", "source_session_id"},
	{"tool_calls", "subagent_session_id"},
}

// MergeMachineIdentities folds one or more historical remote-host machine keys
// into a single target machine key, repairing the fragmentation a remote host
// rename causes: the same physical machine accumulates sessions under each
// name it was configured under. Session ids are rewritten to the target
// prefix; sessions whose target-prefixed id already exists (the same session
// synced under both names) are deleted as duplicates, and their old ids are
// excluded so a re-added historical host cannot resurrect them. The source
// keys remain filter redirects through machine aliases.
//
// Callers must hold the archive write-owner lock, with ingestion stopped.
func (db *DB) MergeMachineIdentities(
	ctx context.Context, installationID, target string, sources []string,
) error {
	target = strings.TrimSpace(target)
	if target == "" || target == "local" {
		return errors.New("merge target machine key is required")
	}
	if len(sources) == 0 {
		return errors.New("select one or more source machine keys to merge")
	}
	seen := make(map[string]bool, len(sources)+1)
	seen[target] = true
	for _, source := range sources {
		source = strings.TrimSpace(source)
		if source == "" {
			return errors.New("source machine key must not be empty")
		}
		if source == target {
			return fmt.Errorf("source machine %q is the merge target", source)
		}
		if seen[source] {
			return fmt.Errorf("source machine %q listed twice", source)
		}
		seen[source] = true
	}
	if target == installationID {
		return errors.New(
			"merge target is this installation's key; use db adopt-machine to claim history instead",
		)
	}

	return db.Update(ctx, func(tx *sql.Tx) error {
		if err := lockArtifactPublicationTx(ctx, tx); err != nil {
			return err
		}
		// Session-id rewrites temporarily break foreign keys from messages and
		// tool_calls to sessions; defer every check to commit, where the sweep
		// has left a consistent archive.
		if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = 1`); err != nil {
			return fmt.Errorf("deferring foreign keys: %w", err)
		}
		for _, raw := range sources {
			if err := mergeMachineRowsTx(ctx, tx, target, strings.TrimSpace(raw)); err != nil {
				return err
			}
		}
		return nil
	})
}

func mergeMachineRowsTx(ctx context.Context, tx *sql.Tx, target, source string) error {
	var known bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM (`+archiveMachineKeysSQL+`)
		WHERE machine = ?)`, source).Scan(&known); err != nil {
		return err
	}
	if !known {
		return fmt.Errorf("machine %q is not recorded in this archive", source)
	}
	var aliased string
	err := tx.QueryRowContext(ctx, `SELECT value FROM pg_sync_state
		WHERE key = ?`, MachineAliasKeyPrefix+source).Scan(&aliased)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if aliased != "" && aliased != target {
		return fmt.Errorf(
			"machine %q is already a filter redirect to %q; merge that key instead", source, aliased,
		)
	}

	prefix := source + "~"
	targetPrefix := target + "~"
	prefixLen := len(prefix)

	// Duplicates: the same session synced under both names. The target row
	// is authoritative (the active host sync keeps it fresh), so the source
	// row and its side rows are deleted and both ids are excluded from any
	// future sync under the historical name.
	duplicates, err := machineMergeDuplicateIDsTx(ctx, tx, source, prefix, targetPrefix)
	if err != nil {
		return err
	}
	for _, id := range duplicates {
		if err := deleteSessionMessagesTx(tx, id); err != nil {
			return fmt.Errorf("deleting duplicate session %s messages: %w", id, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM sessions WHERE id = ?`, id); err != nil {
			return fmt.Errorf("deleting duplicate session %s: %w", id, err)
		}
		if err := deleteMergedSessionRowsTx(ctx, tx, id); err != nil {
			return fmt.Errorf("deleting duplicate session %s side rows: %w", id, err)
		}
		if err := excludeSessionIDTx(ctx, tx, id); err != nil {
			return fmt.Errorf("excluding duplicate session %s: %w", id, err)
		}
	}

	// Kept sessions: rewrite the id prefix in every referencing table, then
	// in the sessions row itself. Session id columns and ref columns are
	// rewritten with SQL prefix arithmetic so the sweep stays proportional
	// to the archive, not quadratic in session count.
	for _, table := range mergeSessionIDTables {
		if err := rewritePrefixedSessionColumnTx(ctx, tx, table, "session_id",
			prefix, prefixLen, targetPrefix); err != nil {
			return fmt.Errorf("rewriting %s.session_id: %w", table, err)
		}
	}
	for _, ref := range mergeSessionRefColumns {
		if err := rewritePrefixedSessionColumnTx(ctx, tx, ref.table, ref.column,
			prefix, prefixLen, targetPrefix); err != nil {
			return fmt.Errorf("rewriting %s.%s: %w", ref.table, ref.column, err)
		}
	}
	// Renamed ids vanish from the archive; record them in the deletion ledger
	// the same way the sessions DELETE trigger does, so any downstream mirror
	// prunes the old-prefixed rows once the renamed sessions republish.
	if _, err := tx.ExecContext(ctx, `INSERT INTO archive_metadata (key, value)
		VALUES ('session_deletion_publication_revision', '1')
		ON CONFLICT(key) DO UPDATE SET
			value = CAST(CAST(value AS INTEGER) + 1 AS TEXT),
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')`); err != nil {
		return fmt.Errorf("bumping deletion ledger revision for %q: %w", source, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_deletion_changes
		(session_id, project, revision, deleted)
		SELECT s.id, s.project, CAST(m.value AS INTEGER), 1
		FROM sessions s CROSS JOIN archive_metadata m
		WHERE s.machine = ? AND substr(s.id, 1, ?) = ?
			AND m.key = 'session_deletion_publication_revision'
		ON CONFLICT(session_id) DO UPDATE SET
			project = excluded.project, revision = excluded.revision, deleted = 1`,
		source, prefixLen, prefix); err != nil {
		return fmt.Errorf("journaling renamed ids for %q: %w", source, err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE sessions
		SET id = ? || substr(id, ?),
		    machine = ?,
		    local_modified_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE machine = ? AND substr(id, 1, ?) = ?`,
		targetPrefix, prefixLen+1, target, source, prefixLen, prefix); err != nil {
		return fmt.Errorf("renaming sessions for %q: %w", source, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions
		SET machine = ?,
		    local_modified_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE machine = ?`, target, source); err != nil {
		return fmt.Errorf("adopting sessions for %q: %w", source, err)
	}

	// Worktree rules follow the adopt-machine conflict contract: refuse
	// conflicting duplicates rather than guessing a winner.
	var conflict string
	err = tx.QueryRowContext(ctx, `
		SELECT old.path_prefix FROM worktree_project_mappings old
		JOIN worktree_project_mappings target ON target.machine = ? AND target.path_prefix = old.path_prefix
		WHERE old.machine = ? AND (old.project != target.project OR old.layout != target.layout
			OR old.enabled != target.enabled OR old.original_project != target.original_project)
		LIMIT 1`, target, source).Scan(&conflict)
	if err == nil {
		return fmt.Errorf(
			"conflicting worktree rules for %q on %q and %q; reconcile these rules before merging the machine",
			conflict, source, target,
		)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM worktree_project_mappings
		WHERE machine = ? AND path_prefix IN (
			SELECT path_prefix FROM worktree_project_mappings WHERE machine = ?)`,
		source, target); err != nil {
		return err
	}
	// Aggregate observations keep the newest observation of a root, matching
	// adopt-machine semantics. This must run before the machine-column sweep
	// so a surviving row can never collide with the target key's existing
	// (project, machine, root_path, git_remote) primary key.
	for _, pair := range [][2]string{{source, target}, {target, source}} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM project_identity_observations AS old
			WHERE old.machine = ? AND EXISTS (
				SELECT 1 FROM project_identity_observations target
				WHERE target.machine = ? AND target.project = old.project
					AND target.root_path = old.root_path AND target.git_remote = old.git_remote
					AND rtrim(target.observed_at, 'Z') >= rtrim(old.observed_at, 'Z')
			)`, pair[0], pair[1]); err != nil {
			return err
		}
	}
	for _, table := range mergeMachineTables {
		if _, err := tx.ExecContext(ctx,
			"UPDATE "+table+" SET machine = ? WHERE machine = ?", target, source); err != nil {
			return fmt.Errorf("merging machine column in %s: %w", table, err)
		}
	}

	// The historical host name is defunct: drop its transfer skip cache so a
	// re-added entry starts from a full sync, and keep the old key as a
	// filter redirect to the target.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM remote_skipped_files WHERE host = ?`, source); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pg_sync_state SET value = ?
		WHERE key LIKE 'machine\_alias:%' ESCAPE '\' AND value = ?`, target, source); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pg_sync_state(key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		MachineAliasKeyPrefix+source, target); err != nil {
		return err
	}
	return nil
}

// machineMergeDuplicateIDsTx lists source-machine session ids whose
// target-prefixed counterpart already exists.
func machineMergeDuplicateIDsTx(
	ctx context.Context, tx *sql.Tx, source, prefix, targetPrefix string,
) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM sessions
		WHERE machine = ? AND substr(id, 1, ?) = ?
		AND EXISTS (SELECT 1 FROM sessions t
			WHERE t.id = ? || substr(sessions.id, ?))`,
		source, len(prefix), prefix, targetPrefix, len(prefix)+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// deleteMergedSessionRowsTx removes a duplicate session's rows from every
// table that references it outside the messages cluster, which
// deleteSessionMessagesTx already handles.
func deleteMergedSessionRowsTx(ctx context.Context, tx *sql.Tx, id string) error {
	for _, table := range []string{
		"artifact_export_queue",
		"artifact_publications",
		"usage_events",
		"recall_evidence",
		"recall_extract_progress",
		"pinned_messages",
		"starred_sessions",
		"local_session_source_baselines",
		"session_project_assignments",
		"session_project_identity_snapshots",
		"subagent_parent_repair_queue",
		"subagent_parent_cleanup_queue",
		"secret_findings",
		"conversation_messages",
		"conversation_session_changes",
		"project_identity_observations",
	} {
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM "+table+" WHERE session_id = ?", id); err != nil {
			return fmt.Errorf("deleting from %s: %w", table, err)
		}
	}
	return nil
}

// rewritePrefixedSessionColumnTx rewrites values that carry the source
// machine's id prefix to the target prefix in one SQL statement. Rows whose
// rewritten value would collide with an existing row in a session-id-keyed
// table are deleted first: the surviving row belongs to the session that
// already lived under the target key.
func rewritePrefixedSessionColumnTx(
	ctx context.Context, tx *sql.Tx, table, column, stringPrefix string, prefixLen int, targetPrefix string,
) error {
	// Collision guard for tables keyed by (session_id[, ...]) whose existing
	// target rows would clash with rewritten source rows. The outer row is
	// aliased so the inner subquery cannot rebind the bare column name to
	// its own table.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %s AS s WHERE substr(s.%s, 1, ?) = ?
		AND EXISTS (SELECT 1 FROM %s AS t
			WHERE t.%s = ? || substr(s.%s, ?))`,
		table, column, table, column, column,
	), prefixLen, stringPrefix, targetPrefix, prefixLen+1); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET %s = ? || substr(%s, ?)
		WHERE substr(%s, 1, ?) = ?`,
		table, column, column, column,
	), targetPrefix, prefixLen+1, prefixLen, stringPrefix); err != nil {
		return err
	}
	return nil
}
