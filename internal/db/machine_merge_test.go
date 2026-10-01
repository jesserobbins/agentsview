package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mergeMachineOpt(machine string) func(*Session) {
	return func(s *Session) { s.Machine = machine }
}

func countRows(t *testing.T, d *DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), query, args...).Scan(&n))
	return n
}

func TestMergeMachineIdentitiesRewritesSessionsAndSideRows(t *testing.T) {
	d := testDB(t)
	ctx := t.Context()

	insertSession(t, d, "legacy~pi:target-only", "proj", mergeMachineOpt("legacy"))
	insertSession(t, d, "legacy-root~pi:keep", "proj", mergeMachineOpt("legacy-root"))
	insertSession(t, d, "legacy-root~pi:child", "proj", mergeMachineOpt("legacy-root"),
		func(s *Session) { parent := "legacy-root~pi:keep"; s.ParentSessionID = &parent })
	insertMessages(t, d,
		userMsg("legacy-root~pi:keep", 0, "hello"),
		userMsg("legacy-root~pi:child", 0, "hi"),
	)
	w := d.getWriter()
	_, err := w.Exec(ctx, `INSERT INTO starred_sessions (session_id) VALUES (?)`,
		"legacy-root~pi:keep")
	require.NoError(t, err)
	_, err = w.Exec(ctx, `INSERT INTO remote_skipped_files (host, path, file_mtime)
		VALUES ('legacy-root', '/remote/pi/keep.jsonl', 123)`)
	require.NoError(t, err)
	_, err = w.Exec(ctx, `INSERT INTO worktree_project_mappings
		(machine, path_prefix, project) VALUES ('legacy-root', '/repo', 'proj')`)
	require.NoError(t, err)

	require.NoError(t, d.MergeMachineIdentities(ctx, "installation-key",
		"legacy", []string{"legacy-root"}))

	// Kept sessions rename to the target prefix and machine.
	sess, err := d.GetSession(ctx, "legacy~pi:keep")
	require.NoError(t, err)
	require.NotNil(t, sess)
	assert.Equal(t, "legacy", sess.Machine)
	gone, err := d.GetSession(ctx, "legacy-root~pi:keep")
	require.NoError(t, err)
	assert.Nil(t, gone, "old id must not survive")

	// Messages, stars, and parent refs follow the rewrite.
	assert.Equal(t, 1, countRows(t, d,
		`SELECT count(*) FROM messages WHERE session_id = 'legacy~pi:keep'`))
	assert.Equal(t, 1, countRows(t, d,
		`SELECT count(*) FROM starred_sessions WHERE session_id = 'legacy~pi:keep'`))
	child, err := d.GetSession(ctx, "legacy~pi:child")
	require.NoError(t, err)
	require.NotNil(t, child)
	require.NotNil(t, child.ParentSessionID)
	assert.Equal(t, "legacy~pi:keep", *child.ParentSessionID)

	// The historical host key remains only as a filter redirect.
	assert.Equal(t, 0, countRows(t, d,
		`SELECT count(*) FROM sessions WHERE machine = 'legacy-root'`))
	assert.Equal(t, 0, countRows(t, d,
		`SELECT count(*) FROM remote_skipped_files WHERE host = 'legacy-root'`))
	var alias string
	require.NoError(t, d.getReader().QueryRowContext(ctx,
		`SELECT value FROM pg_sync_state WHERE key = 'machine_alias:legacy-root'`).Scan(&alias))
	assert.Equal(t, "legacy", alias)

	// Worktree rules move with the machine.
	assert.Equal(t, 1, countRows(t, d,
		`SELECT count(*) FROM worktree_project_mappings WHERE machine = 'legacy'`))
}

func TestMergeMachineIdentitiesDeduplicatesSharedSessions(t *testing.T) {
	d := testDB(t)
	ctx := t.Context()

	// The same session synced under both host names.
	insertSession(t, d, "legacy~pi:shared", "proj", mergeMachineOpt("legacy"))
	insertSession(t, d, "legacy-eth0~pi:shared", "proj", mergeMachineOpt("legacy-eth0"))
	insertMessages(t, d,
		userMsg("legacy~pi:shared", 0, "target copy"),
		userMsg("legacy-eth0~pi:shared", 0, "stale copy"),
	)
	// A session that exists only under the source keeps flowing through.
	insertSession(t, d, "legacy-eth0~pi:source-only", "proj", mergeMachineOpt("legacy-eth0"))

	require.NoError(t, d.MergeMachineIdentities(ctx, "installation-key",
		"legacy", []string{"legacy-eth0"}))

	// Target copy wins; source duplicate and its messages are gone.
	sess, err := d.GetSession(ctx, "legacy~pi:shared")
	require.NoError(t, err)
	require.NotNil(t, sess)
	assert.Equal(t, 1, countRows(t, d,
		`SELECT count(*) FROM messages WHERE session_id = 'legacy~pi:shared'`))
	gone, err := d.GetSession(ctx, "legacy-eth0~pi:shared")
	require.NoError(t, err)
	assert.Nil(t, gone, "duplicate must not survive")

	// The duplicate id is excluded so a re-added historical host cannot
	// resurrect it.
	assert.Equal(t, 1, countRows(t, d,
		`SELECT count(*) FROM excluded_sessions WHERE id = 'legacy-eth0~pi:shared'`))

	// Source-only sessions rename into the target.
	renamed, err := d.GetSession(ctx, "legacy~pi:source-only")
	require.NoError(t, err)
	require.NotNil(t, renamed)
	assert.Equal(t, "legacy", renamed.Machine)
}

func TestMergeMachineIdentitiesValidation(t *testing.T) {
	d := testDB(t)
	ctx := t.Context()

	insertSession(t, d, "legacy~pi:s", "proj", mergeMachineOpt("legacy"))

	err := d.MergeMachineIdentities(ctx, "installation-key", "legacy", nil)
	assert.ErrorContains(t, err, "select one or more")

	err = d.MergeMachineIdentities(ctx, "installation-key", "legacy", []string{"ghost"})
	assert.ErrorContains(t, err, "not recorded")

	err = d.MergeMachineIdentities(ctx, "installation-key", "legacy", []string{"legacy"})
	assert.ErrorContains(t, err, "merge target")

	err = d.MergeMachineIdentities(ctx, "installation-key",
		"installation-key", []string{"legacy"})
	assert.ErrorContains(t, err, "adopt-machine")

	err = d.MergeMachineIdentities(ctx, "installation-key", "legacy",
		[]string{"legacy-root", "legacy-root"})
	assert.ErrorContains(t, err, "listed twice")

	// An already-aliased source refuses a second merge under a new target.
	_, werr := d.getWriter().Exec(ctx, `INSERT INTO pg_sync_state (key, value)
		VALUES ('machine_alias:legacy', 'other')`)
	require.NoError(t, werr)
	insertSession(t, d, "other~pi:x", "proj", mergeMachineOpt("other"))
	err = d.MergeMachineIdentities(ctx, "installation-key", "zephyr", []string{"legacy"})
	assert.ErrorContains(t, err, "filter redirect")
}

func TestMergeMachineIdentitiesRefusesConflictingWorktreeRules(t *testing.T) {
	d := testDB(t)
	ctx := t.Context()

	insertSession(t, d, "legacy~pi:s", "proj", mergeMachineOpt("legacy"))
	insertSession(t, d, "legacy-root~pi:s2", "proj", mergeMachineOpt("legacy-root"))
	w := d.getWriter()
	_, err := w.Exec(ctx, `INSERT INTO worktree_project_mappings
		(machine, path_prefix, project, layout, enabled) VALUES
		('legacy', '/repo', 'proj', 'flat', 1),
		('legacy-root', '/repo', 'other', 'flat', 1)`)
	require.NoError(t, err)

	err = d.MergeMachineIdentities(ctx, "installation-key", "legacy", []string{"legacy-root"})
	assert.ErrorContains(t, err, "conflicting worktree rules")
}
