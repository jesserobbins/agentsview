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

func TestCollapseBackgroundModelsMigration(t *testing.T) {
	d := testDB(t)
	ctx := t.Context()
	seedSessionWithMessage(t, d, "sess-bg")
	var msgID int64
	require.NoError(t, d.getReader().QueryRowContext(ctx,
		`SELECT id FROM messages WHERE session_id = 'sess-bg' AND ordinal = 0`,
	).Scan(&msgID))

	w := d.getWriter()
	_, err := w.Exec(ctx, `INSERT INTO messages
		(id, session_id, ordinal, role, model, content, content_length)
		VALUES
		(?, 'sess-bg', 1, 'assistant', 'lunaroute-glm-5.2-vision-background', 'a', 1),
		(?, 'sess-bg', 2, 'assistant', 'glm-5.2-vision', 'b', 1),
		(?, 'sess-bg', 3, 'assistant', 'lunaroute-deepseek-v4-flash-ballast', 'c', 1)`,
		msgID+1, msgID+2, msgID+3)
	require.NoError(t, err, "insert mode-variant model messages")
	_, err = w.Exec(ctx, `INSERT INTO usage_events
		(id, session_id, message_ordinal, source, model, provider_id)
		VALUES
		(1, 'sess-bg', 1, 'pi', 'deepseek-4.1-flash-background', 'lunaroute'),
		(2, 'sess-bg', 2, 'pi', 'deepseek-4.1-flash', 'lunaroute'),
		(3, 'sess-bg', 3, 'pi', 'glm-5.2-vision-ballast', 'lunaroute')`)
	require.NoError(t, err, "insert mode-variant usage events")

	// Open already ran the one-time collapse on the empty tables during
	// testDB setup; clear the sentinel so it runs against these rows.
	_, err = w.Exec(ctx, `DELETE FROM stats WHERE key = ?`,
		backgroundModelCollapseStatsKey)
	require.NoError(t, err, "clear collapse sentinel")
	require.NoError(t, d.collapseBackgroundModelsLocked(ctx, w), "collapse")

	var got []string
	rows, err := d.getReader().QueryContext(ctx,
		`SELECT model FROM messages WHERE session_id = 'sess-bg' ORDER BY ordinal`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var model string
		require.NoError(t, rows.Scan(&model))
		got = append(got, model)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{
		"", "lunaroute-glm-5.2-vision", "glm-5.2-vision", "lunaroute-deepseek-v4-flash",
	}, got)

	models := map[string]bool{}
	urows, err := d.getReader().QueryContext(ctx,
		`SELECT model FROM usage_events`)
	require.NoError(t, err)
	defer urows.Close()
	for urows.Next() {
		var model string
		require.NoError(t, urows.Scan(&model))
		models[model] = true
	}
	require.NoError(t, urows.Err())
	assert.True(t, models["deepseek-4.1-flash"], "usage event collapsed")
	assert.True(t, models["glm-5.2-vision"], "ballast usage event collapsed")
	assert.Equal(t, 0, countRows(t, d,
		`SELECT count(*) FROM usage_events WHERE model LIKE '%-background' OR model LIKE '%-ballast'`),
		"no serving-mode variants remain")

	// Second run is a no-op: the sentinel is set. Raw -background rows can
	// only enter through pre-migration archives; every write path runs
	// through ValidateAndSanitize, which collapses them at ingest.
	require.NoError(t, d.collapseBackgroundModelsLocked(ctx, w))
	assert.Equal(t, 0, countRows(t, d,
		`SELECT count(*) FROM usage_events WHERE model LIKE '%-background' OR model LIKE '%-ballast'`))
	assert.Equal(t, 1, countRows(t, d,
		`SELECT count(*) FROM stats WHERE key = ?`, backgroundModelCollapseStatsKey))
}
