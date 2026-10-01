package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
)

func TestDBMergeMachinesCommandFoldsRemoteHistory(t *testing.T) {
	isolateDirectCLISources(t)
	dir := testDataDir(t)
	database, err := db.Open(t.Context(), filepath.Join(dir, "sessions.db"))
	require.NoError(t, err)
	const keep = "legacy-root~pi:kept"
	const shared = "legacy-root~pi:shared"
	for id, machine := range map[string]string{
		keep: "legacy-root", shared: "legacy-root",
		"legacy~pi:shared": "legacy", "legacy~pi:native": "legacy",
	} {
		require.NoError(t, database.UpsertSession(t.Context(), db.Session{
			ID: id, Machine: machine, Project: "project", Agent: "pi", UserMessageCount: 1,
		}))
	}
	require.NoError(t, database.Close())
	cfg, err := config.LoadMinimal()
	require.NoError(t, err)

	_, err = executeCommand(newRootCommand(), "db", "merge-machines", "legacy")
	require.ErrorContains(t, err, "provide a target machine key")
	_, err = executeCommand(newRootCommand(), "db", "merge-machines", "legacy", "ghost")
	require.ErrorContains(t, err, "not recorded")
	_, err = executeCommand(newRootCommand(),
		"db", "merge-machines", cfg.InstallationID, "legacy")
	require.ErrorContains(t, err, "adopt-machine")

	output, err := executeCommand(newRootCommand(),
		"db", "merge-machines", "legacy", "legacy-root")
	require.NoError(t, err)
	assert.Contains(t, output, `Merged 1 machine key(s) into "legacy".`)

	database, err = db.Open(t.Context(), filepath.Join(dir, "sessions.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	kept, err := database.GetSession(t.Context(), "legacy~pi:kept")
	require.NoError(t, err)
	require.NotNil(t, kept)
	assert.Equal(t, "legacy", kept.Machine)
	sharedRow, err := database.GetSession(t.Context(), "legacy~pi:shared")
	require.NoError(t, err)
	require.NotNil(t, sharedRow, "target copy of the duplicate survives")
	gone, err := database.GetSession(t.Context(), shared)
	require.NoError(t, err)
	assert.Nil(t, gone, "duplicate source row is removed")
	aliases, err := database.GetMachineAliases(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "legacy", aliases["legacy-root"])
}
