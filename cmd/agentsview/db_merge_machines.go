package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
)

func newDBMergeMachinesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "merge-machines <target-machine> <old-machine> [old-machine ...]",
		Short:        "Fold historical remote-host machine keys into one machine key",
		Long: "Fold the sessions of one or more historical remote-host machine keys " +
			"into a single target machine key, repairing the duplication a remote " +
			"host rename causes. Session ids are rewritten to the target prefix; " +
			"sessions that already exist under the target are deleted as duplicates " +
			"and excluded from future syncs. Old keys remain filter redirects. " +
			"Stop the daemon first. Use `db adopt-machine --list` to inspect keys; " +
			"use `db adopt-machine` to claim history for this installation instead.",
		SilenceUsage: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 2 {
				return errors.New("provide a target machine key and one or more old machine keys")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, machines []string) error {
			target, sources := machines[0], machines[1:]
			cfg, err := config.LoadMinimal()
			if err != nil {
				return err
			}
			database, lock, err := openWriteDBWith(cmd.Context(), cfg, func(ctx context.Context, cfg config.Config) (*db.DB, error) {
				applyClassifierConfig(cfg)
				database, err := db.Open(ctx, cfg.DBPath)
				if err != nil {
					return nil, err
				}
				if err := database.MergeMachineIdentities(cmd.Context(), cfg.InstallationID, target, sources); err != nil {
					database.Close()
					return nil, err
				}
				return database, nil
			})
			if err != nil {
				return err
			}
			defer closeWriteDB(database, lock)
			_, err = fmt.Fprintf(cmd.OutOrStdout(),
				"Merged %d machine key(s) into %q.\n", len(sources), target)
			return err
		},
	}
	return cmd
}
