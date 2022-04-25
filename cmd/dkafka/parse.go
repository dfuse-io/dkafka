package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/bstream/forkable"
)

var ParseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Debug parssing commands",
	Long:  "",
}

var ParseCursorCmd = &cobra.Command{
	Use:   "cursor <opaque-cursor>",
	Short: "Parse an opaque cursor",
	Long:  "Parse an opaque cursor and return its value",
	RunE:  parseCursor,
}

func init() {
	RootCmd.AddCommand(ParseCmd)

	ParseCmd.AddCommand(ParseCursorCmd)
	ParseCursorCmd.Flags().String("cursor", "", "specify the cursor with this flag when it start with a '-'")
}

func parseCursor(cmd *cobra.Command, args []string) error {
	SetupLogger()

	var opaqueCursor string
	if len(args) == 1 {
		opaqueCursor = args[0]
	} else {
		opaqueCursor = viper.GetString("parse-cursor-cmd-cursor")
	}

	if opaqueCursor == "" {
		return fmt.Errorf("un-expected parameter only opaque cursor must be provided")
	}

	cursor, err := forkable.CursorFromOpaque(opaqueCursor)
	if err != nil {
		return err
	}
	fmt.Printf("cursor: { step: %s, block: %s, LIB: %s, head: %s}\n", cursor.Step, cursor.Block, cursor.LIB, cursor.HeadBlock)
	return nil
}
