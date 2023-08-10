package main

import (
	"github.com/spf13/cobra"
)

type (
	Command struct {
		*cobra.Command
	}

	runFn func() error
)

func NewCommand(use string, fn runFn) *Command {
	command := &cobra.Command{
		Use: use,
	}
	if fn != nil {
		command.RunE = func(cmd *cobra.Command, args []string) error {
			return fn()
		}
	}

	return &Command{
		Command: command,
	}
}

func (c *Command) AddCommand(subCommand *Command) {
	c.Command.AddCommand(subCommand.Command)
}

func (c *Command) StringVar(p *string, name string, value string, required bool) {
	c.PersistentFlags().StringVar(p, name, value, "")
	if required {
		if err := c.MarkPersistentFlagRequired(name); err != nil {
			panic(err)
		}
	}
}

func (c *Command) BoolVar(p *bool, name string, value bool, required bool) {
	c.PersistentFlags().BoolVar(p, name, value, "")
	if required {
		if err := c.MarkPersistentFlagRequired(name); err != nil {
			panic(err)
		}
	}
}

func (c *Command) Uint32Var(p *uint32, name string, value uint32, required bool) {
	c.PersistentFlags().Uint32Var(p, name, value, "")
	if required {
		if err := c.MarkPersistentFlagRequired(name); err != nil {
			panic(err)
		}
	}
}

func (c *Command) Uint64Var(p *uint64, name string, value uint64, required bool) {
	c.PersistentFlags().Uint64Var(p, name, value, "")
	if required {
		if err := c.MarkPersistentFlagRequired(name); err != nil {
			panic(err)
		}
	}
}

func (c *Command) Int64Var(p *int64, name string, value int64, required bool) {
	c.PersistentFlags().Int64Var(p, name, value, "")
	if required {
		if err := c.MarkPersistentFlagRequired(name); err != nil {
			panic(err)
		}
	}
}
