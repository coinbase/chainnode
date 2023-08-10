package main

var (
	rootFlags struct {
		env        string
		blockchain string
		network    string
	}

	rootCommand = NewCommand("admin", nil)
)

func init() {
	rootCommand.Command.SilenceUsage = true
	rootCommand.StringVar(&rootFlags.env, "env", "", true)
	rootCommand.StringVar(&rootFlags.blockchain, "blockchain", "", true)
	rootCommand.StringVar(&rootFlags.network, "network", "", true)
}
