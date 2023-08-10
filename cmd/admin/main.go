package main

import (
	"fmt"
	"os"
)

func main() {
	if err := rootCommand.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to execute command: %+v\n", err)
		os.Exit(1)
	}
}
