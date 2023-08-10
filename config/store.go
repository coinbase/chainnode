package config

import (
	"embed"
)

//go:embed chainnode/*
var Store embed.FS
