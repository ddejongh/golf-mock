// Package migrations embeds the goose SQL migrations so binaries can apply
// them without the source tree.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
