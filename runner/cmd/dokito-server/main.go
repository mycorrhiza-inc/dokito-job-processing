// Package main provides the Dokito Job Processing API
//
// @title Dokito Job Processing API
// @version 1.0
// @description API for managing data scraping, processing, and upload pipelines for government documents
// @termsOfService http://swagger.io/terms/
//
// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io
//
// @license.name MIT
// @license.url https://opensource.org/licenses/MIT
//
// @BasePath /
// @schemes http https
package main

import (
	"os"

	"runner/internal/cli"
)

func main() {
	// Force server command to run
	os.Args = append([]string{os.Args[0], "server"}, os.Args[1:]...)
	cli.Execute()
}