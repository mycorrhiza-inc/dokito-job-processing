package core

import (
	"os"
)

type Runner struct {
	ScraperPaths ScraperBinaryPaths
	DokitoPaths  DokitoBinaryPaths
}

type ScraperBinaryPaths struct {
	NYPUCPath    string
	COPUCPath    string
	UtahCoalPath string
}

func GetScraperPaths() ScraperBinaryPaths {
	return ScraperBinaryPaths{
		NYPUCPath:    os.Getenv("OPENSCRAPER_PATH_NYPUC"),
		COPUCPath:    os.Getenv("OPENSCRAPER_PATH_COPUC"),
		UtahCoalPath: os.Getenv("OPENSCRAPER_PATH_UTAHCOAL"),
	}
}

type DokitoBinaryPaths struct {
	ProcessDocketsPath      string
	UploadDocketsPath       string
	DownloadAttachmentsPath string
	DatabaseUtilsPath       string
}

func GetDokitoPaths() DokitoBinaryPaths {
	return DokitoBinaryPaths{
		ProcessDocketsPath:      os.Getenv("DOKITO_PROCESS_DOCKETS_BINARY_PATH"),
		UploadDocketsPath:       os.Getenv("DOKITO_UPLOAD_DOCKETS_BINARY_PATH"),
		DownloadAttachmentsPath: os.Getenv("DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH"),
		DatabaseUtilsPath:       os.Getenv("DOKITO_DATABASE_UTILS_BINARY_PATH"),
	}
}

type ScraperType string

const (
	NYPUC    ScraperType = "nypuc"
	COPUC    ScraperType = "copuc"
	UtahCoal ScraperType = "utahcoal"
)

type GovIDMapping struct {
	directMappings map[string]ScraperType
	defaultScraper ScraperType
}

func GetDefaultGovIDMapping() *GovIDMapping {
	return &GovIDMapping{
		directMappings: map[string]ScraperType{
			"24A-0125E":        COPUC,
			"24A-0168E":        COPUC,
			"24A-0169E":        COPUC,
			"24AL-0024-EL-UNC": COPUC,
			"24AL-0158-EL-UNC": COPUC,
			"24AL-0159-EL-UNC": COPUC,
			"24AL-0160-EL-UNC": COPUC,
			"24AL-0193-EL-UNC": COPUC,
			"24AL-0194-EL-UNC": COPUC,
			"24AL-0195-EL-UNC": COPUC,
			"24AL-0197-EL-UNC": COPUC,
			"24AL-0205-EL-UNC": COPUC,
			"24AL-0206-EL-UNC": COPUC,
			"24AL-0207-EL-UNC": COPUC,
			"24AL-0208-EL-UNC": COPUC,
			"24AL-0209-EL-UNC": COPUC,
			"24AL-0210-EL-UNC": COPUC,
			"24AL-0211-EL-UNC": COPUC,
			"24AL-0212-EL-UNC": COPUC,
		},
		defaultScraper: NYPUC,
	}
}

func (m *GovIDMapping) GetScraperForGovID(govID string) ScraperType {
	if scraper, exists := m.directMappings[govID]; exists {
		return scraper
	}
	return m.defaultScraper
}

func NewRunner() *Runner {
	return &Runner{
		ScraperPaths: GetScraperPaths(),
		DokitoPaths:  GetDokitoPaths(),
	}
}