package internal

import "path"

type JurisdictionInfo struct {
	Country       string
	State         string
	Jurisdictrion string
}

type RawDocketLocation struct {
	JurisdictionInfo JurisdictionInfo
	DocketGovID      string
}

func TryAndExtractGovid(obj map[string]any) (string, error) {
	id := obj["docket_gov_id"]
}

func (rawDocket RawDocketLocation) GenerateCannonicalPath() string {
	basePath := GetProcessingIntermediateDir()
	return path.Join(basePath,
		"objects_raw",
		rawDocket.JurisdictionInfo.Country,
		rawDocket.JurisdictionInfo.State,
		rawDocket.JurisdictionInfo.Jurisdictrion,
		rawDocket.DocketGovID)
}

func (rawDocket RawDocketLocation) GenerateCannonicalS3Location() S3Location {
	key := path.Join(
		"objects_raw",
		rawDocket.JurisdictionInfo.Country,
		rawDocket.JurisdictionInfo.State,
		rawDocket.JurisdictionInfo.Jurisdictrion,
		rawDocket.DocketGovID)
	baseS3Location, _ := NewOpenscrapersBucketLocation(key)

	return baseS3Location
}

type ProcessedDocketLocation struct {
	JurisdictionInfo JurisdictionInfo
	DocketGovID      string
}

func (processedDocket ProcessedDocketLocation) GenerateCannonicalPath() string {
	basePath := GetProcessingIntermediateDir()
	return path.Join(basePath,
		"objects",
		processedDocket.JurisdictionInfo.Country,
		processedDocket.JurisdictionInfo.State,
		processedDocket.JurisdictionInfo.Jurisdictrion,
		processedDocket.DocketGovID)
}

func (processedDocket ProcessedDocketLocation) GenerateCannonicalS3Location() S3Location {
	key := path.Join(
		"objects",
		processedDocket.JurisdictionInfo.Country,
		processedDocket.JurisdictionInfo.State,
		processedDocket.JurisdictionInfo.Jurisdictrion,
		processedDocket.DocketGovID)
	baseS3Location, _ := NewOpenscrapersBucketLocation(key)

	return baseS3Location
}
