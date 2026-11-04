package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// UnifiedIncident matches the structure of our database table.
type UnifiedIncident struct {
	ID               int
	Source           string
	SourceID         string
	EventType        string
	Address          string
	Latitude         sql.NullFloat64
	Longitude        sql.NullFloat64
	Timestamp        time.Time
	Details          []byte // Raw JSONB from the database
	DiscordMessageID sql.NullString
}

// Camera holds the info for a nearby traffic camera.
type Camera struct {
	Name     string
	ImageURL string
}

// Structs for creating a rich Discord Embed message with attachments.
type DiscordWebhookPayload struct {
	Username    string              `json:"username"`
	AvatarURL   string              `json:"avatar_url,omitempty"`
	Embeds      []DiscordEmbed      `json:"embeds"`
	Attachments []DiscordAttachment `json:"attachments,omitempty"`
}

type DiscordAttachment struct {
	ID       int    `json:"id"`
	Filename string `json:"filename"`
}

type DiscordEmbed struct {
	Title     string          `json:"title,omitempty"`
	Color     int             `json:"color"`
	Fields    []EmbedField    `json:"fields,omitempty"`
	Footer    EmbedFooter     `json:"footer,omitempty"`
	Timestamp string          `json:"timestamp,omitempty"`
	Thumbnail *EmbedThumbnail `json:"thumbnail,omitempty"`
	Image     *EmbedImage     `json:"image,omitempty"`
}

type EmbedThumbnail struct {
	URL string `json:"url"`
}

type EmbedImage struct {
	URL string `json:"url"`
}

type EmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type EmbedFooter struct {
	Text string `json:"text"`
}

// getWebhookForSource routes the incident to the correct Discord channel
func getWebhookForSource(source string) string {
	switch source {
	case "NCDOT":
		return os.Getenv("DISCORD_HOOK_NCDOT")
	case "RWECC":
		return os.Getenv("DISCORD_HOOK_RWECC")
	default:
		return "" // Return empty if no route exists
	}
}

// captureCameraImage downloads a camera image and saves it to a temporary file.
// captureCameraImage downloads a camera image and saves it to a temporary file.
func captureCameraImage(db *sql.DB, incidentID int, camera Camera) (string, string, error) {
	log.Printf("Capturing image from camera: %s", camera.Name)

	req, err := http.NewRequest("GET", camera.ImageURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	// Spoof a standard web browser to bypass the camera server's anti-scraping block
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to download image: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", "", fmt.Errorf("received non-200 status code for image: %s", resp.Status)
	}

	fileName := fmt.Sprintf("incident_%d_cam_%s.jpg", incidentID, time.Now().Format("20060102150405"))
	filePath := fileName // Keep local for testing, or revert to filepath.Join(os.TempDir(), fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		os.Remove(filePath)
		return "", "", fmt.Errorf("failed to save image to file: %w", err)
	}

	_, err = db.Exec("INSERT INTO camera_captures (incident_id, camera_name, file_path) VALUES ($1, $2, $3)",
		incidentID, camera.Name, filePath)
	if err != nil {
		log.Printf("Warning: failed to log camera capture to DB: %v", err)
	}

	log.Printf("Successfully saved camera frame to %s", filePath)
	return filePath, fileName, nil
}

// findNearbyCameras queries the database to find the closest cameras to a given point.
func findNearbyCameras(db *sql.DB, lat, lon float64, limit int) ([]Camera, error) {
	var cameras []Camera
	query := `
		SELECT name, image_url
		FROM traffic_cameras
		ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
		LIMIT $3;
	`
	rows, err := db.Query(query, lon, lat, limit)
	if err != nil {
		return nil, fmt.Errorf("error querying for nearby cameras: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cam Camera
		if err := rows.Scan(&cam.Name, &cam.ImageURL); err != nil {
			return nil, fmt.Errorf("error scanning camera row: %w", err)
		}
		cameras = append(cameras, cam)
	}
	return cameras, nil
}

// sendDiscordAlert is the main router for sending a new, enriched alert.
func sendDiscordAlert(db *sql.DB, webhookURL, mapsAPIKey string, incident UnifiedIncident) (string, error) {
	var payload DiscordWebhookPayload
	var attachmentPath, attachmentName string

	var nearbyCameras []Camera
	if incident.Latitude.Valid && incident.Longitude.Valid {
		var err error
		nearbyCameras, err = findNearbyCameras(db, incident.Latitude.Float64, incident.Longitude.Float64, 3)
		if err != nil {
			log.Printf("Could not fetch nearby cameras: %v", err)
		}
	}

	if len(nearbyCameras) > 0 {
		var err error
		attachmentPath, attachmentName, err = captureCameraImage(db, incident.ID, nearbyCameras[0])
		if err != nil {
			log.Printf("Failed to capture camera image: %v", err)
			attachmentPath = ""
		}
	}
	if attachmentPath != "" {
		//defer os.Remove(attachmentPath)
	}

	if incident.Source == "NCDOT" {
		payload = buildNcdotPayload(mapsAPIKey, incident, nearbyCameras, attachmentName)
	} else if incident.Source == "RWECC" {
		payload = buildRweccPayload(mapsAPIKey, incident, nearbyCameras, attachmentName)
	} else {
		return "", fmt.Errorf("unknown incident source: %s", incident.Source)
	}

	if attachmentName != "" {
		payload.Attachments = []DiscordAttachment{
			{
				ID:       0,
				Filename: attachmentName,
			},
		}
	}

	return postMultipartToWebhook(webhookURL, payload, attachmentPath)
}

// buildNcdotPayload creates the multi-embed structure for an NC DOT alert.
func buildNcdotPayload(mapsAPIKey string, incident UnifiedIncident, nearbyCameras []Camera, attachmentName string) DiscordWebhookPayload {
	var rawIncident struct {
		Reason   string `json:"reason"`
		Road     string `json:"road"`
		Location string `json:"location"`
		Severity int    `json:"severity"`
	}
	var weatherDetails *struct {
		Temperature   int    `json:"temperature"`
		WindSpeed     string `json:"windSpeed"`
		ShortForecast string `json:"shortForecast"`
		Icon          string `json:"icon"`
	}

	var detailsMap map[string]json.RawMessage
	if err := json.Unmarshal(incident.Details, &detailsMap); err == nil {
		if rawJSON, ok := detailsMap["raw_incident"]; ok {
			json.Unmarshal(rawJSON, &rawIncident)
		}
		if weatherJSON, ok := detailsMap["weather"]; ok && string(weatherJSON) != "null" {
			json.Unmarshal(weatherJSON, &weatherDetails)
		}
	} else {
		log.Printf("INFO: Could not parse as new format, falling back to old format for NCDOT incident.")
		json.Unmarshal(incident.Details, &rawIncident)
	}

	var color int
	switch rawIncident.Severity {
	case 1:
		color = 3066993
	case 2:
		color = 16776960
	case 3:
		color = 15158332
	default:
		color = 2105893
	}

	fields := []EmbedField{
		{Name: "Reason", Value: rawIncident.Reason, Inline: false},
		{Name: "Road", Value: rawIncident.Road, Inline: false},
		{Name: "Location", Value: rawIncident.Location, Inline: false},
		{Name: "Severity", Value: strconv.Itoa(rawIncident.Severity), Inline: false},
	}

	if weatherDetails != nil {
		weatherValue := fmt.Sprintf("%s\nTemp: %d°F\nWind: %s", weatherDetails.ShortForecast, weatherDetails.Temperature, weatherDetails.WindSpeed)
		fields = append(fields, EmbedField{Name: "Weather Conditions", Value: weatherValue, Inline: false})
	}

	if len(nearbyCameras) > 1 {
		var cameraLinks []string
		for i := 1; i < len(nearbyCameras); i++ {
			cameraLinks = append(cameraLinks, fmt.Sprintf("[%s](%s)", nearbyCameras[i].Name, nearbyCameras[i].ImageURL))
		}
		fields = append(fields, EmbedField{Name: "Other Live Cameras", Value: strings.Join(cameraLinks, "\n"), Inline: false})
	}

	embed := DiscordEmbed{
		Title: "🚨 NC DOT - Incident Alert 🚨", Color: color, Fields: fields,
		Footer: EmbedFooter{Text: "Source: NC DOT API"}, Timestamp: incident.Timestamp.Format(time.RFC3339),
	}

	log.Printf("DEBUG MAP BUILDER - ID: %d | API Key exists: %t | Lat Valid: %t (%.6f) | Lon Valid: %t (%.6f)", incident.ID, mapsAPIKey != "", incident.Latitude.Valid, incident.Latitude.Float64, incident.Longitude.Valid, incident.Longitude.Float64)
	if mapsAPIKey != "" && incident.Latitude.Valid && incident.Longitude.Valid {
		mapURL := fmt.Sprintf("https://maps.googleapis.com/maps/api/staticmap?center=%.6f,%.6f&zoom=14&size=300x300&markers=color:red%%7C%.6f,%.6f&key=%s",
			incident.Latitude.Float64, incident.Longitude.Float64, incident.Latitude.Float64, incident.Longitude.Float64, mapsAPIKey)
		embed.Thumbnail = &EmbedThumbnail{URL: mapURL}
	}

	if attachmentName != "" {
		embed.Image = &EmbedImage{URL: "attachment://" + attachmentName}
	}

	return DiscordWebhookPayload{Username: "Unified Alert Bot", Embeds: []DiscordEmbed{embed}}
}

// buildRweccPayload creates the multi-embed structure for an RWECC alert.
func buildRweccPayload(mapsAPIKey string, incident UnifiedIncident, nearbyCameras []Camera, attachmentName string) DiscordWebhookPayload {
	var rawIncident struct {
		Problem      string `json:"problem"`
		Jurisdiction string `json:"jurisdiction"`
	}
	var weatherDetails *struct {
		Temperature   int    `json:"temperature"`
		WindSpeed     string `json:"windSpeed"`
		ShortForecast string `json:"shortForecast"`
		Icon          string `json:"icon"`
	}

	var detailsMap map[string]json.RawMessage
	if err := json.Unmarshal(incident.Details, &detailsMap); err == nil {
		if rawJSON, ok := detailsMap["raw_incident"]; ok {
			json.Unmarshal(rawJSON, &rawIncident)
		}
		if weatherJSON, ok := detailsMap["weather"]; ok && string(weatherJSON) != "null" {
			json.Unmarshal(weatherJSON, &weatherDetails)
		}
	} else {
		log.Printf("INFO: Could not parse as new format, falling back to old format for RWECC incident.")
		json.Unmarshal(incident.Details, &rawIncident)
	}

	fields := []EmbedField{
		{Name: "Address", Value: incident.Address, Inline: false},
		{Name: "Jurisdiction", Value: rawIncident.Jurisdiction, Inline: false},
	}

	if weatherDetails != nil {
		weatherValue := fmt.Sprintf("%s\nTemp: %d°F\nWind: %s", weatherDetails.ShortForecast, weatherDetails.Temperature, weatherDetails.WindSpeed)
		fields = append(fields, EmbedField{Name: "Weather Conditions", Value: weatherValue, Inline: false})
	}

	if len(nearbyCameras) > 1 {
		var cameraLinks []string
		for i := 1; i < len(nearbyCameras); i++ {
			cameraLinks = append(cameraLinks, fmt.Sprintf("[%s](%s)", nearbyCameras[i].Name, nearbyCameras[i].ImageURL))
		}
		fields = append(fields, EmbedField{Name: "Other Live Cameras", Value: strings.Join(cameraLinks, "\n"), Inline: false})
	}

	embed := DiscordEmbed{
		Title: "🔵 " + rawIncident.Problem + " 🔵", Color: 3447003, Fields: fields,
		Footer: EmbedFooter{Text: "Source: Raleigh-Wake ECC"}, Timestamp: incident.Timestamp.Format(time.RFC3339),
	}
	log.Printf("DEBUG MAP BUILDER - ID: %d | API Key exists: %t | Lat Valid: %t (%.6f) | Lon Valid: %t (%.6f)", incident.ID, mapsAPIKey != "", incident.Latitude.Valid, incident.Latitude.Float64, incident.Longitude.Valid, incident.Longitude.Float64)

	if mapsAPIKey != "" && incident.Latitude.Valid && incident.Longitude.Valid {
		mapURL := fmt.Sprintf("https://maps.googleapis.com/maps/api/staticmap?center=%.6f,%.6f&zoom=14&size=300x300&markers=color:red%%7C%.6f,%.6f&key=%s",
			incident.Latitude.Float64, incident.Longitude.Float64, incident.Latitude.Float64, incident.Longitude.Float64, mapsAPIKey)
		embed.Thumbnail = &EmbedThumbnail{URL: mapURL}
	}

	if attachmentName != "" {
		embed.Image = &EmbedImage{URL: "attachment://" + attachmentName}
	}

	return DiscordWebhookPayload{Username: "Unified Alert Bot", Embeds: []DiscordEmbed{embed}}
}

// postMultipartToWebhook sends a message that may include a file attachment.
func postMultipartToWebhook(webhookURL string, payload DiscordWebhookPayload, attachmentPath string) (string, error) {
	webhookURL += "?wait=true"

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	jsonPart, err := writer.CreateFormField("payload_json")
	if err != nil {
		return "", err
	}
	if err := json.NewEncoder(jsonPart).Encode(payload); err != nil {
		return "", err
	}

	if attachmentPath != "" {
		file, err := os.Open(attachmentPath)
		if err != nil {
			return "", err
		}
		defer file.Close()
		part, err := writer.CreateFormFile("files[0]", filepath.Base(attachmentPath))
		if err != nil {
			return "", err
		}
		if _, err = io.Copy(part, file); err != nil {
			return "", err
		}
	}

	writer.Close()

	req, err := http.NewRequest("POST", webhookURL, body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("discord returned non-2xx status: %s. Body: %s", resp.Status, string(respBody))
	}

	var message struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return "", err
	}
	return message.ID, nil
}

// updateDiscordAlert edits an existing Discord message to show it's cleared.
func updateDiscordAlert(webhookURL, messageID string, incident UnifiedIncident) error {
	embed := DiscordEmbed{
		Title: "✅ Incident Cleared ✅",
		Color: 3066993, // Green
		Fields: []EmbedField{
			{Name: "Source", Value: incident.Source, Inline: false},
			{Name: "Address", Value: incident.Address, Inline: false},
		},
		Footer:    EmbedFooter{Text: "Incident no longer in active feed"},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	payload := DiscordWebhookPayload{Embeds: []DiscordEmbed{embed}}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error creating update JSON payload: %w", err)
	}
	updateURL := fmt.Sprintf("%s/messages/%s", webhookURL, messageID)
	req, err := http.NewRequest("PATCH", updateURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("error creating PATCH request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending PATCH request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("discord returned non-2xx status on update: %s. Body: %s", resp.Status, string(body))
	}
	return nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		if err := godotenv.Load(".env.dev"); err != nil {
			log.Println("Note: No .env or .env.dev file found, reading from system environment")
		} else {
			log.Println("Loaded configuration from .env.dev")
		}
	} else {
		log.Println("Loaded configuration from .env")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_USERNAME"),
		os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"))
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error opening database: %s", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("Error connecting to database: %s", err)
	}
	log.Println("Successfully connected to the database.")

	mapsAPIKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	notifyDiscord := os.Getenv("NOTIFY_DISCORD")

	if os.Getenv("DISCORD_HOOK_NCDOT") == "" && os.Getenv("DISCORD_HOOK_RWECC") == "" {
		log.Println("Warning: No Discord webhooks (NCDOT or RWECC) are configured in the environment.")
	}

	// Step 1: Process New Incidents
	rows, err := db.Query("SELECT id, source, source_id, event_type, address, latitude, longitude, timestamp, details FROM unified_incidents WHERE status = 'active' AND discord_message_id IS NULL AND source != 'ArcGIS_Police'")
	if err != nil {
		log.Fatalf("Error querying for new incidents: %v", err)
	}
	defer rows.Close()

	var newIncidentsFound int
	for rows.Next() {
		var i UnifiedIncident
		if err := rows.Scan(&i.ID, &i.Source, &i.SourceID, &i.EventType, &i.Address, &i.Latitude, &i.Longitude, &i.Timestamp, &i.Details); err != nil {
			log.Printf("Error scanning incident: %v", err)
			continue
		}

		log.Printf("Found new unified incident from %s (ID: %s).", i.Source, i.SourceID)

		targetWebhook := getWebhookForSource(i.Source)
		if targetWebhook == "" {
			log.Printf("No webhook configured for source %s, skipping", i.Source)
			continue
		}

		if notifyDiscord == "0" {
			log.Println("--- DEBUG MODE: NOTIFY_DISCORD=0 ---")
			var prettyJSON bytes.Buffer
			if err := json.Indent(&prettyJSON, i.Details, "", "  "); err != nil {
				log.Printf("Error formatting JSON for debug: %v", err)
			} else {
				log.Println(prettyJSON.String())
			}
			continue
		}

		log.Println("Sending alert to Discord...")
		messageID, err := sendDiscordAlert(db, targetWebhook, mapsAPIKey, i)
		if err != nil {
			log.Printf("Error sending Discord alert: %v", err)
			continue
		}

		_, err = db.Exec("UPDATE unified_incidents SET discord_message_id = $1 WHERE id = $2", messageID, i.ID)
		if err != nil {
			log.Printf("Error saving discord_message_id: %v", err)
		}

		newIncidentsFound++
		time.Sleep(2 * time.Second)
	}
	log.Printf("Processed %d new alerts.", newIncidentsFound)

	// Step 2: Process Cleared Incidents
	clearedRows, err := db.Query("SELECT id, source, address, discord_message_id FROM unified_incidents WHERE status = 'cleared' AND discord_message_id IS NOT NULL AND source != 'ArcGIS_Police'")
	if err != nil {
		log.Fatalf("Error querying for cleared incidents: %v", err)
	}
	defer clearedRows.Close()

	var clearedIncidentsUpdated int
	for clearedRows.Next() {
		var i UnifiedIncident
		if err := clearedRows.Scan(&i.ID, &i.Source, &i.Address, &i.DiscordMessageID); err != nil {
			log.Printf("Error scanning cleared incident: %v", err)
			continue
		}

		targetWebhook := getWebhookForSource(i.Source)
		if targetWebhook == "" {
			log.Printf("No webhook configured for source %s, skipping update", i.Source)
			continue
		}

		log.Printf("Found cleared incident from %s (ID: %d). Updating message.", i.Source, i.ID)
		err := updateDiscordAlert(targetWebhook, i.DiscordMessageID.String, i)
		if err != nil {
			log.Printf("Error updating Discord alert: %v", err)
			continue
		}

		_, err = db.Exec("UPDATE unified_incidents SET discord_message_id = NULL WHERE id = $1", i.ID)
		if err != nil {
			log.Printf("Error nullifying discord_message_id: %v", err)
		}
		clearedIncidentsUpdated++
		time.Sleep(2 * time.Second)
	}
	log.Printf("Processed %d cleared alerts.", clearedIncidentsUpdated)
	log.Println("Run complete.")
}
