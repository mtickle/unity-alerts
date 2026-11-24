package main

import (
	_ "time/tzdata"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
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
		return ""
	}
}

// captureCameraImage downloads a camera image and saves it to a temporary file.
func captureCameraImage(db *sql.DB, incidentID int, camera Camera) (string, string, error) {
	captureStart := time.Now()

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
	filePath := filepath.Join(os.TempDir(), fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	bytesWritten, err := io.Copy(file, resp.Body)
	if err != nil {
		os.Remove(filePath)
		return "", "", fmt.Errorf("failed to save image to file: %w", err)
	}

	_, err = db.Exec("INSERT INTO camera_captures (incident_id, camera_name, file_path) VALUES ($1, $2, $3)",
		incidentID, camera.Name, filePath)
	if err != nil {
		slog.Warn("camera_capture_db_log_failed",
			"incident_id", incidentID,
			"camera", camera.Name,
			"error", err,
		)
	}

	slog.Info("camera_frame_captured",
		"incident_id", incidentID,
		"camera", camera.Name,
		"bytes", bytesWritten,
		"duration_ms", time.Since(captureStart).Milliseconds(),
	)
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
func sendDiscordAlert(db *sql.DB, webhookURL, mapsAPIKey string, incident UnifiedIncident) (string, bool, error) {
	var payload DiscordWebhookPayload
	var attachmentPath, attachmentName string

	var nearbyCameras []Camera
	if incident.Latitude.Valid && incident.Longitude.Valid {
		camStart := time.Now()
		var err error
		nearbyCameras, err = findNearbyCameras(db, incident.Latitude.Float64, incident.Longitude.Float64, 3)
		if err != nil {
			slog.Warn("nearby_cameras_query_failed",
				"incident_id", incident.ID,
				"error", err,
			)
		} else {
			closestName := ""
			if len(nearbyCameras) > 0 {
				closestName = nearbyCameras[0].Name
			}
			slog.Info("nearby_cameras_queried",
				"incident_id", incident.ID,
				"cameras_found", len(nearbyCameras),
				"closest_camera", closestName,
				"duration_ms", time.Since(camStart).Milliseconds(),
			)
		}
	} else {
		slog.Warn("incident_missing_coordinates",
			"incident_id", incident.ID,
			"source", incident.Source,
			"address", incident.Address,
		)
	}

	if len(nearbyCameras) > 0 {
		var err error
		attachmentPath, attachmentName, err = captureCameraImage(db, incident.ID, nearbyCameras[0])
		if err != nil {
			slog.Warn("camera_frame_capture_failed",
				"incident_id", incident.ID,
				"camera", nearbyCameras[0].Name,
				"error", err,
			)
			attachmentPath = ""
			attachmentName = ""
		}
	}
	if attachmentPath != "" {
		defer os.Remove(attachmentPath)
	}

	if incident.Source == "NCDOT" {
		payload = buildNcdotPayload(mapsAPIKey, incident, nearbyCameras, attachmentName)
	} else if incident.Source == "RWECC" {
		payload = buildRweccPayload(mapsAPIKey, incident, nearbyCameras, attachmentName)
	} else {
		return "", false, fmt.Errorf("unknown incident source: %s", incident.Source)
	}

	hasAttachment := attachmentName != ""
	if hasAttachment {
		payload.Attachments = []DiscordAttachment{
			{
				ID:       0,
				Filename: attachmentName,
			},
		}
	}

	msgID, err := postMultipartToWebhook(webhookURL, payload, attachmentPath)
	return msgID, hasAttachment, err
}

// buildNcdotPayload creates the multi-embed structure for an NC DOT alert.
func buildNcdotPayload(mapsAPIKey string, incident UnifiedIncident, nearbyCameras []Camera, attachmentName string) DiscordWebhookPayload {
	var rawIncident struct {
		EventSubType      string `json:"EventSubType"`
		RoadwayName       string `json:"RoadwayName"`
		Description       string `json:"Description"`
		Severity          string `json:"Severity"`
		DirectionOfTravel string `json:"DirectionOfTravel"`
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
		slog.Debug("details_legacy_format_fallback", "incident_id", incident.ID, "source", "NCDOT")
		json.Unmarshal(incident.Details, &rawIncident)
	}

	var color int
	switch strings.ToLower(rawIncident.Severity) {
	case "high":
		color = 15158332 // Red
	case "medium":
		color = 16776960 // Yellow
	case "low":
		color = 3066993 // Green
	default:
		color = 2105893 // Gray fallback
	}

	fields := []EmbedField{
		{Name: "Type", Value: rawIncident.EventSubType, Inline: false},
		{Name: "Road", Value: rawIncident.RoadwayName + " (" + rawIncident.DirectionOfTravel + ")", Inline: false},
		{Name: "Description", Value: rawIncident.Description, Inline: false},
		{Name: "Severity", Value: rawIncident.Severity, Inline: false},
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
		Title: "NC DOT - Incident Alert", Color: color, Fields: fields,
		Footer: EmbedFooter{Text: "Source: NC DOT API"}, Timestamp: incident.Timestamp.Format(time.RFC3339),
	}

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
		slog.Debug("details_legacy_format_fallback", "incident_id", incident.ID, "source", "RWECC")
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
		Title: rawIncident.Problem, Color: 3447003, Fields: fields,
		Footer: EmbedFooter{Text: "Source: Raleigh-Wake ECC"}, Timestamp: incident.Timestamp.Format(time.RFC3339),
	}

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

	client := &http.Client{Timeout: 15 * time.Second}
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
	client := &http.Client{Timeout: 15 * time.Second}
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
	cycleStart := time.Now()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := godotenv.Load(); err != nil {
		slog.Debug("dotenv_not_loaded", "reason", "using container/environment variables")
	}

	dbHost := os.Getenv("DATABASE_HOST")
	mapsAPIKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	notifyDiscord := os.Getenv("NOTIFY_DISCORD")
	hasNcdotHook := os.Getenv("DISCORD_HOOK_NCDOT") != ""
	hasRweccHook := os.Getenv("DISCORD_HOOK_RWECC") != ""

	slog.Info("alert_cycle_started",
		"service", "unity-alerts",
		"db_host", dbHost,
		"ncdot_hook_configured", hasNcdotHook,
		"rwecc_hook_configured", hasRweccHook,
		"static_maps_enabled", mapsAPIKey != "",
		"dry_run_mode", notifyDiscord == "0",
	)

	if !hasNcdotHook && !hasRweccHook {
		slog.Warn("no_discord_webhooks_configured")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		dbHost, os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_USERNAME"),
		os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		slog.Error("db_open_failed", "db_host", dbHost, "error", err)
		os.Exit(1)
	}
	defer db.Close()

	dbStart := time.Now()
	if err := db.Ping(); err != nil {
		slog.Error("db_ping_failed", "db_host", dbHost, "error", err)
		os.Exit(1)
	}
	slog.Info("db_connected", "db_host", dbHost, "latency_ms", time.Since(dbStart).Milliseconds())

	// Step 1: Process New Incidents
	rows, err := db.Query("SELECT id, source, source_id, event_type, address, latitude, longitude, timestamp, details FROM unified_incidents WHERE status = 'active' AND discord_message_id IS NULL AND source != 'ArcGIS_Police'")
	if err != nil {
		slog.Error("new_incidents_query_failed", "error", err)
		os.Exit(1)
	}
	defer rows.Close()

	var newIncidentsFound, newAlertsFailed, newSkipped int
	for rows.Next() {
		var i UnifiedIncident
		if err := rows.Scan(&i.ID, &i.Source, &i.SourceID, &i.EventType, &i.Address, &i.Latitude, &i.Longitude, &i.Timestamp, &i.Details); err != nil {
			newAlertsFailed++
			slog.Error("incident_row_scan_failed", "error", err)
			continue
		}

		slog.Info("new_incident_detected",
			"incident_id", i.ID,
			"source", i.Source,
			"source_id", i.SourceID,
			"event_type", i.EventType,
			"address", i.Address,
		)

		targetWebhook := getWebhookForSource(i.Source)
		if targetWebhook == "" {
			newSkipped++
			slog.Warn("webhook_missing_for_source", "incident_id", i.ID, "source", i.Source)
			continue
		}

		if notifyDiscord == "0" {
			newSkipped++
			slog.Info("discord_dispatch_skipped_dry_run",
				"incident_id", i.ID,
				"source", i.Source,
				"address", i.Address,
			)
			continue
		}

		sendStart := time.Now()
		messageID, hasAttachment, err := sendDiscordAlert(db, targetWebhook, mapsAPIKey, i)
		if err != nil {
			newAlertsFailed++
			slog.Error("discord_alert_send_failed",
				"incident_id", i.ID,
				"source", i.Source,
				"address", i.Address,
				"error", err,
			)
			continue
		}

		_, err = db.Exec("UPDATE unified_incidents SET discord_message_id = $1 WHERE id = $2", messageID, i.ID)
		if err != nil {
			slog.Error("discord_message_id_update_failed",
				"incident_id", i.ID,
				"message_id", messageID,
				"error", err,
			)
		}

		newIncidentsFound++
		slog.Info("discord_alert_sent",
			"incident_id", i.ID,
			"source", i.Source,
			"address", i.Address,
			"discord_message_id", messageID,
			"camera_attached", hasAttachment,
			"dispatch_duration_ms", time.Since(sendStart).Milliseconds(),
		)
		time.Sleep(2 * time.Second)
	}

	// Step 2: Process Cleared Incidents
	clearedRows, err := db.Query("SELECT id, source, address, discord_message_id FROM unified_incidents WHERE status = 'cleared' AND discord_message_id IS NOT NULL AND source != 'ArcGIS_Police'")
	if err != nil {
		slog.Error("cleared_incidents_query_failed", "error", err)
		os.Exit(1)
	}
	defer clearedRows.Close()

	var clearedIncidentsUpdated, clearedFailed int
	for clearedRows.Next() {
		var i UnifiedIncident
		if err := clearedRows.Scan(&i.ID, &i.Source, &i.Address, &i.DiscordMessageID); err != nil {
			clearedFailed++
			slog.Error("cleared_row_scan_failed", "error", err)
			continue
		}

		targetWebhook := getWebhookForSource(i.Source)
		if targetWebhook == "" {
			slog.Warn("webhook_missing_for_cleared_source", "incident_id", i.ID, "source", i.Source)
			continue
		}

		patchStart := time.Now()
		if err := updateDiscordAlert(targetWebhook, i.DiscordMessageID.String, i); err != nil {
			clearedFailed++
			slog.Error("discord_alert_clear_failed",
				"incident_id", i.ID,
				"source", i.Source,
				"discord_message_id", i.DiscordMessageID.String,
				"error", err,
			)
			continue
		}

		_, err = db.Exec("UPDATE unified_incidents SET discord_message_id = NULL WHERE id = $1", i.ID)
		if err != nil {
			slog.Error("discord_message_id_nullify_failed",
				"incident_id", i.ID,
				"error", err,
			)
		}

		clearedIncidentsUpdated++
		slog.Info("discord_alert_cleared",
			"incident_id", i.ID,
			"source", i.Source,
			"address", i.Address,
			"discord_message_id", i.DiscordMessageID.String,
			"duration_ms", time.Since(patchStart).Milliseconds(),
		)
		time.Sleep(2 * time.Second)
	}

	slog.Info("alert_cycle_completed",
		"service", "unity-alerts",
		"new_alerts_sent", newIncidentsFound,
		"new_alerts_skipped", newSkipped,
		"new_alerts_failed", newAlertsFailed,
		"cleared_alerts_updated", clearedIncidentsUpdated,
		"cleared_alerts_failed", clearedFailed,
		"total_duration_ms", time.Since(cycleStart).Milliseconds(),
	)
}
