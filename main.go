package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// UnifiedIncident matches the structure of our new database table.
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

// Camera struct for holding results from our nearby search.
type Camera struct {
	Name     string
	ImageURL string
}

// Structs for creating a rich Discord Embed
type DiscordWebhookPayload struct {
	Username  string         `json:"username"`
	AvatarURL string         `json:"avatar_url,omitempty"`
	Embeds    []DiscordEmbed `json:"embeds"`
}

type DiscordEmbed struct {
	Title     string         `json:"title"`
	Color     int            `json:"color"`
	Fields    []EmbedField   `json:"fields"`
	Footer    EmbedFooter    `json:"footer"`
	Timestamp string         `json:"timestamp"`
	Thumbnail EmbedThumbnail `json:"thumbnail,omitempty"`
}

type EmbedThumbnail struct {
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

// loadSentIncidents reads the JSON file of sent alert IDs into a map.
func loadSentIncidents(filename string) (map[int]bool, error) {
	sentIDs := make(map[int]bool)
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return sentIDs, nil
	} else if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return sentIDs, nil
	}
	err = json.Unmarshal(data, &sentIDs)
	if err != nil {
		log.Printf("WARNING: Could not parse %s. File may be corrupt. Starting fresh. Error: %v", filename, err)
		return make(map[int]bool), nil
	}
	return sentIDs, nil
}

// saveSentIncidents writes the updated map of sent alert IDs back to the file.
func saveSentIncidents(filename string, sentIDs map[int]bool) error {
	data, err := json.MarshalIndent(sentIDs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// --- NEW: Geospatial Query Function ---
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

// sendDiscordAlert determines which format to use and sends the alert.
func sendDiscordAlert(db *sql.DB, webhookURL, mapsAPIKey string, incident UnifiedIncident) (string, error) {
	if incident.Source == "NCDOT" {
		return sendNcdotAlert(db, webhookURL, mapsAPIKey, incident)
	} else if incident.Source == "RWECC" {
		return sendRweccAlert(db, webhookURL, mapsAPIKey, incident)
	}
	return "", fmt.Errorf("unknown incident source: %s", incident.Source)
}

// sendNcdotAlert now accepts a DB connection to find nearby cameras.
func sendNcdotAlert(db *sql.DB, webhookURL, mapsAPIKey string, incident UnifiedIncident) (string, error) {
	var ncdotDetails struct {
		Reason   string `json:"reason"`
		Road     string `json:"road"`
		Location string `json:"location"`
		Severity int    `json:"severity"`
	}
	if err := json.Unmarshal(incident.Details, &ncdotDetails); err != nil {
		return "", fmt.Errorf("error unmarshalling NCDOT details: %w", err)
	}

	var color int
	switch ncdotDetails.Severity {
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
		{Name: "Reason", Value: ncdotDetails.Reason, Inline: false},
		{Name: "Road", Value: ncdotDetails.Road, Inline: false},
		{Name: "Location", Value: ncdotDetails.Location, Inline: false},
		{Name: "Severity", Value: strconv.Itoa(ncdotDetails.Severity), Inline: false},
	}

	// --- ENRICHMENT STEP ---
	if incident.Latitude.Valid && incident.Longitude.Valid {
		nearbyCameras, err := findNearbyCameras(db, incident.Latitude.Float64, incident.Longitude.Float64, 3)
		if err != nil {
			log.Printf("Could not fetch nearby cameras: %v", err)
		} else if len(nearbyCameras) > 0 {
			var cameraLinks []string
			for _, cam := range nearbyCameras {
				cameraLinks = append(cameraLinks, fmt.Sprintf("[%s](%s)", cam.Name, cam.ImageURL))
			}
			fields = append(fields, EmbedField{Name: "Nearby Cameras", Value: strings.Join(cameraLinks, "\n"), Inline: false})
		}
	}

	embed := DiscordEmbed{
		Title: "🚨 NC DOT - New Vehicle Crash 🚨", Color: color, Fields: fields,
		Footer: EmbedFooter{Text: "Source: NC DOT API"}, Timestamp: incident.Timestamp.Format(time.RFC3339),
	}

	if mapsAPIKey != "" && incident.Latitude.Valid && incident.Longitude.Valid {
		mapURL := fmt.Sprintf("https://maps.googleapis.com/maps/api/staticmap?center=%.6f,%.6f&zoom=14&size=300x300&markers=color:red%%7C%.6f,%.6f&key=%s",
			incident.Latitude.Float64, incident.Longitude.Float64, incident.Latitude.Float64, incident.Longitude.Float64, mapsAPIKey)
		embed.Thumbnail = EmbedThumbnail{URL: mapURL}
	}

	payload := DiscordWebhookPayload{Username: "Unified Alert Bot", Embeds: []DiscordEmbed{embed}}
	return postToWebhook(webhookURL, payload)
}

// sendRweccAlert now accepts a DB connection to find nearby cameras.
func sendRweccAlert(db *sql.DB, webhookURL, mapsAPIKey string, incident UnifiedIncident) (string, error) {
	var rweccDetails struct {
		Problem      string `json:"problem"`
		Jurisdiction string `json:"jurisdiction"`
	}
	if err := json.Unmarshal(incident.Details, &rweccDetails); err != nil {
		return "", fmt.Errorf("error unmarshalling RWECC details: %w", err)
	}

	fields := []EmbedField{
		{Name: "Address", Value: incident.Address, Inline: false},
		{Name: "Jurisdiction", Value: rweccDetails.Jurisdiction, Inline: false},
	}

	// --- ENRICHMENT STEP ---
	if incident.Latitude.Valid && incident.Longitude.Valid {
		nearbyCameras, err := findNearbyCameras(db, incident.Latitude.Float64, incident.Longitude.Float64, 3)
		if err != nil {
			log.Printf("Could not fetch nearby cameras: %v", err)
		} else if len(nearbyCameras) > 0 {
			var cameraLinks []string
			for _, cam := range nearbyCameras {
				cameraLinks = append(cameraLinks, fmt.Sprintf("[%s](%s)", cam.Name, cam.ImageURL))
			}
			fields = append(fields, EmbedField{Name: "Nearby Cameras", Value: strings.Join(cameraLinks, "\n"), Inline: false})
		}
	}

	embed := DiscordEmbed{
		Title: "🔵 " + rweccDetails.Problem + " 🔵", Color: 3447003, Fields: fields,
		Footer: EmbedFooter{Text: "Source: Raleigh-Wake ECC"}, Timestamp: incident.Timestamp.Format(time.RFC3339),
	}

	if mapsAPIKey != "" && incident.Latitude.Valid && incident.Longitude.Valid {
		mapURL := fmt.Sprintf("https://maps.googleapis.com/maps/api/staticmap?center=%.6f,%.6f&zoom=14&size=300x300&markers=color:red%%7C%.6f,%.6f&key=%s",
			incident.Latitude.Float64, incident.Longitude.Float64, incident.Latitude.Float64, incident.Longitude.Float64, mapsAPIKey)
		embed.Thumbnail = EmbedThumbnail{URL: mapURL}
	}

	payload := DiscordWebhookPayload{Username: "Unified Alert Bot", Embeds: []DiscordEmbed{embed}}
	return postToWebhook(webhookURL, payload)
}

// postToWebhook posts a new message and returns the created message's ID.
func postToWebhook(webhookURL string, payload DiscordWebhookPayload) (string, error) {
	webhookURL += "?wait=true"
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("error creating JSON payload: %w", err)
	}
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return "", fmt.Errorf("error sending to Discord: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("discord returned non-2xx status: %s. Body: %s", resp.Status, string(body))
	}
	var message struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return "", fmt.Errorf("error decoding Discord response: %w", err)
	}
	return message.ID, nil
}

// updateDiscordAlert edits an existing Discord message to show it's cleared.
func updateDiscordAlert(webhookURL, messageID string, incident UnifiedIncident) error {
	embed := DiscordEmbed{
		Title: "✅ Incident Cleared ✅", Color: 3066993,
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
		log.Println("Note: .env file not found")
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

	webhookURL := os.Getenv("DISCORD_HOOK")
	mapsAPIKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	stateFilename := "sent_unified_alerts.json"
	if webhookURL == "" {
		log.Fatalln("Error: DISCORD_HOOK must be set")
	}
	sentIDs, err := loadSentIncidents(stateFilename)
	if err != nil {
		log.Fatalf("Error loading sent incidents: %s", err)
	}

	// Step 1: Process New Incidents
	rows, err := db.Query("SELECT id, source, source_id, event_type, address, latitude, longitude, timestamp, details FROM unified_incidents WHERE status = 'active'")
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
		if !sentIDs[i.ID] {
			log.Printf("Found new unified incident from %s (ID: %s). Sending alert.", i.Source, i.SourceID)
			messageID, err := sendDiscordAlert(db, webhookURL, mapsAPIKey, i)
			if err != nil {
				log.Printf("Error sending Discord alert: %v", err)
				continue
			}
			_, err = db.Exec("UPDATE unified_incidents SET discord_message_id = $1 WHERE id = $2", messageID, i.ID)
			if err != nil {
				log.Printf("Error saving discord_message_id: %v", err)
			}
			sentIDs[i.ID] = true
			newIncidentsFound++
			time.Sleep(2 * time.Second)
		}
	}
	if newIncidentsFound > 0 {
		if err := saveSentIncidents(stateFilename, sentIDs); err != nil {
			log.Printf("Error saving sent incidents file: %s", err)
		}
	}
	log.Printf("Processed %d new alerts.", newIncidentsFound)

	// Step 2: Process Cleared Incidents
	clearedRows, err := db.Query("SELECT id, source, address, discord_message_id FROM unified_incidents WHERE status = 'cleared' AND discord_message_id IS NOT NULL")
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
		log.Printf("Found cleared incident from %s (ID: %d). Updating message.", i.Source, i.ID)
		err := updateDiscordAlert(webhookURL, i.DiscordMessageID.String, i)
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
