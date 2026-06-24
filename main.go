// VoidScout Uploader — companion daemon that watches WoW SavedVariables
// and uploads scored fights to api.voidscout.io.
//
// Design:
//   1. Find VoidScout.lua in the user's WoW install (auto-detect).
//   2. Watch it for changes (fsnotify).
//   3. On change, parse via gopher-lua, find fights newer than our last
//      checkpoint, POST them as batches to /api/ingest/fight.
//   4. Persist checkpoint to ~/.voidscout-uploader/state.json so we don't
//      re-upload on restart.
//
// No HMAC — public endpoint with rate-limiting + sanity checks server-side.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	lua "github.com/yuin/gopher-lua"
)

const (
	defaultAPIBase = "https://api.voidscout.io"
	stateDirName   = ".voidscout-uploader"
	stateFileName  = "state.json"
	pollInterval   = 5 * time.Second // mtime polling — WoW atomic-replace defeats fsnotify on Windows
	batchSize      = 100             // max fights per HTTP request

	// Profile upload — uploaded on startup + periodic refresh.
	// Server caps batches at 100 (MAX_PROFILES_PER_REQUEST in profile_endpoint.py).
	profileBatchSize     = 50
	profileLoopInterval  = 5 * time.Minute
	profileStartupDelay  = 5 * time.Second
	profileMaxAgeDays    = 30 // skip players we haven't seen in this many days

	// Auto-update
	updateRepo    = "bughatti/voidscout-uploader"   // public GitHub repo for releases
	updateCheckTimeout = 10 * time.Second
	currentVersion = "0.4.1"                         // bumped on each release; compared to GitHub

	// Combat log scan cadence — the addon auto-toggles /combatlog on
	// encounter/run boundaries, so files appear and stabilize at that
	// rhythm. 30s ticks comfortably catch stable files without thrashing.
	combatLogScanInterval = 30 * time.Second

	// VRT Session Recorder scan cadence. Lighter than combat logs since
	// sessions are smaller (one JSON-shaped table per encounter).
	sessionScanInterval = 60 * time.Second
	sessionStartupDelay = 15 * time.Second
)

// ---- types matching the addon's SavedVariables shape ----

type AddonFight struct {
	EncounterID   int                    `json:"encounter_id"`
	EncounterName string                 `json:"encounter_name"`
	DifficultyID  int                    `json:"difficulty_id"`
	Outcome       string                 `json:"outcome"`
	DurationSec   int                    `json:"duration_sec"`
	Timestamp     int64                  `json:"timestamp"`
	PugID         string                 `json:"pug_id"`
	Class         string                 `json:"class"`
	Spec          string                 `json:"spec"`
	Mode          string                 `json:"mode"`           // "raid" | "mplus" | "dungeon"
	Uploaded      bool                   `json:"uploaded"`
	Axes          map[string]float64     `json:"axes"`
	Roster        []string               `json:"roster"`
	Raw           map[string]interface{} `json:"raw,omitempty"`         // dps, casts, avoidable_taken, etc — peer pool seed
	RunID         string                 `json:"run_id,omitempty"`      // groups M+ pulls into one run-event
	DataQuality   string                 `json:"data_quality,omitempty"` // "ok" or "stale" (DC/AFK/reset)
}

type IngestFight struct {
	PlayerName    string                 `json:"player_name"`
	PlayerRealm   string                 `json:"player_realm,omitempty"`
	PlayerRegion  string                 `json:"player_region,omitempty"`
	PlayerClass   string                 `json:"player_class,omitempty"`
	PlayerSpec    string                 `json:"player_spec,omitempty"`
	EncounterID   int                    `json:"encounter_id"`
	EncounterName string                 `json:"encounter_name"`
	DifficultyID  int                    `json:"difficulty_id"`
	Outcome       string                 `json:"outcome"`
	DurationSec   int                    `json:"duration_sec"`
	Timestamp     int64                  `json:"timestamp"`
	PugID         string                 `json:"pug_id,omitempty"`
	Mode          string                 `json:"mode,omitempty"`   // "raid" | "mplus" | "dungeon"
	Axes          map[string]float64     `json:"axes"`
	Roster        []string               `json:"roster,omitempty"`
	Raw           map[string]interface{} `json:"raw,omitempty"`
	RunID         string                 `json:"run_id,omitempty"`
	DataQuality   string                 `json:"data_quality,omitempty"`
}

type IngestPayload struct {
	Contributor string        `json:"contributor"`
	Fights      []IngestFight `json:"fights"`
}

type IngestResponse struct {
	Status         string         `json:"status"`
	Auth           string         `json:"auth"`
	FightsAccepted int            `json:"fights_accepted"`
	FightsSkipped  int            `json:"fights_skipped"`
	SkipReasons    map[string]int `json:"skip_reasons"`
}

// CombatLogRetry tracks backoff state for a single combat log file. Persisted
// so the uploader can't burn the daily rate limit by retrying every 30s
// across a process restart. On 200 the entry is cleared and the file is
// added to UploadedCombatLogs.
type CombatLogRetry struct {
	NextEligibleUnix int64  `json:"next_eligible_unix"` // skip the file until this wall time
	AttemptCount     int    `json:"attempt_count"`      // 1, 2, 3, ... — fed into backoff
	LastErrorCode    int    `json:"last_error_code"`    // most-recent HTTP code (or 0 for transport error)
	LastErrorAt      int64  `json:"last_error_at"`      // unix ts of most recent attempt
	LastErrorMsg     string `json:"last_error_msg,omitempty"`
}

type State struct {
	LastUploadedTs      int64           `json:"last_uploaded_ts"` // upload all fights with ts > this
	Contributor         string          `json:"contributor"`      // last-known uploading character
	UploadedCombatLogs  map[string]bool `json:"uploaded_combat_logs,omitempty"` // filename -> uploaded?
	UploadedRealm       string          `json:"uploaded_realm,omitempty"`       // last-known realm
	UploadedRegion      string          `json:"uploaded_region,omitempty"`      // last-known region
	// Per-file retry state. Empty/missing entries mean "go ahead and try."
	CombatLogRetry      map[string]*CombatLogRetry `json:"combat_log_retry,omitempty"`
	// VRT Session Recorder uploads — session_id -> uploaded? Tracked here
	// so we don't re-POST the same session if the addon's pending_uploads
	// queue keeps it around (the addon can't drop entries without /reload).
	UploadedSessions    map[string]bool `json:"uploaded_sessions,omitempty"`
	// Opt-out request — last requested_at we've successfully POSTed to
	// /api/opt-out. The addon writes VoidScoutDB.opt_out_requested when
	// the user clicks the "Delete + go local" button; the uploader picks
	// it up on next run and POSTs the deletion request to the server.
	LastOptOutTs        int64           `json:"last_opt_out_ts,omitempty"`
}

// ProfileUpload mirrors PlayerScan's per-player record shape — only the
// fields the server cares about. Extras are dropped silently.
type ProfileUpload struct {
	Slug              string                    `json:"slug"`
	Name              string                    `json:"name"`
	Realm             string                    `json:"realm"`
	RealmSlug         string                    `json:"realm_slug"`
	Region            string                    `json:"region"`
	Class             string                    `json:"class,omitempty"`
	Race              string                    `json:"race,omitempty"`
	Faction           string                    `json:"faction,omitempty"`
	Level             int                       `json:"level,omitempty"`
	Guild             string                    `json:"guild,omitempty"`
	Ilvl              int                       `json:"ilvl,omitempty"`
	Spec              string                    `json:"spec,omitempty"`
	SpecID            int                       `json:"spec_id,omitempty"`
	Achievements      map[string]bool           `json:"achievements,omitempty"`
	BossKills         map[string]bool           `json:"boss_kills,omitempty"`
	AchievementPoints int                       `json:"achievement_points,omitempty"`
	RioScore          *float64                  `json:"rio_score,omitempty"`
	RioScorePrev      *float64                  `json:"rio_score_prev,omitempty"`
	Archon            map[string]map[string]any `json:"archon,omitempty"`
	Sources           []string                  `json:"sources,omitempty"`
	LastSeen          int64                     `json:"-"` // local-only for filter logic
}

type ProfileBatch struct {
	SubmitterGUID string          `json:"submitter_guid"`
	Profiles      []ProfileUpload `json:"profiles"`
}

type ProfileResponse struct {
	Accepted int      `json:"accepted"`
	Filtered int      `json:"filtered"`
	Errors   []string `json:"errors"`
}

// ============================================================
// Auto-update — checks GitHub Releases API on launch, downloads
// newer version if available, atomically replaces self, restarts.
// ============================================================

type ghRelease struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	} `json:"assets"`
}

// checkForUpdate queries the GitHub Releases API for the latest tag.
// Returns (newerVersionTag, assetURL) if an update is available, else ("", "").
// Designed to be silent on failure — never blocks startup.
func checkForUpdate() (string, string) {
	client := &http.Client{Timeout: updateCheckTimeout}
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", updateRepo)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", ""
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "VoidScoutUploader/"+currentVersion)
	resp, err := client.Do(req)
	if err != nil {
		return "", ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", ""
	}
	var rel ghRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return "", ""
	}
	// Strip leading "v" from tag (e.g. "v0.3.0" -> "0.3.0")
	latest := strings.TrimPrefix(rel.TagName, "v")
	if !semverGreater(latest, currentVersion) {
		return "", ""
	}
	// Find the asset matching this OS/arch
	wanted := assetNameForCurrent()
	for _, a := range rel.Assets {
		if a.Name == wanted {
			return latest, a.BrowserDownloadURL
		}
	}
	return "", ""
}

// assetNameForCurrent returns the release artifact filename for this
// platform — matches what build.sh emits.
func assetNameForCurrent() string {
	osName := runtime.GOOS
	arch := runtime.GOARCH
	ext := ""
	if osName == "windows" {
		ext = ".exe"
	}
	return fmt.Sprintf("voidscout-uploader-%s-%s%s", osName, arch, ext)
}

// semverGreater returns true if a > b for "X.Y.Z" version strings.
// Tolerant of pre-release suffixes (just compares the numeric prefix).
func semverGreater(a, b string) bool {
	pa := parseSemver(a)
	pb := parseSemver(b)
	for i := 0; i < 3; i++ {
		if pa[i] > pb[i] {
			return true
		}
		if pa[i] < pb[i] {
			return false
		}
	}
	return false
}
func parseSemver(s string) [3]int {
	// Drop anything after first "-" (pre-release tag)
	if idx := strings.Index(s, "-"); idx >= 0 {
		s = s[:idx]
	}
	parts := strings.Split(s, ".")
	var out [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		var n int
		fmt.Sscanf(parts[i], "%d", &n)
		out[i] = n
	}
	return out
}

// downloadAndReplace downloads the new binary to a temp file, then atomically
// replaces the current executable + restarts. Returns the new exec path.
func downloadAndReplace(downloadURL string) (string, error) {
	resp, err := http.Get(downloadURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("download HTTP %d", resp.StatusCode)
	}

	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	// Write to temp file next to current executable, then rename over it
	tmp := exe + ".new"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return "", err
	}
	if _, err := copyResponseTo(resp, out); err != nil {
		out.Close()
		os.Remove(tmp)
		return "", err
	}
	out.Close()

	// On Windows we can't replace a running .exe directly — rename current to
	// .old, move .new into place. Old .old gets cleaned up on next launch.
	if runtime.GOOS == "windows" {
		old := exe + ".old"
		os.Remove(old) // clean up previous .old if any
		if err := os.Rename(exe, old); err != nil {
			os.Remove(tmp)
			return "", fmt.Errorf("rename current → .old: %w", err)
		}
		if err := os.Rename(tmp, exe); err != nil {
			// Try to roll back
			os.Rename(old, exe)
			return "", fmt.Errorf("rename .new → current: %w", err)
		}
	} else {
		// Unix: atomic rename works even on running binary
		if err := os.Rename(tmp, exe); err != nil {
			return "", err
		}
	}
	return exe, nil
}

func copyResponseTo(resp *http.Response, w *os.File) (int64, error) {
	buf := make([]byte, 64*1024)
	var total int64
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if err != nil {
			if err.Error() == "EOF" {
				return total, nil
			}
			return total, err
		}
	}
}

// performAutoUpdate is the public entry point — called once at startup.
// Silent on any failure; logs only on success.
func performAutoUpdate(verbose bool) {
	defer func() { _ = recover() }() // any panic = silent skip
	newVer, url := checkForUpdate()
	if newVer == "" {
		if verbose {
			log.Printf("auto-update: no newer version (current=%s)", currentVersion)
		}
		return
	}
	log.Printf("auto-update: new version %s available — downloading...", newVer)
	exe, err := downloadAndReplace(url)
	if err != nil {
		log.Printf("auto-update: failed: %v", err)
		return
	}
	log.Printf("auto-update: replaced %s — please restart for changes to take effect", exe)
	// Note: we don't auto-exec the new binary here because the running process
	// has open file handles, watchers, etc. Cleaner to log + let user restart,
	// or let the process exit naturally on next reboot.
}

// syncWriter forces an fsync after every Write so log output is visible to
// `tail -f` immediately rather than sitting in the kernel page cache.
type syncWriter struct{ f *os.File }

func (w *syncWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	_ = w.f.Sync()
	return n, err
}

// ---- VRT Session Recorder upload ----
//
// Reads VoidRaidToolsReader.lua, walks VoidRaidToolsReaderDB.pending_uploads,
// JSONifies each export-shaped payload, POSTs to /api/v1/sessions/upload.
// Tracks uploaded session_ids in state.UploadedSessions so we don't double-
// post entries that haven't rolled off the addon's cap-20 queue yet.

// detectVRTReaderSavedVarsPath finds VoidRaidToolsReader.lua under the
// same WoW install hierarchy as VoidScout.lua. Returns most-recently-
// modified candidate.
func detectVRTReaderSavedVarsPath() (string, error) {
	candidates := wowAccountRoots()
	var best string
	var bestMtime time.Time
	for _, root := range candidates {
		path := filepath.Join(root, "SavedVariables", "VoidRaidToolsReader.lua")
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		if best == "" || st.ModTime().After(bestMtime) {
			best = path
			bestMtime = st.ModTime()
		}
	}
	if best == "" {
		return "", fmt.Errorf("no VoidRaidToolsReader.lua found under any of %d candidate WoW installs", len(candidates))
	}
	return best, nil
}

// luaTableToMap walks a *lua.LTable and produces a JSON-friendly
// map[string]interface{}. Lists with sequential int keys become []interface{}.
// Used for session payloads where event/aura array entries are polymorphic.
func luaValueToInterface(v lua.LValue) interface{} {
	switch x := v.(type) {
	case lua.LNumber:
		f := float64(x)
		if f == float64(int64(f)) {
			return int64(f)
		}
		return f
	case lua.LString:
		return string(x)
	case lua.LBool:
		return bool(x)
	case *lua.LTable:
		return luaTableToInterface(x)
	case *lua.LNilType:
		return nil
	}
	return nil
}

func luaTableToInterface(t *lua.LTable) interface{} {
	if t == nil {
		return nil
	}
	// Detect array-shape: 1..N integer keys, nothing else.
	isArray := true
	maxIdx := 0
	keyCount := 0
	t.ForEach(func(k, _ lua.LValue) {
		keyCount++
		if n, ok := k.(lua.LNumber); ok {
			i := int(n)
			if float64(i) == float64(n) && i >= 1 {
				if i > maxIdx {
					maxIdx = i
				}
				return
			}
		}
		isArray = false
	})
	if isArray && keyCount == maxIdx && keyCount > 0 {
		out := make([]interface{}, maxIdx)
		t.ForEach(func(k, v lua.LValue) {
			if n, ok := k.(lua.LNumber); ok {
				out[int(n)-1] = luaValueToInterface(v)
			}
		})
		return out
	}
	out := make(map[string]interface{})
	t.ForEach(func(k, v lua.LValue) {
		out[k.String()] = luaValueToInterface(v)
	})
	return out
}

// parseVRTReaderSessions returns the pending_uploads list from the
// SavedVariables file. Each entry has {queued_ts, status, payload}.
// We only care about payload (the export-shaped session).
func parseVRTReaderSessions(path string) ([]map[string]interface{}, error) {
	L := lua.NewState()
	defer L.Close()
	if err := L.DoFile(path); err != nil {
		return nil, fmt.Errorf("execute lua: %w", err)
	}
	db := L.GetGlobal("VoidRaidToolsReaderDB")
	if db.Type() != lua.LTTable {
		return nil, nil // file exists but no addon DB (first launch)
	}
	queue := db.(*lua.LTable).RawGetString("pending_uploads")
	if queue.Type() != lua.LTTable {
		return nil, nil
	}
	var out []map[string]interface{}
	queue.(*lua.LTable).ForEach(func(_, entry lua.LValue) {
		if entry.Type() != lua.LTTable {
			return
		}
		payload := entry.(*lua.LTable).RawGetString("payload")
		if payload.Type() != lua.LTTable {
			return
		}
		m, ok := luaTableToInterface(payload.(*lua.LTable)).(map[string]interface{})
		if !ok {
			return
		}
		out = append(out, m)
	})
	return out, nil
}

// uploadSession POSTs a single session payload as JSON to the server.
// Returns the HTTP status code + any error.
func uploadSession(apiBase string, payload map[string]interface{}, contributor string) (int, error) {
	body, err := json.Marshal(map[string]interface{}{
		"contributor": contributor,
		"session":     payload,
	})
	if err != nil {
		return 0, err
	}
	url := strings.TrimRight(apiBase, "/") + "/api/v1/sessions/upload"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "voidscout-uploader/"+currentVersion)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("server: %s", string(b))
	}
	return resp.StatusCode, nil
}

// scanSessions reads the Reader SavedVariables, finds pending sessions
// the server hasn't seen, uploads them. Called from a periodic loop.
func scanSessions(apiBase string, state *State, statePath string, dryRun, verbose bool) {
	path, err := detectVRTReaderSavedVarsPath()
	if err != nil {
		if verbose {
			log.Printf("session-scan: %v", err)
		}
		return
	}
	sessions, err := parseVRTReaderSessions(path)
	if err != nil {
		log.Printf("session-scan: parse error: %v", err)
		return
	}
	if len(sessions) == 0 {
		if verbose {
			log.Printf("session-scan: queue empty")
		}
		return
	}
	if state.UploadedSessions == nil {
		state.UploadedSessions = make(map[string]bool)
	}
	var sent, skipped, failed int
	for _, payload := range sessions {
		sid, _ := payload["session_id"].(string)
		if sid == "" {
			continue
		}
		if state.UploadedSessions[sid] {
			skipped++
			continue
		}
		if dryRun {
			log.Printf("session-scan: [dry-run] would upload session %s (label=%v encounter=%v)",
				sid, payload["label"], payload["encounter"])
			sent++
			continue
		}
		status, err := uploadSession(apiBase, payload, state.Contributor)
		if err != nil {
			failed++
			log.Printf("session-scan: upload %s failed (status=%d): %v", sid, status, err)
			continue
		}
		state.UploadedSessions[sid] = true
		sent++
		if verbose {
			log.Printf("session-scan: uploaded %s (status=%d)", sid, status)
		}
	}
	if sent > 0 || failed > 0 {
		log.Printf("session-scan: %d uploaded, %d skipped (already sent), %d failed",
			sent, skipped, failed)
		if err := saveState(state, statePath); err != nil {
			log.Printf("session-scan: save state failed: %v", err)
		}
	}
	// Cap the UploadedSessions map so it doesn't grow unbounded over months
	// of sessions. The addon's queue is capped at 20, so we only need to
	// remember the last few hundred to safely skip duplicates.
	if len(state.UploadedSessions) > 500 {
		// Naive: drop random half. We don't track insertion order; the cost
		// of re-uploading a single stale entry is just one extra POST.
		dropped := 0
		for k := range state.UploadedSessions {
			delete(state.UploadedSessions, k)
			dropped++
			if dropped >= 250 {
				break
			}
		}
	}
}

// ---- main loop ----

// pidFilePath returns the fixed location for our single-instance PID file.
// Windows: %TEMP%\voidscout-uploader.pid (matches existing convention).
func pidFilePath() string {
	return filepath.Join(os.TempDir(), "voidscout-uploader.pid")
}

// isProcessAlive returns true if the given PID belongs to a running process
// whose image name matches our uploader binary. Windows-specific (tasklist).
// Returns false if PID is dead, name doesn't match, or check itself fails.
func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if runtime.GOOS != "windows" {
		// On Unix the existing flow doesn't run, but be safe: trust the PID
		// is alive only if we can signal it (signal 0 = liveness check).
		p, err := os.FindProcess(pid)
		if err != nil {
			return false
		}
		// Sending nil/0 signal is the standard Unix liveness probe.
		// On Windows this would terminate the process, so the runtime.GOOS
		// guard above is critical.
		return p.Signal(os.Signal(nil)) == nil
	}
	// Windows: shell out to tasklist. /fi "PID eq N" /fo csv /nh
	out, err := exec.Command("tasklist", "/fi", "PID eq "+strconv.Itoa(pid),
		"/fo", "csv", "/nh").Output()
	if err != nil {
		return false
	}
	// tasklist prints a tip line when no match; check for our image name.
	s := strings.ToLower(string(out))
	return strings.Contains(s, "voidscout-uploader")
}

// acquireSingleInstanceLock either returns nil (we're the only instance and
// our PID is now written to disk) or an error describing the other instance.
// Caller should defer releaseSingleInstanceLock on success.
func acquireSingleInstanceLock() error {
	pf := pidFilePath()
	if data, err := os.ReadFile(pf); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			if isProcessAlive(pid) {
				return fmt.Errorf("another voidscout-uploader is already running (PID %d). "+
					"Stop it first or wait for it to exit. PID file: %s", pid, pf)
			}
			// Stale PID file from a crashed/killed instance — overwrite.
		}
	}
	myPID := os.Getpid()
	if err := os.WriteFile(pf, []byte(strconv.Itoa(myPID)), 0o644); err != nil {
		return fmt.Errorf("write PID file %s: %w", pf, err)
	}
	return nil
}

// releaseSingleInstanceLock deletes our PID file. Best-effort; safe to call
// multiple times. Only removes the file if it still contains OUR pid (so we
// don't stomp on a successor instance).
func releaseSingleInstanceLock() {
	pf := pidFilePath()
	data, err := os.ReadFile(pf)
	if err != nil {
		return
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid == os.Getpid() {
		_ = os.Remove(pf)
	}
}

func main() {
	var (
		apiBase  = flag.String("api", defaultAPIBase, "API base URL")
		oncePath = flag.String("once", "", "If set, parse this specific SavedVariables file once and exit")
		dryRun   = flag.Bool("dry-run", false, "Parse + show what would upload but don't POST")
		verbose  = flag.Bool("v", false, "Verbose logging")
		logPath  = flag.String("log", "", "If set, write logs to this file (in addition to stdout)")
		noUpdate = flag.Bool("no-update", false, "Skip auto-update check on launch")
	)
	flag.Parse()

	// Single-instance lock — prevents the duplicate-process race we hit
	// when the launcher/scheduled-task spawned multiple uploaders that
	// then competed to upload the same SavedVariables fights.
	// One-shot mode (-once) is exempt: it parses a file and exits, doesn't
	// daemon-poll, so multiple invocations don't race.
	if *oncePath == "" {
		if err := acquireSingleInstanceLock(); err != nil {
			log.Printf("VoidScout Uploader: %v", err)
			os.Exit(0) // exit 0 — duplicate isn't a failure, the other instance handles work
		}
		defer releaseSingleInstanceLock()
	}

	log.SetFlags(log.LstdFlags)
	// Optional file logging — bypasses any shell-redirection buffering
	// when running as a background daemon. Auto-flushes per Printf.
	if *logPath != "" {
		f, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatalf("open log: %v", err)
		}
		defer f.Close()
		log.SetOutput(&syncWriter{f: f})
	}
	log.Printf("VoidScout Uploader v%s starting (api=%s)", currentVersion, *apiBase)

	// Auto-update check on launch. Silent on no-update / failure.
	// User can skip with -no-update flag.
	if !*noUpdate {
		performAutoUpdate(*verbose)
	}
	// Clean up Windows .old file from a previous self-replace
	if runtime.GOOS == "windows" {
		if exe, err := os.Executable(); err == nil {
			_ = os.Remove(exe + ".old")
		}
	}

	state, statePath := loadState()
	log.Printf("State: lastUploadedTs=%d contributor=%s (at %s)",
		state.LastUploadedTs, state.Contributor, statePath)

	// One-shot mode for testing
	if *oncePath != "" {
		runOnce(*oncePath, *apiBase, state, statePath, *dryRun, *verbose)
		return
	}

	// Auto-detect file
	svPath, err := detectSavedVariablesPath()
	if err != nil {
		log.Fatalf("Could not find VoidScout SavedVariables: %v", err)
	}
	log.Printf("Watching: %s", svPath)

	// Initial run
	runOnce(svPath, *apiBase, state, statePath, *dryRun, *verbose)

	// Profile uploader runs in the background — independent of fight uploads.
	// Reads VoidScoutDB.playerScan.players, pushes to /api/profile/batch,
	// keeps track of which slugs were last uploaded so we only re-send when
	// the addon refreshed a player's data. Strictly outbound — server never
	// gets queried back. See voidscout-data-direction memory.
	go runProfileLoop(svPath, *apiBase, *dryRun, *verbose)

	// Combat log file watcher — independent of fight + profile loops.
	// Polls Logs/WoWCombatLog-*.txt periodically, uploads any new stable
	// (60s+ idle) file to /api/upload-log. State persisted to avoid
	// re-uploading the same file twice.
	go func() {
		// Brief startup delay so the initial runOnce can populate
		// state.Contributor before the first scan.
		time.Sleep(10 * time.Second)
		ticker := time.NewTicker(combatLogScanInterval)
		defer ticker.Stop()
		for {
			scanCombatLogs(*apiBase, state, statePath, *dryRun, *verbose)
			<-ticker.C
		}
	}()

	// VRT Session Recorder uploader — polls VoidRaidToolsReader.lua's
	// pending_uploads queue, posts each session to /api/v1/sessions/upload.
	// Independent of other loops; survives addon/file mismatches silently.
	go func() {
		time.Sleep(sessionStartupDelay)
		ticker := time.NewTicker(sessionScanInterval)
		defer ticker.Stop()
		for {
			scanSessions(*apiBase, state, statePath, *dryRun, *verbose)
			<-ticker.C
		}
	}()

	// Watch (blocking)
	watch(svPath, func() {
		runOnce(svPath, *apiBase, state, statePath, *dryRun, *verbose)
	})
}

// runOnce reads the file, finds pending fights, uploads them, persists state.
func runOnce(svPath, apiBase string, state *State, statePath string, dryRun, verbose bool) {
	// Opt-out runs FIRST so even if all other uploads fail (e.g. server
	// down for fights but reachable for the small opt-out POST), the
	// user's deletion request still goes through. Persists state at the
	// end of the function so a successful opt-out is recorded.
	if !dryRun {
		checkAndSendOptOut(svPath, apiBase, state)
		// Also check the VRTReader SV — the addon writes the same flag.
		if vrtPath, _ := detectVRTReaderSavedVarsPath(); vrtPath != "" {
			checkAndSendOptOut(vrtPath, apiBase, state)
		}
	}

	fights, contributor, err := parseSavedVars(svPath)
	if err != nil {
		log.Printf("parse error: %v", err)
		return
	}
	pending := filterPending(fights, state.LastUploadedTs)
	if len(pending) == 0 {
		if verbose {
			log.Printf("Nothing new to upload (total=%d, lastTs=%d)", len(fights), state.LastUploadedTs)
		}
		return
	}
	log.Printf("Found %d new fights (total=%d) — uploading as %s", len(pending), len(fights), contributor)

	if dryRun {
		for _, f := range pending {
			log.Printf("  [DRY] %s diff=%d outcome=%s axes=%v", f.EncounterName, f.DifficultyID, f.Outcome, f.Axes)
		}
		return
	}

	// Upload in batches
	maxTs := state.LastUploadedTs
	for i := 0; i < len(pending); i += batchSize {
		end := i + batchSize
		if end > len(pending) {
			end = len(pending)
		}
		batch := pending[i:end]
		resp, err := uploadBatch(apiBase, contributor, batch)
		if err != nil {
			log.Printf("upload failed (batch %d-%d): %v", i, end, err)
			break // don't advance ts on failure
		}
		log.Printf("Uploaded %d/%d accepted=%d skipped=%d reasons=%v",
			end-i, len(batch), resp.FightsAccepted, resp.FightsSkipped, resp.SkipReasons)
		for _, f := range batch {
			if f.Timestamp > maxTs {
				maxTs = f.Timestamp
			}
		}
	}

	if maxTs > state.LastUploadedTs {
		state.LastUploadedTs = maxTs
		state.Contributor = contributor
		if err := saveState(state, statePath); err != nil {
			log.Printf("warning: failed to persist state: %v", err)
		}
	}
}

// ---- file discovery ----

// detectSavedVariablesPath finds the most recently modified VoidScout.lua
// under the standard WoW install paths.
func detectSavedVariablesPath() (string, error) {
	candidates := wowAccountRoots()
	var best string
	var bestMtime time.Time
	for _, root := range candidates {
		path := filepath.Join(root, "SavedVariables", "VoidScout.lua")
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		if best == "" || st.ModTime().After(bestMtime) {
			best = path
			bestMtime = st.ModTime()
		}
	}
	if best == "" {
		return "", fmt.Errorf("no VoidScout.lua found under any of %d candidate WoW installs", len(candidates))
	}
	return best, nil
}

// wowAccountRoots returns possible "Account/<ACCOUNT>" directories.
func wowAccountRoots() []string {
	var roots []string
	for _, install := range wowInstallPaths() {
		accountDir := filepath.Join(install, "WTF", "Account")
		entries, err := os.ReadDir(accountDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				roots = append(roots, filepath.Join(accountDir, e.Name()))
			}
		}
	}
	return roots
}

// wowInstallPaths returns common WoW _retail_ install locations per OS.
func wowInstallPaths() []string {
	var paths []string
	switch runtime.GOOS {
	case "windows":
		paths = []string{
			`C:\Program Files (x86)\World of Warcraft\_retail_`,
			`C:\Program Files\World of Warcraft\_retail_`,
			`D:\World of Warcraft\_retail_`,
			`E:\World of Warcraft\_retail_`,
		}
	case "darwin":
		home, _ := os.UserHomeDir()
		paths = []string{
			"/Applications/World of Warcraft/_retail_",
			filepath.Join(home, "Applications", "World of Warcraft", "_retail_"),
		}
	default: // linux (Lutris/Wine)
		home, _ := os.UserHomeDir()
		paths = []string{
			filepath.Join(home, "Games", "world-of-warcraft", "drive_c", "Program Files (x86)", "World of Warcraft", "_retail_"),
		}
	}
	var existing []string
	for _, p := range paths {
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			existing = append(existing, p)
		}
	}
	return existing
}

// ---- parsing ----

// parseSavedVars loads VoidScout.lua in an embedded Lua interpreter and
// pulls out VoidScoutDB.scores[<name>].fights[] into Go structs.
// Returns: per-player fights map, contributor name (the uploading character).
// OptOutRequest mirrors VoidScoutDB.opt_out_requested in SavedVariables.
// Written by the in-addon "Delete + go local" buttons in VoidScout's and
// VRTReader's consent dialogs. Drained by checkAndSendOptOut().
type OptOutRequest struct {
	Name         string
	Realm        string
	Region       string
	RequestedAt  int64
	Source       string
}

// parseOptOutRequest reads VoidScoutDB.opt_out_requested from any
// SavedVariables file that contains a VoidScoutDB global. Returns nil
// if no request is present (typical case).
func parseOptOutRequest(path string) (*OptOutRequest, error) {
	L := lua.NewState()
	defer L.Close()
	if err := L.DoFile(path); err != nil {
		return nil, fmt.Errorf("execute lua: %w", err)
	}
	db := L.GetGlobal("VoidScoutDB")
	if db.Type() != lua.LTTable {
		return nil, nil
	}
	oo := db.(*lua.LTable).RawGetString("opt_out_requested")
	if oo.Type() != lua.LTTable {
		return nil, nil
	}
	t := oo.(*lua.LTable)
	get := func(k string) string {
		v := t.RawGetString(k)
		if s, ok := v.(lua.LString); ok {
			return string(s)
		}
		return ""
	}
	getInt := func(k string) int64 {
		v := t.RawGetString(k)
		if n, ok := v.(lua.LNumber); ok {
			return int64(n)
		}
		return 0
	}
	req := &OptOutRequest{
		Name:        get("name"),
		Realm:       get("realm"),
		Region:      get("region"),
		RequestedAt: getInt("requested_at"),
		Source:      get("source"),
	}
	if req.Name == "" || req.Realm == "" || req.Region == "" {
		return nil, nil
	}
	return req, nil
}

// checkAndSendOptOut: if SavedVariables has a pending opt-out request that
// we haven't already POSTed (per state.LastOptOutTs), send it to
// /api/opt-out and on success record the timestamp.
func checkAndSendOptOut(svPath, apiBase string, state *State) {
	req, err := parseOptOutRequest(svPath)
	if err != nil {
		log.Printf("opt-out parse error: %v", err)
		return
	}
	if req == nil {
		return
	}
	if req.RequestedAt <= state.LastOptOutTs {
		return // already processed
	}
	log.Printf("opt-out requested for %s-%s-%s (source=%s); POSTing to %s",
		req.Name, req.Realm, req.Region, req.Source, apiBase)
	body := map[string]interface{}{
		"name":   req.Name,
		"realm":  req.Realm,
		"region": req.Region,
		"reason": fmt.Sprintf("addon button (%s)", req.Source),
	}
	bodyJSON, _ := json.Marshal(body)
	url := strings.TrimRight(apiBase, "/") + "/api/opt-out"
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(bodyJSON))
	if err != nil {
		log.Printf("opt-out request build failed: %v", err)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", "voidscout-uploader/"+currentVersion)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Printf("opt-out POST failed: %v", err)
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		state.LastOptOutTs = req.RequestedAt
		log.Printf("opt-out POST ok: %s", string(respBody)[:min(len(respBody), 200)])
	} else {
		log.Printf("opt-out POST returned %d: %s", resp.StatusCode, string(respBody)[:min(len(respBody), 200)])
	}
}

func parseSavedVars(path string) (map[string][]AddonFight, string, error) {
	L := lua.NewState()
	defer L.Close()
	if err := L.DoFile(path); err != nil {
		return nil, "", fmt.Errorf("execute lua: %w", err)
	}
	db := L.GetGlobal("VoidScoutDB")
	if db.Type() != lua.LTTable {
		return nil, "", fmt.Errorf("VoidScoutDB is not a table")
	}
	dbT := db.(*lua.LTable)
	scores := dbT.RawGetString("scores")
	if scores.Type() != lua.LTTable {
		return nil, "", fmt.Errorf("VoidScoutDB.scores missing")
	}

	out := make(map[string][]AddonFight)
	scores.(*lua.LTable).ForEach(func(name, pdata lua.LValue) {
		if pdata.Type() != lua.LTTable {
			return
		}
		fights := pdata.(*lua.LTable).RawGetString("fights")
		if fights.Type() != lua.LTTable {
			return
		}
		var list []AddonFight
		fights.(*lua.LTable).ForEach(func(_, fight lua.LValue) {
			if fight.Type() != lua.LTTable {
				return
			}
			ft := fight.(*lua.LTable)
			af := AddonFight{
				EncounterID:   intField(ft, "encounter_id"),
				EncounterName: strField(ft, "encounter_name"),
				DifficultyID:  intField(ft, "difficulty_id"),
				Outcome:       strField(ft, "outcome"),
				DurationSec:   intField(ft, "duration_sec"),
				Timestamp:     int64(intField(ft, "timestamp")),
				PugID:         strField(ft, "pug_id"),
				Class:         strField(ft, "class"),
				Spec:          strField(ft, "spec"),
				Mode:          strField(ft, "mode"),
				Uploaded:      boolField(ft, "uploaded"),
				Axes:          axesField(ft, "axes"),
				Roster:        stringListField(ft, "roster"),
				Raw:           rawField(ft, "raw"),
				RunID:         strField(ft, "run_id"),
				DataQuality:   strField(ft, "data_quality"),
			}
			list = append(list, af)
		})
		if len(list) > 0 {
			out[name.String()] = list
		}
	})

	// Contributor = the uploading character. Heuristic: the name with the
	// MOST fight rows (your own character will have a row for every fight
	// they were in across multiple pugs). If we can't tell, leave empty.
	contributor := pickContributor(out)
	return out, contributor, nil
}

func pickContributor(byName map[string][]AddonFight) string {
	best := ""
	bestCount := -1
	for name, fights := range byName {
		if len(fights) > bestCount {
			best = name
			bestCount = len(fights)
		}
	}
	return best
}

// ---- field helpers ----

func intField(t *lua.LTable, key string) int {
	v := t.RawGetString(key)
	if n, ok := v.(lua.LNumber); ok {
		return int(n)
	}
	return 0
}
func strField(t *lua.LTable, key string) string {
	v := t.RawGetString(key)
	if s, ok := v.(lua.LString); ok {
		return string(s)
	}
	return ""
}
func boolField(t *lua.LTable, key string) bool {
	v := t.RawGetString(key)
	if b, ok := v.(lua.LBool); ok {
		return bool(b)
	}
	return false
}
func stringListField(t *lua.LTable, key string) []string {
	v := t.RawGetString(key)
	if v.Type() != lua.LTTable {
		return nil
	}
	var out []string
	v.(*lua.LTable).ForEach(func(_, val lua.LValue) {
		if s, ok := val.(lua.LString); ok {
			out = append(out, string(s))
		}
	})
	return out
}
func axesField(t *lua.LTable, key string) map[string]float64 {
	v := t.RawGetString(key)
	if v.Type() != lua.LTTable {
		return nil
	}
	out := make(map[string]float64)
	v.(*lua.LTable).ForEach(func(k, val lua.LValue) {
		if n, ok := val.(lua.LNumber); ok {
			out[k.String()] = float64(n)
		}
	})
	return out
}

// rawField extracts the heterogeneous `raw = {...}` block per fight: damage_done,
// dps, casts, avoidable_taken, died, class — mix of numbers/strings/bools.
// Stored server-side as JSONB for peer-pool aggregation queries.
func rawField(t *lua.LTable, key string) map[string]interface{} {
	v := t.RawGetString(key)
	if v.Type() != lua.LTTable {
		return nil
	}
	out := make(map[string]interface{})
	v.(*lua.LTable).ForEach(func(k, val lua.LValue) {
		ks := k.String()
		switch val.Type() {
		case lua.LTNumber:
			out[ks] = float64(val.(lua.LNumber))
		case lua.LTString:
			out[ks] = string(val.(lua.LString))
		case lua.LTBool:
			out[ks] = bool(val.(lua.LBool))
		}
	})
	return out
}

// ---- pending logic ----

// filterPending returns one IngestFight per (player, fight) where the
// fight's timestamp is strictly greater than lastTs.
func filterPending(byName map[string][]AddonFight, lastTs int64) []IngestFight {
	var pending []IngestFight
	for fullName, fights := range byName {
		name, realm, region := parseFullName(fullName)
		for _, f := range fights {
			if f.Timestamp <= lastTs {
				continue
			}
			pending = append(pending, IngestFight{
				PlayerName:    name,
				PlayerRealm:   realm,
				PlayerRegion:  region,
				PlayerClass:   f.Class,
				PlayerSpec:    f.Spec,
				EncounterID:   f.EncounterID,
				EncounterName: f.EncounterName,
				DifficultyID:  f.DifficultyID,
				Outcome:       f.Outcome,
				DurationSec:   f.DurationSec,
				Timestamp:     f.Timestamp,
				PugID:         f.PugID,
				Mode:          f.Mode,
				Axes:          f.Axes,
				Roster:        f.Roster,
				Raw:           f.Raw,
				RunID:         f.RunID,
				DataQuality:   f.DataQuality,
			})
		}
	}
	return pending
}

// parseFullName splits "Vede-Elune-US" → ("Vede", "Elune", "US").
// Handles single-name (own char) and 2-part (no region) cases.
func parseFullName(full string) (name, realm, region string) {
	parts := strings.Split(full, "-")
	switch len(parts) {
	case 1:
		return parts[0], "Unknown", "US"
	case 2:
		return parts[0], parts[1], "US"
	default:
		// name-realm-region (or longer; assume last is region)
		return parts[0], strings.Join(parts[1:len(parts)-1], "-"), strings.ToUpper(parts[len(parts)-1])
	}
}

// ---- HTTP upload ----

func uploadBatch(apiBase, contributor string, batch []IngestFight) (*IngestResponse, error) {
	payload := IngestPayload{Contributor: contributor, Fights: batch}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", apiBase+"/api/ingest/fight", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "VoidScoutUploader/0.1")
	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", httpResp.StatusCode)
	}
	var resp IngestResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &resp, nil
}

// ---- state persistence ----

func loadState() (*State, string) {
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, stateDirName)
	path := filepath.Join(dir, stateFileName)
	st := &State{}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, st)
	}
	return st, path
}

func saveState(st *State, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ---- file watcher ----

// watch polls the file's mtime + size on a fixed interval. When EITHER
// changes, calls onChange. Polling is more reliable than fsnotify on
// Windows because WoW writes SavedVariables via atomic-replace (write
// temp, rename over) which is a known fsnotify weak point on win32.
// 5s latency is fine — uploads happen on human "after a raid" timescale.
func watch(svPath string, onChange func()) {
	var lastMod time.Time
	var lastSize int64
	if st, err := os.Stat(svPath); err == nil {
		lastMod = st.ModTime()
		lastSize = st.Size()
	}
	log.Printf("watcher: polling %s every %s (initial mod=%s size=%d)",
		svPath, pollInterval, lastMod.Format(time.RFC3339), lastSize)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for range ticker.C {
		st, err := os.Stat(svPath)
		if err != nil {
			continue
		}
		if st.ModTime().Equal(lastMod) && st.Size() == lastSize {
			continue
		}
		log.Printf("watcher: change detected (mod %s→%s, size %d→%d)",
			lastMod.Format(time.RFC3339), st.ModTime().Format(time.RFC3339),
			lastSize, st.Size())
		lastMod = st.ModTime()
		lastSize = st.Size()
		onChange()
	}
}

// ============================================================
// Profile uploader — outbound-only push of VoidScoutDB.playerScan.players
// to /api/profile/batch. Runs at startup (after a short delay) + every 15
// minutes. NEVER queries the server back — strictly upload. See the
// voidscout-data-direction memory.
//
// Per-slug skip set tracks the last_seen timestamp we pushed for each
// profile; we re-upload only when the addon refreshed a player's data.
// ============================================================

// runProfileLoop is the long-lived goroutine. svPath/apiBase/etc captured
// from main(). The skip-set lives in memory only (no persistence) — losing
// it on restart just means one extra full upload, which is fine.
func runProfileLoop(svPath, apiBase string, dryRun, verbose bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("profile: goroutine panic: %v", r)
		}
	}()

	// Tiny startup delay so the fight upload + auto-update have settled.
	time.Sleep(profileStartupDelay)

	lastUploadedLastSeen := make(map[string]int64) // slug → last_seen we pushed
	for {
		runProfileBatch(svPath, apiBase, lastUploadedLastSeen, dryRun, verbose)
		time.Sleep(profileLoopInterval)
	}
}

func runProfileBatch(svPath, apiBase string, lastUploadedLastSeen map[string]int64, dryRun, verbose bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("profile: batch panic: %v", r)
		}
	}()

	profiles, submitterGUID, err := parseProfiles(svPath)
	if err != nil {
		log.Printf("profile: parse error: %v", err)
		return
	}
	if len(profiles) == 0 {
		if verbose {
			log.Printf("profile: no profiles in SavedVars yet")
		}
		return
	}
	if submitterGUID == "" {
		// We can still upload without it — server just records empty
		// contributor_guids — but log so we notice.
		if verbose {
			log.Printf("profile: no self GUID found; uploading anonymously")
		}
	}

	// Filter: drop stale + sub-cap + un-changed-since-last-upload
	now := time.Now().Unix()
	maxAge := int64(profileMaxAgeDays) * 86400
	var pending []ProfileUpload
	var (
		skippedLevel   int
		skippedStale   int
		skippedNoSlug  int
		skippedNoChange int
	)
	for _, p := range profiles {
		if p.Slug == "" {
			skippedNoSlug++
			continue
		}
		if p.Level > 0 && p.Level < 80 {
			skippedLevel++
			continue
		}
		if p.LastSeen > 0 && (now-p.LastSeen) > maxAge {
			skippedStale++
			continue
		}
		// Re-upload only when the addon's last_seen advanced past what we sent.
		if prev, ok := lastUploadedLastSeen[p.Slug]; ok && p.LastSeen > 0 && p.LastSeen <= prev {
			skippedNoChange++
			continue
		}
		pending = append(pending, p)
	}
	if len(pending) == 0 {
		if verbose {
			log.Printf("profile: nothing new to upload (total=%d, lvl=%d stale=%d noSlug=%d noChange=%d)",
				len(profiles), skippedLevel, skippedStale, skippedNoSlug, skippedNoChange)
		}
		return
	}

	log.Printf("profile: uploading %d/%d (lvl=%d stale=%d noSlug=%d noChange=%d) as %q",
		len(pending), len(profiles), skippedLevel, skippedStale, skippedNoSlug, skippedNoChange, submitterGUID)

	if dryRun {
		for _, p := range pending {
			log.Printf("  [DRY] %s lvl=%d ilvl=%d rio=%v", p.Slug, p.Level, p.Ilvl, p.RioScore)
		}
		return
	}

	for i := 0; i < len(pending); i += profileBatchSize {
		end := i + profileBatchSize
		if end > len(pending) {
			end = len(pending)
		}
		batch := pending[i:end]
		resp, err := uploadProfileBatch(apiBase, submitterGUID, batch)
		if err != nil {
			log.Printf("profile: batch %d-%d failed: %v", i, end, err)
			// Don't advance lastUploadedLastSeen for this batch — try again next loop.
			continue
		}
		log.Printf("profile: batch %d-%d accepted=%d filtered=%d errors=%v",
			i, end, resp.Accepted, resp.Filtered, resp.Errors)
		for _, p := range batch {
			if p.LastSeen > 0 {
				lastUploadedLastSeen[p.Slug] = p.LastSeen
			} else {
				// No last_seen → mark with current unix time so we don't
				// re-upload until the addon re-touches it.
				lastUploadedLastSeen[p.Slug] = now
			}
		}
	}
}

// parseProfiles loads VoidScout.lua, walks VoidScoutDB.playerScan.players,
// and converts each table to a ProfileUpload. Returns the slice + the
// submitter's GUID (the player marked is_self=true).
func parseProfiles(path string) ([]ProfileUpload, string, error) {
	L := lua.NewState()
	defer L.Close()
	if err := L.DoFile(path); err != nil {
		return nil, "", fmt.Errorf("execute lua: %w", err)
	}
	db := L.GetGlobal("VoidScoutDB")
	if db.Type() != lua.LTTable {
		return nil, "", fmt.Errorf("VoidScoutDB is not a table")
	}
	playerScan := db.(*lua.LTable).RawGetString("playerScan")
	if playerScan.Type() != lua.LTTable {
		// Older addon SavedVars before PlayerScan module — not an error,
		// just nothing to upload.
		return nil, "", nil
	}
	players := playerScan.(*lua.LTable).RawGetString("players")
	if players.Type() != lua.LTTable {
		return nil, "", nil
	}

	var out []ProfileUpload
	var submitterGUID string
	players.(*lua.LTable).ForEach(func(guidVal, pdata lua.LValue) {
		if pdata.Type() != lua.LTTable {
			return
		}
		t := pdata.(*lua.LTable)
		isSelf := boolField(t, "is_self")
		guid := ""
		if s, ok := guidVal.(lua.LString); ok {
			guid = string(s)
		}
		if isSelf && submitterGUID == "" {
			submitterGUID = guid
		}

		p := ProfileUpload{
			Slug:              strField(t, "slug"),
			Name:              strField(t, "name"),
			Realm:             strField(t, "realm"),
			RealmSlug:         strField(t, "realm_slug"),
			Region:            strField(t, "region"),
			Class:             strField(t, "class"),
			Race:              strField(t, "race"),
			Faction:           strField(t, "faction"),
			Level:             intField(t, "level"),
			Guild:             strField(t, "guild"),
			Ilvl:              intField(t, "ilvl"),
			Spec:              strField(t, "spec"),
			SpecID:            intField(t, "spec_id"),
			AchievementPoints: intField(t, "achievement_points"),
			Achievements:      truthyMapField(t, "achievements"),
			BossKills:         truthyMapField(t, "boss_kills"),
			Archon:            archonField(t, "archon"),
			LastSeen:          int64(intField(t, "last_seen")),
		}
		if rio := floatPtrField(t, "rio_score"); rio != nil {
			p.RioScore = rio
		}
		if rioPrev := floatPtrField(t, "rio_score_prev"); rioPrev != nil {
			p.RioScorePrev = rioPrev
		}
		// Source attribution — tells the website where this came from
		var sources []string
		sources = append(sources, "scan")
		if _, ok := t.RawGetString("rio_score").(lua.LNumber); ok {
			sources = append(sources, "rio_local")
		}
		if t.RawGetString("archon").Type() == lua.LTTable {
			sources = append(sources, "archon_local")
		}
		p.Sources = sources

		out = append(out, p)
	})

	return out, submitterGUID, nil
}

// truthyMapField pulls a {key = true} table into a Go map. Skips false / nil
// values so the server-side set-union doesn't get smashed by false.
func truthyMapField(t *lua.LTable, key string) map[string]bool {
	v := t.RawGetString(key)
	if v.Type() != lua.LTTable {
		return nil
	}
	out := make(map[string]bool)
	v.(*lua.LTable).ForEach(func(k, val lua.LValue) {
		if b, ok := val.(lua.LBool); ok && bool(b) {
			if ks, ok2 := k.(lua.LString); ok2 {
				out[string(ks)] = true
			}
		}
	})
	if len(out) == 0 {
		return nil
	}
	return out
}

// archonField unpacks p.archon = { N={...}, H={...}, M={...} } where each
// inner table has {progress, total, avg, asp, rank, encounters}.
func archonField(t *lua.LTable, key string) map[string]map[string]any {
	v := t.RawGetString(key)
	if v.Type() != lua.LTTable {
		return nil
	}
	out := make(map[string]map[string]any)
	v.(*lua.LTable).ForEach(func(k, val lua.LValue) {
		ks, ok := k.(lua.LString)
		if !ok {
			return
		}
		if val.Type() != lua.LTTable {
			return
		}
		diffKey := string(ks)
		inner := make(map[string]any)
		val.(*lua.LTable).ForEach(func(fk, fv lua.LValue) {
			fks, ok := fk.(lua.LString)
			if !ok {
				return
			}
			fieldName := string(fks)
			switch fieldName {
			case "progress", "total", "avg", "asp", "rank":
				if n, ok := fv.(lua.LNumber); ok {
					inner[fieldName] = float64(n)
				}
			case "encounters":
				// Pass through as nested map: { encID: { kills, best } }
				if fv.Type() == lua.LTTable {
					enc := make(map[string]map[string]float64)
					fv.(*lua.LTable).ForEach(func(eid, edata lua.LValue) {
						if edata.Type() != lua.LTTable {
							return
						}
						idStr := ""
						if n, ok := eid.(lua.LNumber); ok {
							idStr = fmt.Sprintf("%d", int(n))
						} else if s, ok := eid.(lua.LString); ok {
							idStr = string(s)
						}
						if idStr == "" {
							return
						}
						ed := make(map[string]float64)
						edata.(*lua.LTable).ForEach(func(efk, efv lua.LValue) {
							if efks, ok := efk.(lua.LString); ok {
								if n, ok := efv.(lua.LNumber); ok {
									ed[string(efks)] = float64(n)
								}
							}
						})
						if len(ed) > 0 {
							enc[idStr] = ed
						}
					})
					if len(enc) > 0 {
						inner["encounters"] = enc
					}
				}
			}
		})
		if len(inner) > 0 {
			out[diffKey] = inner
		}
	})
	if len(out) == 0 {
		return nil
	}
	return out
}

// floatPtrField returns a pointer so we can distinguish "missing" from
// "explicitly zero" — required because RioScore = nil should NOT clobber
// an existing server-side value via COALESCE.
func floatPtrField(t *lua.LTable, key string) *float64 {
	v := t.RawGetString(key)
	if n, ok := v.(lua.LNumber); ok {
		f := float64(n)
		return &f
	}
	return nil
}

func uploadProfileBatch(apiBase, submitterGUID string, batch []ProfileUpload) (*ProfileResponse, error) {
	payload := ProfileBatch{
		SubmitterGUID: submitterGUID,
		Profiles:      batch,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", apiBase+"/api/profile/batch", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "VoidScoutUploader/"+currentVersion)
	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", httpResp.StatusCode)
	}
	var resp ProfileResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &resp, nil
}

// =================================================================
// COMBAT LOG FILE WATCHER (Phase 2 — addon auto-toggles /combatlog;
// uploader detects new files and POSTs them to /api/upload-log).
// =================================================================

// detectLogsDirs returns Logs/ directories under all detected WoW installs.
// Each install has Logs/ as a sibling of WTF/. We watch all of them.
func detectLogsDirs() []string {
	var dirs []string
	for _, install := range wowInstallPaths() {
		logs := filepath.Join(install, "Logs")
		if st, err := os.Stat(logs); err == nil && st.IsDir() {
			dirs = append(dirs, logs)
		}
	}
	return dirs
}

// combatLogStable is the quiet period after which we consider a combat log
// file "done writing." WoW only stops writing when /combatlog is disabled;
// our addon disables on encounter/run end, so 60s of no activity is safe.
const combatLogStable = 60 * time.Second

// combatLogMaxBytes is the local sanity cap. Server's MAX_UPLOAD_BYTES is
// 1 GB; this is a paranoia check so we never accidentally pump a 10 GB
// rotating debug log up. 2 GB covers any legitimate raid night.
const combatLogMaxBytes = int64(2 * 1024 * 1024 * 1024)

// combatLogChunkThreshold is the file size above which we switch to
// per-encounter chunked upload instead of pumping the whole file in one
// POST. Cloudflare's Free/Pro tiers cap bodies at 100 MB, but in practice a
// single whole-file upload through the cloudflared tunnel gets flaky well
// below that — a 25.9 MB log intermittently 400'd ("expected multipart") in
// transit. So we chunk early (8 MB): per-encounter chunks are small and
// robust everywhere, which is also what keeps public (non-bypass) users
// CF-safe. See [[voidscout-upload-cf-bypass-constraint]].
const combatLogChunkThreshold = int64(8 * 1024 * 1024)

// combatLogMaxChunkBytes is the hard cap on a single chunk after splitting.
// If one encounter alone is bigger than this we have to skip it (a future
// release can stream-split inside an encounter, but no pull in current
// Mythic content gets close to 100 MB).
const combatLogMaxChunkBytes = int64(95 * 1024 * 1024)

// computeBackoff maps {http status, attempt#} → next-eligible-time offset.
// 429: rate-limited. Server's per-char cap is 200/day; once we hit it, no
//      point retrying for hours. Step: 5 min, 15 min, 1 hr, 6 hr, 24 hr.
// 413: payload too big for Cloudflare. We've already switched to chunking
//      mode if it's possible — a 413 on the chunked path means the chunk
//      itself is bigger than 100 MB, can't fix at runtime. 24 hr cooldown
//      so we don't spam the log.
// 5xx: server-side issue. Short retry — 1 min, 5 min, 30 min, 2 hr.
// 4xx other: client mistake we can't fix. Long cooldown — 6 hr — so the
//      user has time to read the error before we retry.
// 0 (transport error): network hiccup. Short retry — 30s, 2m, 10m, 1h.
func computeBackoff(code, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	switch {
	case code == 429:
		steps := []time.Duration{5 * time.Minute, 15 * time.Minute, 1 * time.Hour, 6 * time.Hour, 24 * time.Hour}
		return steps[min(attempt-1, len(steps)-1)]
	case code == 413:
		return 24 * time.Hour
	case code >= 500 && code < 600:
		steps := []time.Duration{1 * time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour}
		return steps[min(attempt-1, len(steps)-1)]
	case code >= 400 && code < 500:
		return 6 * time.Hour
	case code == 0:
		// Transport error — likely transient.
		steps := []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, 1 * time.Hour}
		return steps[min(attempt-1, len(steps)-1)]
	}
	return 5 * time.Minute
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// extractEncounterChunks splits a combat log into per-encounter mini-logs.
// Each chunk starts with the file's `COMBAT_LOG_VERSION` header and the
// preceding `ZONE_CHANGE` for context, followed by exactly one
// ENCOUNTER_START → ENCOUNTER_END block. The server treats each chunk
// like a normal upload — no new endpoint needed.
//
// Why per-encounter: the parser already operates one encounter at a time,
// so splitting at the encounter boundary preserves all the data it
// actually scores from. Trash between encounters is dropped, but the
// scoring pipeline doesn't consume it anyway.
//
// Returns one byte slice per chunk. Skips encounters that exceed the
// 95 MB per-chunk cap (logged so the user knows).
func extractEncounterChunks(fileBytes []byte) [][]byte {
	var chunks [][]byte
	var header []byte // COMBAT_LOG_VERSION + last-seen ZONE_CHANGE

	// Find COMBAT_LOG_VERSION line — must be present, it's literally line 1.
	headerEnd := bytes.IndexByte(fileBytes, '\n')
	if headerEnd < 0 {
		return chunks
	}
	header = append(header, fileBytes[:headerEnd+1]...)

	// Scan for ZONE_CHANGE + ENCOUNTER_START/END markers.
	var lastZoneChange []byte
	var inEncounter bool
	var encStart int

	pos := headerEnd + 1
	for pos < len(fileBytes) {
		// Find end of this line.
		nl := bytes.IndexByte(fileBytes[pos:], '\n')
		var lineEnd int
		if nl < 0 {
			lineEnd = len(fileBytes)
		} else {
			lineEnd = pos + nl + 1
		}
		line := fileBytes[pos:lineEnd]

		// Skim past the timestamp prefix (`MM/DD/YYYY HH:MM:SS.mmm-N  `).
		// Two-space separator marks the event payload.
		idx := bytes.Index(line, []byte("  "))
		eventStart := pos
		if idx >= 0 {
			eventStart = pos + idx + 2
		}

		if bytes.HasPrefix(fileBytes[eventStart:], []byte("ZONE_CHANGE,")) {
			lastZoneChange = append(lastZoneChange[:0], line...)
		} else if !inEncounter && bytes.HasPrefix(fileBytes[eventStart:], []byte("ENCOUNTER_START,")) {
			inEncounter = true
			encStart = pos
		} else if inEncounter && bytes.HasPrefix(fileBytes[eventStart:], []byte("ENCOUNTER_END,")) {
			// Build the chunk: header + last zone change + encounter slice.
			var chunk []byte
			chunk = append(chunk, header...)
			if len(lastZoneChange) > 0 {
				chunk = append(chunk, lastZoneChange...)
			}
			chunk = append(chunk, fileBytes[encStart:lineEnd]...)
			if int64(len(chunk)) <= combatLogMaxChunkBytes {
				chunks = append(chunks, chunk)
			} else {
				log.Printf("combatlog: skip oversize encounter chunk (%.1f MB > %d MB)",
					float64(len(chunk))/1024/1024, combatLogMaxChunkBytes/1024/1024)
			}
			inEncounter = false
			_ = encStart
		}
		pos = lineEnd
		if nl < 0 {
			break
		}
	}
	return chunks
}

// scanCombatLogs walks all detected Logs directories, identifies stable
// (no-recent-writes) WoWCombatLog-*.txt files we haven't uploaded yet,
// and POSTs each one to /api/upload-log. State is persisted after each
// successful upload so we never re-send.
func scanCombatLogs(apiBase string, state *State, statePath string, dryRun, verbose bool) {
	if state.Contributor == "" {
		if verbose {
			log.Printf("combatlog: no Contributor yet, skipping scan")
		}
		return
	}
	playerName, playerRealm := splitContributor(state.Contributor)
	if playerName == "" {
		if verbose {
			log.Printf("combatlog: cannot parse Contributor %q", state.Contributor)
		}
		return
	}
	if playerRealm == "" {
		playerRealm = state.UploadedRealm
	}
	playerRegion := state.UploadedRegion
	if playerRegion == "" {
		playerRegion = "us"
	}

	dirs := detectLogsDirs()
	if len(dirs) == 0 {
		if verbose {
			log.Printf("combatlog: no WoW Logs/ directory found")
		}
		return
	}

	now := time.Now()
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			log.Printf("combatlog: read %s: %v", dir, err)
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasPrefix(name, "WoWCombatLog-") || !strings.HasSuffix(name, ".txt") {
				continue
			}
			full := filepath.Join(dir, name)
			if state.UploadedCombatLogs[full] {
				continue
			}
			st, err := os.Stat(full)
			if err != nil {
				continue
			}
			if st.Size() == 0 {
				continue
			}
			if st.Size() > combatLogMaxBytes {
				log.Printf("combatlog: SKIP %s (%.1f MB > %d MB cap)",
					name, float64(st.Size())/1024/1024, combatLogMaxBytes/1024/1024)
				// Mark uploaded so we don't keep re-checking it
				if state.UploadedCombatLogs == nil {
					state.UploadedCombatLogs = map[string]bool{}
				}
				state.UploadedCombatLogs[full] = true
				saveState(state, statePath)
				continue
			}
			if now.Sub(st.ModTime()) < combatLogStable {
				if verbose {
					log.Printf("combatlog: %s still being written (last write %ds ago)",
						name, int(now.Sub(st.ModTime()).Seconds()))
				}
				continue
			}

			// Per-file backoff. If we've failed previously and the cool-down
			// hasn't expired, skip silently — burning the 200/day rate cap
			// by retrying every 30s is exactly the bug we're fixing.
			if r := state.CombatLogRetry[full]; r != nil && r.NextEligibleUnix > now.Unix() {
				if verbose {
					remaining := time.Until(time.Unix(r.NextEligibleUnix, 0))
					log.Printf("combatlog: %s on backoff %s (attempt %d, last code %d: %s)",
						name, remaining.Round(time.Second), r.AttemptCount, r.LastErrorCode, r.LastErrorMsg)
				}
				continue
			}

			if dryRun {
				log.Printf("combatlog: [DRY] would upload %s (%.1f MB) as %s-%s-%s",
					name, float64(st.Size())/1024/1024, playerName, playerRealm, playerRegion)
				continue
			}

			// Chunked path for anything that would blow Cloudflare's 100MB body cap.
			// Per-encounter chunks: each ENCOUNTER_START → ENCOUNTER_END block
			// becomes its own POST to the same /api/upload-log endpoint.
			var lastCode int
			var lastErr error
			if st.Size() > combatLogChunkThreshold {
				log.Printf("combatlog: uploading %s in chunks (%.1f MB > %d MB threshold)",
					name, float64(st.Size())/1024/1024, combatLogChunkThreshold/1024/1024)
				lastCode, lastErr = uploadCombatLogChunked(apiBase, full, playerName, playerRealm, playerRegion, verbose)
			} else {
				log.Printf("combatlog: uploading %s (%.1f MB)...",
					name, float64(st.Size())/1024/1024)
				lastCode, lastErr = uploadCombatLog(apiBase, full, playerName, playerRealm, playerRegion, verbose)
			}

			if lastErr != nil {
				// 422 = the server parsed the log fine; there was simply nothing
				// scorable in it (dummy / trash / abandoned — no completed boss
				// fight). That's a terminal SUCCESS, not a retryable failure: mark
				// it done so we stop re-uploading a no-boss log every 6h forever.
				// Covers BOTH paths — the whole-file path returns the server's 422,
				// and the chunked path returns 422 when len(chunks)==0 (no encounters).
				if lastCode == 422 {
					if state.UploadedCombatLogs == nil {
						state.UploadedCombatLogs = map[string]bool{}
					}
					state.UploadedCombatLogs[full] = true
					delete(state.CombatLogRetry, full)
					saveState(state, statePath)
					log.Printf("combatlog: %s — no scorable encounters, marking done (no retry)", name)
					continue
				}
				log.Printf("combatlog: UPLOAD FAILED %s (code %d): %v", name, lastCode, lastErr)
				// Record retry state so we back off intelligently.
				if state.CombatLogRetry == nil {
					state.CombatLogRetry = map[string]*CombatLogRetry{}
				}
				r := state.CombatLogRetry[full]
				if r == nil {
					r = &CombatLogRetry{}
					state.CombatLogRetry[full] = r
				}
				r.AttemptCount++
				r.LastErrorCode = lastCode
				r.LastErrorAt = now.Unix()
				r.LastErrorMsg = truncate(lastErr.Error(), 200)
				backoff := computeBackoff(lastCode, r.AttemptCount)
				r.NextEligibleUnix = now.Add(backoff).Unix()
				log.Printf("combatlog: will retry %s in %s (attempt #%d)",
					name, backoff.Round(time.Second), r.AttemptCount+1)
				saveState(state, statePath)
				continue
			}

			// Success — clear retry state, mark uploaded.
			if state.UploadedCombatLogs == nil {
				state.UploadedCombatLogs = map[string]bool{}
			}
			state.UploadedCombatLogs[full] = true
			delete(state.CombatLogRetry, full)
			saveState(state, statePath)
			log.Printf("combatlog: uploaded %s", name)
		}
	}
}

// uploadCombatLog POSTs a single file via multipart/form-data to
// /api/upload-log. The server parses, validates, scores, and ingests
// per upload_log_endpoint.py.
//
// IMPORTANT: We read the file fully into memory BEFORE opening the HTTP
// request, so the OS file handle is released within milliseconds. Without
// this, a slow or retried upload holds the handle for the entire transfer
// duration, which on Windows blocks `move`, `del`, and even `mv` from
// other processes ("file is being used by another process"). The 200MB
// upload cap means worst-case memory is 200MB, fine on any modern PC.
func uploadCombatLog(apiBase, path, playerName, playerRealm, playerRegion string, verbose bool) (int, error) {
	// Buffer the file content into memory first, then close the handle.
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	return postCombatLogBytes(apiBase, filepath.Base(path), fileBytes, playerName, playerRealm, playerRegion, verbose)
}

// uploadCombatLogChunked splits the log at ENCOUNTER_START/END boundaries
// and POSTs each chunk separately. Each chunk is a self-contained mini
// combat log (COMBAT_LOG_VERSION + last ZONE_CHANGE + one encounter), so
// the existing server endpoint handles them with no changes.
//
// Returns the worst HTTP code seen and the first error encountered, since
// the caller's backoff logic only needs one code/error to decide what to do.
// If ANY chunk fails, the whole file is treated as failed (we don't want to
// mark a partially-uploaded log as done — re-uploading is idempotent
// thanks to the (player_id, fight_id, contributor) PK).
func uploadCombatLogChunked(apiBase, path, playerName, playerRealm, playerRegion string, verbose bool) (int, error) {
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	chunks := extractEncounterChunks(fileBytes)
	if len(chunks) == 0 {
		return 422, fmt.Errorf("no encounters found in file (was /combatlog on?)")
	}
	base := filepath.Base(path)
	log.Printf("combatlog: split %s into %d encounter chunks", base, len(chunks))
	// A multi-session log routinely contains chunks with nothing scorable —
	// target-dummy tests, trash-only segments, abandoned pulls. The server
	// answers those with 422 "no scorable encounters". That is NOT a file
	// failure: skip the empty chunk and keep the GOOD encounters that already
	// uploaded. Only a real error (5xx / network / 413 / 429) fails the whole
	// file and triggers backoff. (Before this, ONE 422 chunk poisoned the
	// entire file — a completed +15 sitting alongside a morning of dummy
	// testing never got marked done. Verified 2026-06-12: the +15 chunks POST
	// 200 individually; it was the dummy-test chunks' 422 failing the file.)
	var realCode int
	var realErr error
	okCount, skipped := 0, 0
	for i, chunk := range chunks {
		chunkName := fmt.Sprintf("%s.chunk%d.txt", base, i+1)
		if verbose {
			log.Printf("combatlog: chunk %d/%d (%.1f MB)",
				i+1, len(chunks), float64(len(chunk))/1024/1024)
		}
		code, err := postCombatLogBytes(apiBase, chunkName, chunk, playerName, playerRealm, playerRegion, verbose)
		if err != nil {
			// 422 = this chunk has no scorable encounter (dummy/trash/abandoned).
			// Expected in a mixed log — skip it, do NOT fail the file.
			if code == 422 {
				skipped++
				if verbose {
					log.Printf("combatlog: chunk %d/%d no scorable encounters — skipping", i+1, len(chunks))
				}
				continue
			}
			log.Printf("combatlog: chunk %d/%d FAILED (code %d): %v", i+1, len(chunks), code, err)
			if realErr == nil {
				realErr = fmt.Errorf("chunk %d/%d: %w", i+1, len(chunks), err)
				realCode = code
			}
			// 429/413 won't fix on the next chunk — abort the file now.
			if code == 429 || code == 413 {
				return code, realErr
			}
			// 5xx might be transient (server restart) — try the next chunk.
			continue
		}
		okCount++
		log.Printf("combatlog: chunk %d/%d ok", i+1, len(chunks))
	}
	// Only a genuine error fails the file. Empty (422) chunks are fine.
	if realErr != nil {
		return realCode, realErr
	}
	log.Printf("combatlog: %s complete — %d scored, %d skipped (no scorable encounters)", base, okCount, skipped)
	return 200, nil
}

// postCombatLogBytes is the shared multipart-POST helper. Returns the HTTP
// status code (0 if the transport itself failed) and an error describing
// what went wrong. Successful (2xx) responses return (code, nil).
func postCombatLogBytes(apiBase, filename string, fileBytes []byte, playerName, playerRealm, playerRegion string, verbose bool) (int, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("character_name", playerName); err != nil {
		return 0, err
	}
	if err := mw.WriteField("realm", playerRealm); err != nil {
		return 0, err
	}
	if err := mw.WriteField("region", playerRegion); err != nil {
		return 0, err
	}
	part, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return 0, fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(fileBytes); err != nil {
		return 0, fmt.Errorf("write file body: %w", err)
	}
	if err := mw.Close(); err != nil {
		return 0, err
	}

	req, err := http.NewRequest("POST", apiBase+"/api/upload-log", &body)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("User-Agent", fmt.Sprintf("voidscout-uploader/%s", currentVersion))

	client := &http.Client{Timeout: 600 * time.Second} // server can take ~5 min on a big file
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 300))
	}
	if verbose {
		log.Printf("combatlog: server response: %s", truncate(string(respBody), 300))
	}
	return resp.StatusCode, nil
}

// splitContributor splits "Name-Realm" or "Name-Realm-Region" into parts.
// Returns name + realm (region handled separately).
func splitContributor(s string) (name, realm string) {
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, "-")
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
