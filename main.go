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
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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

	// Auto-update
	updateRepo    = "bughatti/voidscout-uploader"   // public GitHub repo for releases
	updateCheckTimeout = 10 * time.Second
	currentVersion = "0.2.0"                         // bumped on each release; compared to GitHub
)

// ---- types matching the addon's SavedVariables shape ----

type AddonFight struct {
	EncounterID   int                `json:"encounter_id"`
	EncounterName string             `json:"encounter_name"`
	DifficultyID  int                `json:"difficulty_id"`
	Outcome       string             `json:"outcome"`
	DurationSec   int                `json:"duration_sec"`
	Timestamp     int64              `json:"timestamp"`
	PugID         string             `json:"pug_id"`
	Class         string             `json:"class"`
	Spec          string             `json:"spec"`
	Uploaded      bool               `json:"uploaded"`
	Axes          map[string]float64 `json:"axes"`
	Roster        []string           `json:"roster"`
}

type IngestFight struct {
	PlayerName    string             `json:"player_name"`
	PlayerRealm   string             `json:"player_realm,omitempty"`
	PlayerRegion  string             `json:"player_region,omitempty"`
	PlayerClass   string             `json:"player_class,omitempty"`
	PlayerSpec    string             `json:"player_spec,omitempty"`
	EncounterID   int                `json:"encounter_id"`
	EncounterName string             `json:"encounter_name"`
	DifficultyID  int                `json:"difficulty_id"`
	Outcome       string             `json:"outcome"`
	DurationSec   int                `json:"duration_sec"`
	Timestamp     int64              `json:"timestamp"`
	PugID         string             `json:"pug_id,omitempty"`
	Axes          map[string]float64 `json:"axes"`
	Roster        []string           `json:"roster,omitempty"`
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

type State struct {
	LastUploadedTs int64  `json:"last_uploaded_ts"` // upload all fights with ts > this
	Contributor    string `json:"contributor"`      // last-known uploading character
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

// ---- main loop ----

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

	// Watch
	watch(svPath, func() {
		runOnce(svPath, *apiBase, state, statePath, *dryRun, *verbose)
	})
}

// runOnce reads the file, finds pending fights, uploads them, persists state.
func runOnce(svPath, apiBase string, state *State, statePath string, dryRun, verbose bool) {
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
				Uploaded:      boolField(ft, "uploaded"),
				Axes:          axesField(ft, "axes"),
				Roster:        stringListField(ft, "roster"),
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
				Axes:          f.Axes,
				Roster:        f.Roster,
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
