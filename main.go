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
	currentVersion = "0.3.1"                         // bumped on each release; compared to GitHub

	// Combat log scan cadence — the addon auto-toggles /combatlog on
	// encounter/run boundaries, so files appear and stabilize at that
	// rhythm. 30s ticks comfortably catch stable files without thrashing.
	combatLogScanInterval = 30 * time.Second
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

type State struct {
	LastUploadedTs      int64           `json:"last_uploaded_ts"` // upload all fights with ts > this
	Contributor         string          `json:"contributor"`      // last-known uploading character
	UploadedCombatLogs  map[string]bool `json:"uploaded_combat_logs,omitempty"` // filename -> uploaded?
	UploadedRealm       string          `json:"uploaded_realm,omitempty"`       // last-known realm
	UploadedRegion      string          `json:"uploaded_region,omitempty"`      // last-known region
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

	// Watch (blocking)
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

// combatLogMaxBytes mirrors the server's MAX_UPLOAD_BYTES (200 MB). Files
// larger than this are skipped with a warning — the server would reject
// them anyway. Future improvement: split by ENCOUNTER_START markers.
const combatLogMaxBytes = int64(200 * 1024 * 1024)

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

			if dryRun {
				log.Printf("combatlog: [DRY] would upload %s (%.1f MB) as %s-%s-%s",
					name, float64(st.Size())/1024/1024, playerName, playerRealm, playerRegion)
				continue
			}

			log.Printf("combatlog: uploading %s (%.1f MB)...",
				name, float64(st.Size())/1024/1024)
			if err := uploadCombatLog(apiBase, full, playerName, playerRealm, playerRegion, verbose); err != nil {
				log.Printf("combatlog: UPLOAD FAILED %s: %v", name, err)
				continue
			}
			if state.UploadedCombatLogs == nil {
				state.UploadedCombatLogs = map[string]bool{}
			}
			state.UploadedCombatLogs[full] = true
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
func uploadCombatLog(apiBase, path, playerName, playerRealm, playerRegion string, verbose bool) error {
	// Buffer the file content into memory first, then close the handle.
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("character_name", playerName); err != nil {
		return err
	}
	if err := mw.WriteField("realm", playerRealm); err != nil {
		return err
	}
	if err := mw.WriteField("region", playerRegion); err != nil {
		return err
	}
	part, err := mw.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(fileBytes); err != nil {
		return fmt.Errorf("write file body: %w", err)
	}
	if err := mw.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest("POST", apiBase+"/api/upload-log", &body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("User-Agent", fmt.Sprintf("voidscout-uploader/%s", currentVersion))

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 300))
	}
	if verbose {
		log.Printf("combatlog: server response: %s", truncate(string(respBody), 300))
	}
	return nil
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
